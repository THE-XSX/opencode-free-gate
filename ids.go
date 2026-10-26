package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type requestIDs struct {
	Session string
	Request string
	Project string
}

// deriveRequestIDs 生成上游 OpenCode 协议所需的会话、请求与项目标识。
// 会话 ID 优先透传客户端已有的合法 ses_ 格式标识，或根据显式会话信号 / 第一条用户消息生成稳定哈希；
// 请求 ID 保留合法 req_/msg_ 标识或生成全新 ID；
// 项目标识保留 global 或合法 prj_ 标识，未指定时消除网关特定特征并使用标准默认值。
func deriveRequestIDs(headers http.Header, body map[string]any) requestIDs {
	rawSession := firstString(
		headers.Get("x-opencode-session"),
		headers.Get("x-session-id"),
		headers.Get("conversation-id"),
		stringAt(body, "conversation_id"),
		stringAt(body, "metadata", "session_id"),
	)

	var sessionID string
	if strings.HasPrefix(rawSession, "ses_") && len(rawSession) > 4 {
		// 客户端显式传递了符合 OpenCode 格式的 session，直接透传保留
		sessionID = rawSession
	} else if rawSession != "" {
		// 客户端传递了非 ses_ 格式的会话信号（如普通 uuid、conversation_id），生成稳定哈希
		sessionID = stableID("ses", rawSession)
	} else {
		// 从消息历史中提取第一轮消息生成稳定会话哈希
		seed := conversationSeed(body)
		if seed == "" {
			seed = stringAt(body, "previous_response_id")
		}
		if seed == "" || seed == "{}" {
			sessionID = randomID("ses", 32)
		} else {
			sessionID = stableID("ses", seed)
		}
	}

	rawRequest := firstString(
		headers.Get("x-opencode-request"),
		headers.Get("x-request-id"),
	)
	var requestID string
	if (strings.HasPrefix(rawRequest, "req_") || strings.HasPrefix(rawRequest, "msg_")) && len(rawRequest) > 4 {
		requestID = rawRequest
	} else {
		requestID = randomID("req", 16)
	}

	rawProject := firstString(
		headers.Get("x-opencode-project"),
		stringAt(body, "metadata", "project_id"),
	)
	var projectID string
	if rawProject == "global" || (strings.HasPrefix(rawProject, "prj_") && len(rawProject) > 4) {
		projectID = rawProject
	} else if rawProject != "" {
		projectID = stableID("prj", rawProject)
	} else {
		defaultPrj := opencodeDefaultProject()
		if defaultPrj == "global" {
			// 如果环境变量显式指定了 OPENCODE_PROJECT=global，则使用 "global"；
			// 否则使用与 global 绑定的稳定 prj_ 哈希，保持与既有测试兼容
			if strings.EqualFold(os.Getenv("OPENCODE_PROJECT"), "global") {
				projectID = "global"
			} else {
				projectID = stableID("prj", "global")
			}
		} else if strings.HasPrefix(defaultPrj, "prj_") {
			projectID = defaultPrj
		} else {
			projectID = stableID("prj", defaultPrj)
		}
	}

	return requestIDs{
		Session: sessionID,
		Request: requestID,
		Project: projectID,
	}
}

func conversationSeed(body map[string]any) string {
	if input, ok := body["input"].(string); ok && input != "" {
		return input
	}
	for _, field := range []string{"messages", "input"} {
		items, _ := body[field].([]any)
		for _, raw := range items {
			item, ok := raw.(map[string]any)
			if !ok || stringAt(item, "role") != "user" {
				continue
			}
			encoded, _ := json.Marshal(item["content"])
			if len(encoded) > 0 && string(encoded) != "null" {
				return string(encoded)
			}
		}
	}
	return ""
}

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func randomBase62(n int) string {
	b := make([]byte, n)
	randBuf := make([]byte, n)
	if _, err := rand.Read(randBuf); err != nil {
		for i := 0; i < n; i++ {
			b[i] = base62Alphabet[time.Now().UnixNano()%int64(len(base62Alphabet))]
		}
		return string(b)
	}
	for i := 0; i < n; i++ {
		b[i] = base62Alphabet[randBuf[i]%byte(len(base62Alphabet))]
	}
	return string(b)
}

// encodeOpencodeID 生成符合 OpenCode 客户端指纹的标识。
// OpenCode 协议规定：
// 会话 ID (ses_) 为 48-bit 递减时间戳 (0-63 计数器保证 low12 >= 0xfc0)，后接 14 位 base62 尾部；
// 请求 ID (req_/msg_) 为 48-bit 递增时间戳，后接 14 位 base62 尾部。
func encodeOpencodeID(prefix string, descending bool) string {
	nowMs := time.Now().UnixMilli()
	var counterBuf [1]byte
	_, _ = rand.Read(counterBuf[:])
	counter := int64(counterBuf[0] & 0x3F) // 0..63

	val := nowMs*4096 + counter
	if descending {
		val = ^val
	}
	val = val & 0xFFFFFFFFFFFF

	hexPart := fmt.Sprintf("%012x", val)
	tail := randomBase62(14)
	return fmt.Sprintf("%s_%s%s", prefix, hexPart, tail)
}

// stableOpencodeID 根据特定 seed (如用户消息内容或外部会话ID) 确定性生成符合格式的 ID，
// 保证多次会话的 ID 既具备连续稳定性，又严格符合 OpenCode 的反脚本校验规则（嵌入动态滑动毫秒戳，确保时效性）。
func stableOpencodeID(prefix string, descending bool, seed string) string {
	sum := sha256.Sum256([]byte(seed))
	counter := int64(sum[0] & 0x3F) // 0..63
	nowMs := time.Now().UnixMilli()
	const windowMs = int64(10 * 60 * 1000) // 10 分钟滑动窗口
	baseTs := (nowMs / windowMs) * windowMs
	offset := (int64(sum[1])<<16 | int64(sum[2])<<8 | int64(sum[3])) % (windowMs / 2)
	ts := baseTs + offset
	val := ts*4096 + counter
	if descending {
		val = ^val
	}
	val = val & 0xFFFFFFFFFFFF

	hexPart := fmt.Sprintf("%012x", val)
	tail := make([]byte, 14)
	for i := 0; i < 14; i++ {
		tail[i] = base62Alphabet[sum[4+i]%byte(len(base62Alphabet))]
	}
	return fmt.Sprintf("%s_%s%s", prefix, hexPart, string(tail))
}

func stableID(prefix, value string) string {
	if prefix == "ses" {
		return stableOpencodeID(prefix, true, value)
	}
	sum := sha256.Sum256([]byte(prefix + "\x00" + value))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

func randomID(prefix string, size int) string {
	if prefix == "ses" {
		return encodeOpencodeID(prefix, true)
	}
	if prefix == "req" || prefix == "msg" {
		return encodeOpencodeID(prefix, false)
	}
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return prefix + "_" + fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf)
}

func firstString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func stringAt(value map[string]any, path ...string) string {
	current := any(value)
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[key]
	}
	result, _ := current.(string)
	return result
}

var (
	clientMu           sync.RWMutex
	defaultVersion     = "2.1.4"
	defaultPlatform    = "linux x64"
	defaultClientType  = "cli"
	defaultProject     = "global"

	configuredUA       string
	configuredVersion  string
	configuredClient   string
	configuredPlatform string
	configuredProject  string

	currentVersion     string
	currentUA          string
	lastFetchedVer     string
)

func init() {
	initClientFingerprint()
}

func initClientFingerprint() {
	clientMu.Lock()
	defer clientMu.Unlock()

	configuredUA = strings.TrimSpace(os.Getenv("OPENCODE_UA"))
	configuredVersion = strings.TrimSpace(os.Getenv("OPENCODE_VERSION"))
	configuredClient = strings.TrimSpace(os.Getenv("OPENCODE_CLIENT"))
	configuredPlatform = strings.TrimSpace(os.Getenv("OPENCODE_PLATFORM"))
	configuredProject = strings.TrimSpace(os.Getenv("OPENCODE_PROJECT"))

	if configuredVersion != "" {
		currentVersion = configuredVersion
	} else {
		currentVersion = defaultVersion
	}

	if configuredUA != "" {
		currentUA = configuredUA
	} else if configuredPlatform != "" {
		currentUA = fmt.Sprintf("opencode/%s (%s)", currentVersion, configuredPlatform)
	} else {
		currentUA = fmt.Sprintf("opencode/%s", currentVersion)
	}
}

func opencodeUserAgent() string {
	clientMu.RLock()
	defer clientMu.RUnlock()
	return currentUA
}

func opencodeVersion() string {
	clientMu.RLock()
	defer clientMu.RUnlock()
	return currentVersion
}

func opencodeClient() string {
	clientMu.RLock()
	defer clientMu.RUnlock()
	if configuredClient != "" {
		return configuredClient
	}
	return defaultClientType
}

func opencodeDefaultProject() string {
	clientMu.RLock()
	defer clientMu.RUnlock()
	if configuredProject != "" {
		return configuredProject
	}
	return defaultProject
}

func updateOpencodeUserAgent(version string) {
	version = strings.TrimSpace(version)
	if version == "" {
		return
	}
	clientMu.Lock()
	defer clientMu.Unlock()

	// 若用户通过环境变量锁定了版本和 UA，则不被定时拉取覆盖
	if configuredVersion != "" && configuredUA != "" {
		return
	}
	if version == lastFetchedVer {
		return
	}
	lastFetchedVer = version

	if configuredVersion == "" {
		currentVersion = version
	}

	if configuredUA == "" {
		if configuredPlatform != "" {
			currentUA = fmt.Sprintf("opencode/%s (%s)", currentVersion, configuredPlatform)
		} else {
			currentUA = fmt.Sprintf("opencode/%s", currentVersion)
		}
	}
	log.Printf("[UA] OpenCode 客户端指纹已自动同步为: %s (版本: %s)", currentUA, currentVersion)
}

func fetchLatestOpencodeVersion(ctx context.Context) (string, error) {
	reqContext, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqContext, http.MethodGet, "https://registry.npmjs.org/@opencode/cli/latest", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", opencodeUserAgent())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("npm registry 返回状态码 %d", resp.StatusCode)
	}

	var meta struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return "", err
	}
	return meta.Version, nil
}

// startUAFetcher 启动后台更新协程：启动时先探测一次，之后每 3 天自动检查并更新 OpenCode 最新版本号
func startUAFetcher(ctx context.Context) {
	go func() {
		if ver, err := fetchLatestOpencodeVersion(ctx); err == nil && ver != "" {
			updateOpencodeUserAgent(ver)
		} else if err != nil && ctx.Err() == nil {
			log.Printf("[UA] 初始化查询最新版本失败 (使用默认 %s): %v", opencodeUserAgent(), err)
		}

		ticker := time.NewTicker(3 * 24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ver, err := fetchLatestOpencodeVersion(ctx); err == nil && ver != "" {
					updateOpencodeUserAgent(ver)
				} else if err != nil && ctx.Err() == nil {
					log.Printf("[UA] 定时检查最新版本失败 (维持当前 %s): %v", opencodeUserAgent(), err)
				}
			}
		}
	}()
}


