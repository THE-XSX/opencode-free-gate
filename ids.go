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
// 会话 ID 优先取客户端显式标识，否则用第一条用户消息生成稳定哈希，
// 保证同一段多轮对话在历史增长时仍映射到同一个会话。
func deriveRequestIDs(headers http.Header, body map[string]any) requestIDs {
	signal := firstString(
		headers.Get("x-opencode-session"),
		headers.Get("x-session-id"),
		headers.Get("conversation-id"),
		stringAt(body, "conversation_id"),
		stringAt(body, "metadata", "session_id"),
	)
	if signal == "" {
		signal = conversationSeed(body)
	}
	if signal == "" {
		signal = stringAt(body, "previous_response_id")
	}
	if signal == "" || signal == "{}" {
		signal = randomID("fallback", 16)
	}
	projectSignal := firstString(headers.Get("x-opencode-project"), stringAt(body, "metadata", "project_id"))
	if projectSignal == "" {
		projectSignal = "opencode-free-gate:default-project"
	}
	return requestIDs{
		Session: stableID("ses", signal),
		Request: randomID("req", 16),
		Project: stableID("prj", projectSignal),
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

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + value))
	if prefix == "ses" {
		return prefix + "_" + hex.EncodeToString(sum[:])
	}
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

func randomID(prefix string, size int) string {
	if prefix == "ses" {
		size = 32
	}
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		if prefix == "ses" {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
			return prefix + "_" + hex.EncodeToString(sum[:])
		}
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
	cachedUAMu     sync.RWMutex
	cachedUA       = "opencode/2.0.29 (linux x64)"
	lastFetchedVer string
)

func opencodeUserAgent() string {
	cachedUAMu.RLock()
	defer cachedUAMu.RUnlock()
	return cachedUA
}

func updateOpencodeUserAgent(version string) {
	version = strings.TrimSpace(version)
	if version == "" {
		return
	}
	cachedUAMu.Lock()
	defer cachedUAMu.Unlock()
	if version == lastFetchedVer {
		return
	}
	lastFetchedVer = version
	cachedUA = fmt.Sprintf("opencode/%s (linux x64)", version)
	log.Printf("[UA] OpenCode 客户端指纹已自动同步为: %s", cachedUA)
}

func fetchLatestOpencodeVersion(ctx context.Context) (string, error) {
	reqContext, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqContext, http.MethodGet, "https://registry.npmjs.org/@opencode/cli/latest", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")

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

