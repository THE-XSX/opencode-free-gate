package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errAttemptTimeout = errors.New("代理首字节超时")
	errRequestTimeout = errors.New("请求总超时")
	errNoProxy        = errors.New("没有可用代理")
)

const maxUpstreamBody = 64 << 20

type proxyItem struct {
	Address      string `json:"address"`
	Protocol     string `json:"protocol"`
	Latency      int    `json:"latency"`
	QualityGrade string `json:"quality_grade"`
	Status       string `json:"status"`
}

type slot struct {
	addr     string
	proxyURL *url.URL
}

type requestTrace struct {
	start       time.Time
	attempts    int
	proxies     map[string]struct{}
	finalProxy  string
	finalStatus int
}

func newRequestTrace() *requestTrace {
	return &requestTrace{start: time.Now(), proxies: make(map[string]struct{})}
}

func (t *requestTrace) addAttempt(proxyName string) {
	t.attempts++
	t.proxies[proxyName] = struct{}{}
}

type gateway struct {
	cfg config

	mu         sync.RWMutex
	candidates []proxyItem
	slots      []slot
	custom     []slot

	fillMu          sync.Mutex
	refreshMu       sync.Mutex
	refillRunning   atomic.Bool
	rr              atomic.Uint64
	rootContext     context.Context
	modelMu         sync.Mutex
	modelCache      *cachedModels
	responsesModels sync.Map

	// 公共池为空的状态。上游池进入 emergency 后会持续返回空列表，
	// 记录状态是为了让请求路径在短时间内不再重复拉取。
	poolMu      sync.Mutex
	poolEmpty   bool
	poolEmptyAt time.Time
}

func newGateway(cfg config) *gateway {
	return &gateway{cfg: cfg}
}

func (g *gateway) start(ctx context.Context) {
	g.rootContext = ctx
	go func() {
		if g.cfg.usesPublicPool() {
			if err := g.loadCandidates(ctx); err != nil {
				log.Printf("[选] load failed: %v", err)
			}
			if err := g.fillSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[槽] initial fill failed: %v", err)
			}
		}
		if err := g.initCustomSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[兜底] initial fill failed: %v", err)
		}
		log.Printf("[门] 预热完成")
	}()

	if g.cfg.usesPublicPool() {
		go func() {
			// 启动时如果代理池为空，每 10 秒快速重试直到有可用代理
			retryTicker := time.NewTicker(10 * time.Second)
			defer retryTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-retryTicker.C:
					if g.slotCount() == 0 {
						_ = g.loadCandidates(ctx)
						_ = g.fillSlots(ctx)
					}
				}
			}
		}()

		go func() {
			ticker := time.NewTicker(g.cfg.refreshInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.refresh(ctx)
				}
			}
		}()
	}
}

func (g *gateway) refresh(ctx context.Context) {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	if err := g.loadCandidates(ctx); err != nil {
		log.Printf("[刷新] candidate load failed: %v", err)
		return
	}
	if err := g.fillSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[刷新] slot fill failed: %v", err)
	}
}

func (g *gateway) loadCandidates(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.cfg.proxyAPI, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Transport: controlTransport(5 * time.Second)}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("proxy API returned %d", res.StatusCode)
	}

	var all []proxyItem
	if err := json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&all); err != nil {
		return err
	}
	var sGrade, aGrade, bGrade []proxyItem
	for _, item := range all {
		if item.Status != "active" {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.QualityGrade)) {
		case "S":
			sGrade = append(sGrade, item)
		case "A", "A+":
			aGrade = append(aGrade, item)
		case "B":
			bGrade = append(bGrade, item)
		}
	}
	sort.SliceStable(sGrade, func(i, j int) bool { return sGrade[i].Latency < sGrade[j].Latency })
	sort.SliceStable(aGrade, func(i, j int) bool { return aGrade[i].Latency < aGrade[j].Latency })
	sort.SliceStable(bGrade, func(i, j int) bool { return bGrade[i].Latency < bGrade[j].Latency })

	filtered := append(sGrade, aGrade...)
	if len(filtered) < g.cfg.slotCount {
		filtered = append(filtered, bGrade...)
	}

	g.mu.Lock()
	g.candidates = filtered
	g.mu.Unlock()
	log.Printf("[选] 从池获取到 %d 个原始代理 -> %d 个候选 (%d S级, %d A级, %d B级)", len(all), len(filtered), len(sGrade), len(aGrade), len(bGrade))
	g.notePoolResult(len(all), len(filtered))
	return nil
}

// poolBackoffActive 报告公共池是否处于"空池退避"窗口内。上游池处于 emergency
// 时对 /api/proxies 返回的是 HTTP 200 + null（不是错误），窗口内再密集重试也只会
// 让每个请求都白白消耗自己的一部分超时预算去换一个空列表，因此只在窗口外拉取。
func (g *gateway) poolBackoffActive() bool {
	if g.cfg.poolEmptyBackoff <= 0 {
		return false
	}
	g.poolMu.Lock()
	defer g.poolMu.Unlock()
	return g.poolEmpty && time.Since(g.poolEmptyAt) < g.cfg.poolEmptyBackoff
}

// notePoolResult 记录一次候选拉取的结果。候选为空（原始列表为空，或全部被等级
// 过滤掉）时进入退避窗口，并且只在状态翻转时打日志，避免空池期间每轮刷新刷屏。
func (g *gateway) notePoolResult(raw, candidates int) {
	empty := candidates == 0
	g.poolMu.Lock()
	wasEmpty := g.poolEmpty
	g.poolEmpty = empty
	if empty {
		g.poolEmptyAt = time.Now()
	}
	g.poolMu.Unlock()

	switch {
	case empty && !wasEmpty:
		log.Printf("[选] 池为空: 原始代理 %d 个 -> 候选 0 个, 进入 %s 退避窗口 (窗口内不再随请求同步拉取; 未配置 ZENPROXY_KEY/CUSTOM_PROXIES 时流量将退化为直连)",
			raw, g.cfg.poolEmptyBackoff)
	case !empty && wasEmpty:
		log.Printf("[选] 池已恢复: 原始代理 %d 个 -> 候选 %d 个, 退出退避窗口", raw, candidates)
	}
}

func controlTransport(timeout time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          8,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
}

func (g *gateway) fillSlots(ctx context.Context) error {
	if !g.fillMu.TryLock() {
		return nil
	}
	defer g.fillMu.Unlock()

	needed := g.cfg.slotCount - g.slotCount()
	if needed <= 0 {
		return nil
	}

	batch := g.takeCandidates(needed + 3)
	if len(batch) == 0 {
		if err := g.loadCandidates(ctx); err != nil {
			return err
		}
		batch = g.takeCandidates(needed + 3)
	}
	if len(batch) == 0 {
		return errNoProxy
	}

	type probeResult struct {
		slot    slot
		latency time.Duration
		ok      bool
	}
	results := make(chan probeResult, len(batch))
	var wg sync.WaitGroup
	for _, item := range batch {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := slotFromProxy(item.Address, item.Protocol)
			if err != nil {
				results <- probeResult{}
				return
			}
			started := time.Now()
			ok := g.probe(ctx, s)
			results <- probeResult{slot: s, latency: time.Since(started), ok: ok}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	added := 0
	for result := range results {
		if !result.ok || g.slotCount() >= g.cfg.slotCount {
			continue
		}
		if g.addSlot(result.slot, false) {
			added++
			log.Printf("[探+] %s (%dms)", result.slot.addr, result.latency.Milliseconds())
		}
	}
	log.Printf("[槽] %d/%d ready (added %d)", g.slotCount(), g.cfg.slotCount, added)
	return nil
}

func (g *gateway) initCustomSlots(ctx context.Context) error {
	parsed := parseCustomProxies(g.cfg.customProxies)
	if len(parsed) == 0 {
		return nil
	}

	type probeResult struct {
		slot    slot
		latency time.Duration
		ok      bool
	}
	results := make(chan probeResult, len(parsed))
	var wg sync.WaitGroup
	for _, candidate := range parsed {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			ok := g.probe(ctx, candidate)
			results <- probeResult{slot: candidate, latency: time.Since(started), ok: ok}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	ready := 0
	for result := range results {
		if result.ok && g.addSlot(result.slot, true) {
			ready++
			log.Printf("[兜底+] %s (%dms)", result.slot.addr, result.latency.Milliseconds())
		}
	}
	log.Printf("[兜底] %d/%d custom proxies ready", ready, len(parsed))
	return nil
}

func parseCustomProxies(raw string) []slot {
	parts := strings.Split(raw, ",")
	result := make([]slot, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		s, err := slotFromRawURL(part)
		if err != nil {
			log.Printf("[配置] 忽略无效代理 %q: %v", redactProxy(part), err)
			continue
		}
		result = append(result, s)
	}
	return result
}

func slotFromProxy(address, protocol string) (slot, error) {
	scheme := "http"
	if strings.HasPrefix(strings.ToLower(protocol), "socks5") {
		scheme = "socks5h"
	}
	return slotFromRawURL(scheme + "://" + address)
}

func slotFromRawURL(raw string) (slot, error) {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return slot{}, err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	case "socks5", "socks5h":
		u.Scheme = "socks5h"
	default:
		return slot{}, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return slot{}, errors.New("missing proxy host")
	}
	return slot{addr: u.Host, proxyURL: u}, nil
}

func redactProxy(raw string) string {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	u.User = nil
	return u.String()
}

func (g *gateway) probe(ctx context.Context, candidate slot) bool {
	deadline := time.Now().Add(g.cfg.probeTimeout)
	headers := g.cfg.project.probeHeaders.Clone()
	headers.Set("User-Agent", opencodeUserAgent())
	request := upstreamRequest{
		method:   http.MethodGet,
		path:     g.cfg.project.probePath,
		headers:  headers,
		deadline: deadline,
	}
	live, err := g.openUpstream(ctx, request, candidate.proxyURL, g.cfg.probeTimeout)
	if err != nil {
		return false
	}
	status := live.response.StatusCode
	live.Close()
	return status >= 200 && status < 400
}

func (g *gateway) takeCandidates(limit int) []proxyItem {
	g.mu.Lock()
	defer g.mu.Unlock()
	used := make(map[string]struct{}, len(g.slots))
	for _, current := range g.slots {
		used[current.addr] = struct{}{}
	}

	result := make([]proxyItem, 0, limit)
	for len(g.candidates) > 0 && len(result) < limit {
		item := g.candidates[0]
		g.candidates = g.candidates[1:]
		if _, exists := used[item.Address]; exists {
			continue
		}
		used[item.Address] = struct{}{}
		result = append(result, item)
	}
	return result
}

func (g *gateway) addSlot(candidate slot, custom bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	target := &g.slots
	if custom {
		target = &g.custom
	}
	for _, existing := range *target {
		if existing.addr == candidate.addr {
			return false
		}
	}
	*target = append(*target, candidate)
	return true
}

func (g *gateway) slotCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.slots)
}

func (g *gateway) slotAddresses(custom bool) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	source := g.slots
	if custom {
		source = g.custom
	}
	result := make([]string, 0, len(source))
	for _, current := range source {
		result = append(result, current.addr)
	}
	return result
}

// nextSlot 选取下一个代理槽位。session 非空时使用 rendezvous 哈希，
// 让同一会话在槽位存活期间固定同一出口（匿名通道按出口 IP 限流），
// 槽位增删只影响映射到该槽位的会话；session 为空时保持轮询行为。
func (g *gateway) nextSlot(custom bool, tried map[string]struct{}, session string, attempt int) (slot, bool) {
	g.mu.RLock()
	source := g.slots
	if custom {
		source = g.custom
	}
	snapshot := append([]slot(nil), source...)
	g.mu.RUnlock()
	if len(snapshot) == 0 {
		return slot{}, false
	}

	if session != "" {
		sort.SliceStable(snapshot, func(i, j int) bool {
			return rendezvousScore(session, snapshot[i].addr) > rendezvousScore(session, snapshot[j].addr)
		})
		if custom {
			return snapshot[attempt%len(snapshot)], true
		}
		for _, candidate := range snapshot {
			if _, exists := tried[candidate.addr]; !exists {
				return candidate, true
			}
		}
		return slot{}, false
	}

	start := int(g.rr.Add(1)-1) % len(snapshot)
	for offset := 0; offset < len(snapshot); offset++ {
		candidate := snapshot[(start+offset)%len(snapshot)]
		if custom {
			return candidate, true
		}
		if _, exists := tried[candidate.addr]; !exists {
			return candidate, true
		}
	}
	return slot{}, false
}

func rendezvousScore(session, addr string) uint64 {
	sum := sha256.Sum256([]byte(session + "\x00" + addr))
	return binary.BigEndian.Uint64(sum[:8])
}

func (g *gateway) dropSlot(address string) {
	g.mu.Lock()
	removed := false
	for i, current := range g.slots {
		if current.addr == address {
			g.slots = append(g.slots[:i], g.slots[i+1:]...)
			removed = true
			break
		}
	}
	remaining := len(g.slots)
	g.mu.Unlock()
	if !removed {
		return
	}
	log.Printf("[弃] %s -> %d/%d", address, remaining, g.cfg.slotCount)
	g.scheduleFill()
}

func (g *gateway) scheduleFill() {
	if g.rootContext == nil || !g.refillRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer g.refillRunning.Store(false)
		ctx, cancel := context.WithTimeout(g.rootContext, g.cfg.probeTimeout+5*time.Second)
		defer cancel()
		if err := g.fillSlots(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[槽] refill failed: %v", err)
		}
	}()
}

type upstreamRequest struct {
	method    string
	path      string
	headers   http.Header
	body      []byte
	stream    bool
	nonStream bool
	session   string
	deadline  time.Time
}

type gatewayResponse struct {
	status          int
	header          http.Header
	body            []byte
	live            *liveResponse
	responsesBridge bool
	bridgeModel     string
}

type liveResponse struct {
	response  *http.Response
	cancel    context.CancelFunc
	transport *http.Transport
	once      sync.Once
}

func (r *liveResponse) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.response != nil && r.response.Body != nil {
			_ = r.response.Body.Close()
		}
		r.cancel()
		r.transport.CloseIdleConnections()
	})
}

func (r *liveResponse) readAll(deadline time.Time) ([]byte, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		r.Close()
		return nil, errRequestTimeout
	}
	timedOut := make(chan struct{})
	timer := time.AfterFunc(remaining, func() {
		close(timedOut)
		r.cancel()
	})
	body, err := io.ReadAll(io.LimitReader(r.response.Body, maxUpstreamBody+1))
	stopped := timer.Stop()
	r.Close()
	if !stopped {
		<-timedOut
		return nil, errRequestTimeout
	}
	if err != nil {
		return nil, err
	}
	if len(body) > maxUpstreamBody {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", maxUpstreamBody)
	}
	return body, nil
}

func (g *gateway) openUpstream(ctx context.Context, request upstreamRequest, proxyURL *url.URL, maxWait time.Duration) (*liveResponse, error) {
	target := strings.TrimRight(g.cfg.project.upstream, "/") + request.path
	wait, connectTimeout, responseHeaderTimeout := g.openTimeouts(request.deadline, maxWait)
	return openHTTP(ctx, request.method, target, request.headers, request.body, proxyURL, wait, connectTimeout, responseHeaderTimeout)
}

func (g *gateway) openTimeouts(deadline time.Time, firstByteLimit time.Duration) (time.Duration, time.Duration, time.Duration) {
	if firstByteLimit > 0 {
		wait := boundedWait(deadline, firstByteLimit)
		return wait, wait, wait
	}
	wait := boundedWait(deadline, 0)
	connectTimeout := boundedWait(deadline, g.cfg.firstByteTimeout)
	return wait, connectTimeout, 0
}

func openHTTP(ctx context.Context, method, target string, headers http.Header, body []byte, proxyURL *url.URL, wait, connectTimeout, responseHeaderTimeout time.Duration) (*liveResponse, error) {
	if wait <= 0 {
		return nil, errRequestTimeout
	}
	if connectTimeout <= 0 || connectTimeout > wait {
		connectTimeout = wait
	}
	if responseHeaderTimeout < 0 {
		responseHeaderTimeout = 0
	}
	if responseHeaderTimeout > wait {
		responseHeaderTimeout = wait
	}

	attemptContext, cancel := context.WithCancel(ctx)
	timedOut := make(chan struct{})
	timer := time.AfterFunc(wait, func() {
		close(timedOut)
		cancel()
	})

	transport := requestTransport(proxyURL, connectTimeout, responseHeaderTimeout)
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(attemptContext, method, target, bytes.NewReader(body))
	if err != nil {
		timer.Stop()
		cancel()
		transport.CloseIdleConnections()
		return nil, err
	}
	req.Header = headers.Clone()
	req.Header.Del("Host")
	req.Header.Del("Content-Length")

	res, err := client.Do(req)
	stopped := timer.Stop()
	if err != nil {
		cancel()
		transport.CloseIdleConnections()
		select {
		case <-timedOut:
			return nil, errAttemptTimeout
		default:
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if !stopped {
		<-timedOut
		_ = res.Body.Close()
		cancel()
		transport.CloseIdleConnections()
		return nil, errAttemptTimeout
	}
	return &liveResponse{response: res, cancel: cancel, transport: transport}, nil
}

func requestTransport(proxyURL *url.URL, connectTimeout, responseHeaderTimeout time.Duration) *http.Transport {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: -1,
		}).DialContext,
		ForceAttemptHTTP2:      false,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		TLSHandshakeTimeout:    connectTimeout,
		ResponseHeaderTimeout:  responseHeaderTimeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, // 保持旧网关行为；上游与代理链可能使用非标准证书。
		},
	}
	if proxyURL != nil {
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	return transport
}

func boundedWait(deadline time.Time, maximum time.Duration) time.Duration {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}
	if maximum <= 0 || remaining < maximum {
		return remaining
	}
	return maximum
}

func (g *gateway) perform(ctx context.Context, request upstreamRequest, proxyURL *url.URL) (*gatewayResponse, error) {
	firstByteLimit := g.cfg.firstByteTimeout
	if request.nonStream {
		firstByteLimit = 0
	}
	live, err := g.openUpstream(ctx, request, proxyURL, firstByteLimit)
	if err != nil {
		if errors.Is(err, errAttemptTimeout) && time.Until(request.deadline) <= 0 {
			return nil, errRequestTimeout
		}
		return nil, err
	}
	status := live.response.StatusCode
	header := cloneEndToEndHeaders(live.response.Header)
	if request.stream && status < 400 {
		return &gatewayResponse{status: status, header: header, live: live}, nil
	}
	body, err := live.readAll(request.deadline)
	if err != nil {
		return nil, err
	}
	return &gatewayResponse{status: status, header: header, body: body}, nil
}

func (g *gateway) dispatch(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	if g.cfg.forceRelay {
		if g.cfg.zenKey == "" {
			return jsonGatewayResponse(http.StatusBadGateway, "FORCE_RELAY 但未配置 ZENPROXY_KEY"), nil
		}
		return g.dispatchZen(ctx, request, trace)
	}

	var last *gatewayResponse
	for _, layer := range g.cfg.orderedLayers() {
		var response *gatewayResponse
		var err error
		switch layer {
		case layerPublic:
			response, err = g.dispatchPublicLayer(ctx, request, trace)
		case layerZen:
			if g.cfg.zenKey == "" {
				continue
			}
			response, err = g.dispatchZen(ctx, request, trace)
		case layerCustom:
			if g.customCount() == 0 {
				continue
			}
			response, err = g.dispatchCustomLayer(ctx, request, trace)
		default:
			continue
		}
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			if !errors.Is(err, errNoProxy) {
				log.Printf("[层错] %s: %v", layer, err)
			}
			continue
		}
		if !retryableStatus(response.status) {
			return response, nil
		}
		last = response
	}

	if g.cfg.project.directFallback {
		return g.dispatchDirect(ctx, request, trace)
	}
	if last != nil {
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) dispatchPublicLayer(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	// 槽位为空时补池。空池退避窗口内跳过这次同步拉取：候选列表注定为空，
	// 却要让当前请求在自己的超时预算里等一次完整的 HTTPS 往返，
	// 补池交给后台的 PROXY_REFRESH_MS 刷新与启动期重试协程即可。
	if g.slotCount() == 0 && !g.poolBackoffActive() {
		fillContext, cancel := context.WithDeadline(ctx, request.deadline)
		_ = g.fillSlots(fillContext)
		cancel()
	}

	var last *gatewayResponse
	lastProxy := ""
	tried := make(map[string]struct{})
	for retry := 0; retry < g.cfg.slotRetries; retry++ {
		if err := requestBudgetError(ctx, request.deadline); err != nil {
			return nil, err
		}
		candidate, ok := g.nextSlot(false, tried, request.session, retry)
		if !ok {
			break
		}
		tried[candidate.addr] = struct{}{}
		trace.addAttempt(candidate.addr)
		log.Printf("[S级] %s (%d/%d)", candidate.addr, retry+1, g.cfg.slotRetries)
		response, err := g.perform(ctx, request, candidate.proxyURL)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			log.Printf("[错] %s: %v", candidate.addr, err)
			g.dropSlot(candidate.addr)
			continue
		}
		if !retryableStatus(response.status) {
			trace.finalProxy = candidate.addr
			return response, nil
		}
		log.Printf("[错码] %s 状态码 %d", candidate.addr, response.status)
		if shouldDropSlot(response.status, response.body) {
			g.dropSlot(candidate.addr)
		}
		last = response
		lastProxy = candidate.addr
	}
	if last != nil {
		trace.finalProxy = lastProxy
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) dispatchCustomLayer(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	maxRetries := g.cfg.customRetries
	if maxRetries == 0 {
		maxRetries = g.customCount()
	}
	var last *gatewayResponse
	lastProxy := ""
	for retry := 0; retry < maxRetries; retry++ {
		if err := requestBudgetError(ctx, request.deadline); err != nil {
			return nil, err
		}
		candidate, ok := g.nextSlot(true, nil, request.session, retry)
		if !ok {
			break
		}
		trace.addAttempt(candidate.addr)
		log.Printf("[自定义] %s (%d/%d)", candidate.addr, retry+1, maxRetries)
		response, err := g.perform(ctx, request, candidate.proxyURL)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			log.Printf("[错] %s: %v", candidate.addr, err)
			continue
		}
		if !retryableStatus(response.status) {
			trace.finalProxy = candidate.addr
			return response, nil
		}
		log.Printf("[错码] %s 状态码 %d", candidate.addr, response.status)
		last = response
		lastProxy = candidate.addr
	}
	if last != nil {
		trace.finalProxy = lastProxy
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) dispatchZen(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	retries := g.cfg.zenRetries
	if retries < 1 {
		retries = 1
	}
	var last *gatewayResponse
	for retry := 0; retry < retries; retry++ {
		if err := requestBudgetError(ctx, request.deadline); err != nil {
			return nil, err
		}
		trace.addAttempt("ZenProxy")
		log.Printf("[ZenProxy] (%d/%d)", retry+1, retries)
		response, err := g.performRelay(ctx, request)
		if err != nil {
			if isTerminalContextError(ctx, err) {
				return nil, err
			}
			log.Printf("[ZenProxy] 错误: %v", err)
			continue
		}
		if !retryableStatus(response.status) {
			trace.finalProxy = "ZenProxy"
			return response, nil
		}
		log.Printf("[ZenProxy] 状态码 %d，重试", response.status)
		last = response
	}
	if last != nil {
		trace.finalProxy = "ZenProxy"
		return last, nil
	}
	return nil, errNoProxy
}

func (g *gateway) performRelay(ctx context.Context, request upstreamRequest) (*gatewayResponse, error) {
	relay, err := url.Parse(g.cfg.zenRelay)
	if err != nil {
		return nil, err
	}
	target := strings.TrimRight(g.cfg.project.upstream, "/") + request.path
	query := relay.Query()
	query.Set("api_key", g.cfg.zenKey)
	query.Set("url", target)
	query.Set("method", request.method)
	relay.RawQuery = query.Encode()

	headers := request.headers.Clone()
	headers.Del("Host")
	headers.Del("Content-Length")
	headers.Del("Authorization")
	firstByteLimit := g.cfg.firstByteTimeout
	if request.nonStream {
		firstByteLimit = 0
	}
	wait, connectTimeout, responseHeaderTimeout := g.openTimeouts(request.deadline, firstByteLimit)
	live, err := openHTTP(ctx, http.MethodPost, relay.String(), headers, request.body, nil, wait, connectTimeout, responseHeaderTimeout)
	if err != nil {
		if errors.Is(err, errAttemptTimeout) && time.Until(request.deadline) <= 0 {
			return nil, errRequestTimeout
		}
		return nil, err
	}
	status := live.response.StatusCode
	header := cloneEndToEndHeaders(live.response.Header)
	if request.stream && status < 400 {
		return &gatewayResponse{status: status, header: header, live: live}, nil
	}
	body, err := live.readAll(request.deadline)
	if err != nil {
		return nil, err
	}
	return &gatewayResponse{status: status, header: header, body: body}, nil
}

func (g *gateway) dispatchDirect(ctx context.Context, request upstreamRequest, trace *requestTrace) (*gatewayResponse, error) {
	trace.addAttempt("direct")
	log.Printf("[直连] directly connecting to upstream")
	response, err := g.perform(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	trace.finalProxy = "direct"
	return response, nil
}

func (g *gateway) customCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.custom)
}

// shouldUseResponses 判断模型是否需要走 /v1/responses 协议（优先查自动学习的内存表，其次查已知前缀）
func (g *gateway) shouldUseResponses(model string) bool {
	lower := strings.ToLower(strings.TrimSpace(model))
	if lower == "" {
		return false
	}
	if _, ok := g.responsesModels.Load(lower); ok {
		return true
	}
	return strings.Contains(lower, "muse-spark")
}

// markResponsesModel 将探测到不支持 Chat 协议的模型永久记入内存
func (g *gateway) markResponsesModel(model string) {
	lower := strings.ToLower(strings.TrimSpace(model))
	if lower != "" {
		g.responsesModels.Store(lower, struct{}{})
	}
}

func requestBudgetError(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if time.Until(deadline) <= 0 {
		return errRequestTimeout
	}
	return nil
}

func isTerminalContextError(ctx context.Context, err error) bool {
	return errors.Is(err, errRequestTimeout) || ctx.Err() != nil
}

func retryableStatus(status int) bool {
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests {
		return true
	}
	return status >= 500 && status <= 599
}

// shouldDropSlot 当上游返回包含明确业务 error JSON 的 500 时（模型自身偶发异常而非代理故障），保留健康代理槽位。
func shouldDropSlot(status int, body []byte) bool {
	if status == http.StatusInternalServerError && len(body) > 0 && bytes.Contains(body, []byte(`"error"`)) {
		return false
	}
	return retryableStatus(status)
}

func jsonGatewayResponse(status int, message string) *gatewayResponse {
	body, _ := json.Marshal(map[string]string{"error": message})
	return &gatewayResponse{
		status: status,
		header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		body:   body,
	}
}

func cloneEndToEndHeaders(source http.Header) http.Header {
	result := make(http.Header, len(source))
	for key, values := range source {
		if hopByHopHeader(key) {
			continue
		}
		result[key] = append([]string(nil), values...)
	}
	return result
}

func hopByHopHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}