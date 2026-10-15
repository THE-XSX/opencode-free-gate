package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectTransportReusesConnections(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%v", http2), func(t *testing.T) {
			var connections atomic.Int32
			upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wantProtocol := 1
				if http2 {
					wantProtocol = 2
				}
				if r.ProtoMajor != wantProtocol {
					t.Errorf("protocol = %s, want HTTP/%d", r.Proto, wantProtocol)
				}
				_, _ = w.Write([]byte("ok"))
			}))
			upstream.EnableHTTP2 = http2
			upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			upstream.StartTLS()
			defer upstream.Close()
			gw := newGateway(config{project: projectSpec{upstream: upstream.URL}, firstByteTimeout: time.Second})
			defer gw.direct.CloseIdleConnections()
			for i := 0; i < 3; i++ {
				response, err := gw.perform(context.Background(), upstreamRequest{
					method: http.MethodGet, path: "/", headers: http.Header{}, nonStream: true, deadline: time.Now().Add(2 * time.Second),
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if string(response.body) != "ok" {
					t.Fatalf("body = %q", response.body)
				}
			}
			if got := connections.Load(); got != 1 {
				t.Fatalf("3 direct requests opened %d connections, want 1", got)
			}
		})
	}
}

func TestProxyAttemptsKeepIndependentConnections(t *testing.T) {
	var connections atomic.Int32
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	proxy.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	proxy.Start()
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	gw := newGateway(config{project: projectSpec{upstream: "http://upstream.invalid"}, firstByteTimeout: time.Second})
	defer gw.direct.CloseIdleConnections()
	for i := 0; i < 3; i++ {
		_, err := gw.perform(context.Background(), upstreamRequest{
			method: http.MethodGet, path: "/", headers: http.Header{}, nonStream: true, deadline: time.Now().Add(time.Second),
		}, proxyURL)
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 3 {
		t.Fatalf("3 proxy attempts opened %d connections, want 3", got)
	}
}

func TestDirectTimeoutDoesNotPoisonConnectionPool(t *testing.T) {
	var connections atomic.Int32
	canceled := make(chan struct{})
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			close(canceled)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	upstream.Start()
	defer upstream.Close()
	gw := newGateway(config{project: projectSpec{upstream: upstream.URL}, firstByteTimeout: 100 * time.Millisecond})
	defer gw.direct.CloseIdleConnections()
	_, err := gw.perform(context.Background(), upstreamRequest{
		method: http.MethodGet, path: "/slow", headers: http.Header{}, stream: true, deadline: time.Now().Add(time.Second),
	}, nil)
	if !errors.Is(err, errAttemptTimeout) {
		t.Fatalf("expected attempt timeout, got %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("timed-out direct request was not canceled")
	}
	for i := 0; i < 2; i++ {
		_, err := gw.perform(context.Background(), upstreamRequest{
			method: http.MethodGet, path: "/", headers: http.Header{}, nonStream: true, deadline: time.Now().Add(time.Second),
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("expected 1 discarded and 1 reused connection, got %d", got)
	}
}

func TestEmptyCandidateRefreshFetchesOnce(t *testing.T) {
	gw, calls := newPoolStubGateway(t, "null", nil)
	defer gw.direct.CloseIdleConnections()
	gw.refresh(context.Background())
	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh fetched the empty pool %d times, want 1", got)
	}
	if err := gw.fillSlots(context.Background()); !errors.Is(err, errNoProxy) {
		t.Fatalf("expected empty pool, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("immediate refill fetched the pool again: %d", got)
	}
	gw.mu.Lock()
	gw.candidatesLoadedAt = time.Now().Add(-candidateReloadDebounce)
	gw.mu.Unlock()
	_ = gw.fillSlots(context.Background())
	if got := calls.Load(); got != 2 {
		t.Fatalf("expired candidates should be fetched again: %d", got)
	}
}

func TestConcurrentCandidateLoadsAreDebounced(t *testing.T) {
	gw, calls := newPoolStubGateway(t, "null", nil)
	defer gw.direct.CloseIdleConnections()
	var workers sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if err := gw.loadCandidates(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	workers.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent loaders fetched the pool %d times, want 1", got)
	}
}

func TestFailedCandidateLoadIsNotImmediatelyRetried(t *testing.T) {
	var calls atomic.Int32
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer pool.Close()
	gw := newGateway(config{proxyAPI: pool.URL, slotCount: 3})
	defer gw.direct.CloseIdleConnections()
	if err := gw.loadCandidates(context.Background()); err == nil {
		t.Fatal("expected pool API error")
	}
	if err := gw.fillSlots(context.Background()); err == nil {
		t.Fatal("expected refill error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("load + refill fetched a failing API %d times, want 1", got)
	}
	gw.mu.Lock()
	gw.candidatesLoadedAt = time.Now().Add(-candidateReloadDebounce)
	gw.mu.Unlock()
	_ = gw.fillSlots(context.Background())
	if got := calls.Load(); got != 2 {
		t.Fatalf("failed load must be retryable after debounce: %d", got)
	}
}

func TestCandidateWaiterCancellationLeavesSharedLoadRunning(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			_, _ = w.Write([]byte("null"))
		case <-r.Context().Done():
		}
	}))
	defer pool.Close()
	defer unblock()
	gw := newGateway(config{proxyAPI: pool.URL})
	defer gw.direct.CloseIdleConnections()
	done := make(chan error, 1)
	go func() { done <- gw.loadCandidates(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pool load did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := gw.loadCandidates(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatalf("waiter canceled the shared fetch: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared load made %d API requests", got)
	}
}

func TestCanceledCandidateLoadAllowsImmediateRetry(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte("null"))
	}))
	defer pool.Close()
	gw := newGateway(config{proxyAPI: pool.URL})
	defer gw.direct.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gw.loadCandidates(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pool load did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("load error = %v", err)
	}
	if err := gw.loadCandidates(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("caller cancellation was cached; API calls = %d", got)
	}
}

func TestHTTP2CancelDoesNotCloseOtherStreams(t *testing.T) {
	var connections atomic.Int32
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("expected HTTP/2, got %s", r.Proto)
		}
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	upstream.EnableHTTP2 = true
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	upstream.StartTLS()
	defer upstream.Close()
	gw := newGateway(config{project: projectSpec{upstream: upstream.URL}, firstByteTimeout: time.Second})
	defer gw.direct.CloseIdleConnections()
	openStream := func() *liveResponse {
		t.Helper()
		live, err := gw.openUpstream(context.Background(), upstreamRequest{
			method: http.MethodGet, path: "/stream", headers: http.Header{}, deadline: time.Now().Add(time.Second),
		}, nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return live
	}
	first := openStream()
	defer first.Close()
	second := openStream()
	defer second.Close()
	first.Close()
	select {
	case <-second.response.Request.Context().Done():
		t.Fatal("canceling one stream canceled its sibling")
	default:
	}
	response, err := gw.perform(context.Background(), upstreamRequest{
		method: http.MethodGet, path: "/", headers: http.Header{}, nonStream: true, deadline: time.Now().Add(time.Second),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(response.body) != "ok" {
		t.Fatalf("response = %q", response.body)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("canceling a stream discarded the HTTP/2 connection: %d", got)
	}
}

// --- 补池不再占用请求预算 / 探活提前结束 / 并行预热 ---

// newPoolWithCandidatesGateway 构造一个公共池返回 payload、探活与转发都走 stub 的网关。
func newPoolWithCandidatesGateway(t *testing.T, payload string, configure func(*config)) (*gateway, *atomic.Int32) {
	t.Helper()
	var poolCalls atomic.Int32
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		poolCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	t.Cleanup(pool.Close)

	// 探活与转发都要经代理发出，所以上游用明文 HTTP 测试服务器：明文请求以绝对 URI
	// 直接发给 stub 代理，不需要 CONNECT 隧道，stub 可以直接应答。
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(upstream.Close)

	cfg := config{
		project:          projectSpec{upstream: upstream.URL, probePath: "/v1/models", probeHeaders: http.Header{}, directFallback: true},
		proxyAPI:         pool.URL,
		proxyMode:        "auto",
		slotCount:        3,
		slotRetries:      3,
		firstByteTimeout: time.Second,
		hardTimeout:      5 * time.Second,
		probeTimeout:     time.Second,
		poolEmptyBackoff: time.Minute,
		refreshInterval:  time.Minute,
	}
	if configure != nil {
		configure(&cfg)
	}
	gw := newGateway(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	gw.rootContext = ctx
	t.Cleanup(func() {
		cancel()
		gw.direct.CloseIdleConnections()
	})
	return gw, &poolCalls
}

// candidateFor 把一个测试服务器登记成 S 级候选。
func candidateFor(server *httptest.Server) string {
	return fmt.Sprintf(`[{"address":%q,"protocol":"http","latency":10,"quality_grade":"S","status":"active"}]`,
		strings.TrimPrefix(server.URL, "http://"))
}

func TestFillSlotsStopsProbingOnceSlotsAreFull(t *testing.T) {
	fast := make([]*httptest.Server, 0, 3)
	for i := 0; i < 3; i++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}))
		defer server.Close()
		fast = append(fast, server)
	}
	// 一个永远不响应的代理：旧实现会在这里等满探活超时。
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()

	gw, _ := newPoolWithCandidatesGateway(t, "null", func(cfg *config) {
		// 探活超时远大于断言窗口，确保失败来自"没有提前结束"。
		cfg.probeTimeout = 10 * time.Second
	})
	for _, server := range fast {
		gw.candidates = append(gw.candidates, proxyItem{Address: strings.TrimPrefix(server.URL, "http://"), Protocol: "http"})
	}
	gw.candidates = append(gw.candidates, proxyItem{Address: strings.TrimPrefix(slow.URL, "http://"), Protocol: "http"})

	started := time.Now()
	if err := gw.fillSlots(context.Background()); err != nil {
		t.Fatalf("fillSlots: %v", err)
	}
	elapsed := time.Since(started)
	if got := gw.slotCount(); got != 3 {
		t.Fatalf("expected 3 slots, got %d", got)
	}
	if elapsed > time.Second {
		t.Fatalf("fillSlots waited %s for a hanging proxy although all slots were full", elapsed)
	}
}

func TestColdPoolRequestUsesInFlightRefillInsteadOfWaitingFullProbeTimeout(t *testing.T) {
	// 池里只有一个很慢的代理：探活要 600ms 才返回，远慢于 poolFillWait。
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(600 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer slow.Close()

	var directCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		directCalls.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	original := poolFillWait
	poolFillWait = 150 * time.Millisecond
	t.Cleanup(func() { poolFillWait = original })

	gw, poolCalls := newPoolWithCandidatesGateway(t, candidateFor(slow), func(cfg *config) {
		cfg.project.upstream = upstream.URL
		cfg.probeTimeout = 5 * time.Second
	})

	started := time.Now()
	response, err := gw.dispatch(context.Background(), upstreamRequest{
		method:    http.MethodGet,
		path:      "/v1/chat/completions",
		headers:   http.Header{},
		nonStream: true,
		deadline:  started.Add(gw.cfg.hardTimeout),
	}, newRequestTrace())
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if response.status != http.StatusOK {
		t.Fatalf("expected direct 200, got %d", response.status)
	}
	if poolCalls.Load() == 0 {
		t.Fatal("expected the request path to trigger a background refill")
	}
	if got := directCalls.Load(); got != 1 {
		t.Fatalf("expected one direct fallback, got %d", got)
	}
	if elapsed < poolFillWait {
		t.Fatalf("expected the request to wait %s for the in-flight refill, returned after %s", poolFillWait, elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("request waited %s for a hanging probe instead of falling through", elapsed)
	}
}

func TestColdPoolRequestUsesProxyWhenRefillSucceeds(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer proxy.Close()

	original := poolFillWait
	poolFillWait = 2 * time.Second
	t.Cleanup(func() { poolFillWait = original })

	gw, _ := newPoolWithCandidatesGateway(t, candidateFor(proxy), nil)
	trace := newRequestTrace()
	response, err := gw.dispatch(context.Background(), upstreamRequest{
		method:    http.MethodGet,
		path:      "/v1/chat/completions",
		headers:   http.Header{},
		nonStream: true,
		deadline:  time.Now().Add(gw.cfg.hardTimeout),
	}, trace)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if response.status != http.StatusOK {
		t.Fatalf("expected 200 through the proxy layer, got %d", response.status)
	}
	want := strings.TrimPrefix(proxy.URL, "http://")
	if trace.finalProxy != want {
		t.Fatalf("expected the request to wait for the refilled slot %s, used %s", want, trace.finalProxy)
	}
}

func TestRequestWithoutFallbackLayerStillFillsSynchronously(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer proxy.Close()

	gw, poolCalls := newPoolWithCandidatesGateway(t, candidateFor(proxy), func(cfg *config) {
		cfg.project.directFallback = false
	})
	trace := newRequestTrace()
	response, err := gw.dispatch(context.Background(), upstreamRequest{
		method:    http.MethodGet,
		path:      "/v1/chat/completions",
		headers:   http.Header{},
		nonStream: true,
		deadline:  time.Now().Add(gw.cfg.hardTimeout),
	}, trace)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if response.status != http.StatusOK {
		t.Fatalf("expected 200 through the proxy layer, got %d", response.status)
	}
	// 没有后备层时必须是同步补池：请求返回时池已经被拉取过，槽位已就绪。
	if got := poolCalls.Load(); got == 0 {
		t.Fatal("expected a synchronous refill when no fallback layer exists")
	}
	if got := gw.slotCount(); got == 0 {
		t.Fatal("expected a ready slot when the request returned")
	}
}

func TestStartWarmsCustomSlotsWhilePoolIsSlow(t *testing.T) {
	poolStarted := make(chan struct{})
	var poolOnce sync.Once
	pool := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		poolOnce.Do(func() { close(poolStarted) })
		<-r.Context().Done()
	}))
	defer pool.Close()

	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer custom.Close()

	gw, _ := newPoolWithCandidatesGateway(t, "null", func(cfg *config) {
		cfg.proxyAPI = pool.URL
		cfg.customProxies = custom.URL
		cfg.proxyOrder = []string{layerCustom, layerPublic}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.start(ctx)

	select {
	case <-poolStarted:
	case <-time.After(time.Second):
		t.Fatal("public pool warmup did not start")
	}
	waitForCondition(t, "custom slots to warm up without waiting for the public pool", func() bool {
		return gw.customCount() > 0
	})
}

// --- 下游写入上限 ---

type infiniteZeroReader struct{}

func (infiniteZeroReader) Read(p []byte) (int, error) { return len(p), nil }

func TestStreamWriteDeadlineUnblocksStalledClient(t *testing.T) {
	const idle = 100 * time.Millisecond
	outcome := make(chan streamOutcome, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		live := &liveResponse{
			response: &http.Response{Body: io.NopCloser(io.LimitReader(infiniteZeroReader{}, 1<<30))},
			cancel:   cancel,
			teardown: func() {},
		}
		outcome <- streamResponse(w, ctx, live, idle)
	}))
	defer server.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	// 只发请求，不读取响应：写截止时间必须让 handler 自己退出。
	_, _ = fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")

	select {
	case got := <-outcome:
		if got.note != "客户端写入超时或断开" {
			t.Fatalf("expected a downstream write failure, got note %q", got.note)
		}
		if got.bytes == 0 {
			t.Fatal("expected some bytes to be written before the client stalled")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stalled client kept the streaming handler blocked past the write deadline")
	}
}

// logCapture 收集标准 logger 的输出；读写都加锁，避免与 handler 协程竞争。
type logCapture struct {
	mu   sync.Mutex
	text strings.Builder
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.text.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.text.String()
}

func TestCompletionLogIsWrittenAfterStreamEnds(t *testing.T) {
	capture := &logCapture{}
	log.SetOutput(capture)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"a\":1}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	cfg := config{
		project: projectSpec{
			upstream:       upstream.URL,
			directFallback: true,
			postPaths:      map[string]struct{}{"/v1/chat/completions": {}},
			modelMode:      modelPassthrough,
		},
		proxyMode:        "custom",
		firstByteTimeout: time.Second,
		hardTimeout:      5 * time.Second,
		streamIdle:       5 * time.Second,
	}
	gw := newGateway(cfg)
	defer gw.direct.CloseIdleConnections()
	application := &app{gateway: gw}

	done := make(chan struct{})
	go func() {
		defer close(done)
		application.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
			strings.NewReader(`{"model":"m","stream":true}`)))
	}()

	waitForCondition(t, "the streamed request to reach the upstream", func() bool {
		return strings.Contains(capture.String(), "[>] POST")
	})
	time.Sleep(200 * time.Millisecond)
	if strings.Contains(capture.String(), "[完成]") {
		t.Fatal("completion was logged before the stream finished")
	}

	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamed request did not finish")
	}

	text := capture.String()
	if !strings.Contains(text, "[完成]") {
		t.Fatalf("completion log missing from %q", text)
	}
	match := regexp.MustCompile(`耗时:(\d+)ms`).FindStringSubmatch(text)
	if match == nil {
		t.Fatalf("completion log has no duration: %q", text)
	}
	elapsed, err := time.ParseDuration(match[1] + "ms")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("completion log duration %s was recorded before the stream ended", elapsed)
	}
}

// blockingWriter 模拟停止读取的客户端：Write 会一直阻塞，直到写截止时间到期。
// 真实 TCP 的缓冲大小依赖操作系统，这里直接观察写截止时间是否被设置。
type blockingWriter struct {
	header   http.Header
	deadline atomic.Value
}

func (b *blockingWriter) Header() http.Header { return b.header }

func (b *blockingWriter) WriteHeader(int) {}

func (b *blockingWriter) Write(p []byte) (int, error) {
	for {
		due, _ := b.deadline.Load().(time.Time)
		if !due.IsZero() && !time.Now().Before(due) {
			return 0, os.ErrDeadlineExceeded
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (b *blockingWriter) SetWriteDeadline(deadline time.Time) error {
	b.deadline.Store(deadline)
	return nil
}

func TestNonStreamWriteDeadlineUnblocksStalledClient(t *testing.T) {
	const idle = 100 * time.Millisecond
	writer := &blockingWriter{header: http.Header{}}
	outcome := make(chan streamOutcome, 1)
	go func() {
		outcome <- writeGatewayResponse(writer, httptest.NewRequest(http.MethodGet, "/", nil), &gatewayResponse{
			status: http.StatusOK,
			header: http.Header{},
			body:   make([]byte, 1<<20),
		}, idle)
	}()

	select {
	case got := <-outcome:
		if got.bytes != 0 {
			t.Fatalf("expected the stalled write to stop at 0 bytes, wrote %d", got.bytes)
		}
		if due, _ := writer.deadline.Load().(time.Time); due.IsZero() {
			t.Fatal("expected the non-stream write path to set a downstream write deadline")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stalled client kept the non-stream response blocked past the write deadline")
	}
}
