package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Keep the model endpoint blocked until the test releases it; request latency
// must not depend on this endpoint completing.
func newBlockedModelGateway(t *testing.T) (*gateway, *atomic.Int32, <-chan struct{}, func()) {
	t.Helper()
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			calls.Add(1)
			startOnce.Do(func() { close(started) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"new-model-free"},{"id":"paid-model"}]}`))
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["model"] != "special-free" {
			t.Errorf("model was not rewritten with fallback map: %v", body["model"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	cfg := config{
		project: currentProject(), proxyMode: "custom", firstByteTimeout: time.Second,
		hardTimeout: 3 * time.Second, nonStreamTimeout: 3 * time.Second, streamIdle: time.Second,
	}
	cfg.project.upstream = upstream.URL
	cfg.project.specialModels = map[string]string{"special-free": "special"}
	cfg.project.extraModels = []string{"big-pickle"}
	gw := newGateway(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	gw.rootContext = ctx
	t.Cleanup(func() {
		cancel()
		unblock()
		upstream.Close()
		gw.direct.CloseIdleConnections()
		gw.modelMu.Lock()
		done := gw.modelRefresh
		gw.modelMu.Unlock()
		if done != nil {
			<-done
		}
	})
	return gw, &calls, started, unblock
}

func TestColdModelsReturnFallbackWithoutWaiting(t *testing.T) {
	gw, calls, started, release := newBlockedModelGateway(t)
	recorder := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		(&app{gateway: gw}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/openai/v1/models", nil))
	}()
	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		release()
		<-finished
		t.Fatal("cold model list waited for the blocked upstream instead of returning fallback")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	ids, err := extractModelIDs(recorder.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []string{"big-pickle", "special"}) {
		t.Fatalf("fallback models = %v", ids)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}
	done := gw.startModelRefresh()
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not finish")
	}
	rename, redirect := gw.modelMaps(context.Background(), 0)
	if rename["new-model-free"] != "new-model" || redirect["new-model"] != "new-model-free" {
		t.Fatalf("missing refreshed model: %v / %v", rename, redirect)
	}
	if _, exists := rename["paid-model"]; exists {
		t.Fatal("paid model was included")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one model refresh, got %d", got)
	}
}

func TestColdStreamingPostWaitsOnlyForBoundedModelWarmup(t *testing.T) {
	// 模型接口一直不返回：改写请求体最多等 modelWarmWait，然后用兜底映射继续，
	// 既不会被整次刷新（8s）拖住，也不会把展示名当成上游模型名发出去。
	gw, _, _, release := newBlockedModelGateway(t)
	recorder := httptest.NewRecorder()
	finished := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(finished)
		request := httptest.NewRequest(http.MethodPost, "/openai/v1/chat/completions", strings.NewReader(`{"model":"special","stream":true,"messages":[]}`))
		(&app{gateway: gw}).ServeHTTP(recorder, request)
	}()
	var elapsed time.Duration
	select {
	case <-finished:
		elapsed = time.Since(started)
	case <-time.After(3 * time.Second):
		release()
		<-finished
		t.Fatal("cold streaming POST waited for the whole model refresh instead of modelWarmWait")
	}
	if elapsed < modelWarmWait {
		t.Fatalf("冷启动改写只等了 %s，没有给首次刷新 %s 的落地窗口", elapsed, modelWarmWait)
	}
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "[DONE]") {
		t.Fatalf("streaming response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestStartPrewarmsModelCacheAndCancelsRefresh(t *testing.T) {
	gw, _, started, _ := newBlockedModelGateway(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.start(ctx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("start did not prewarm models")
	}
	done := gw.startModelRefresh()
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("model prewarm ignored gateway shutdown")
	}
}

func TestConcurrentStaleModelRequestsUseOneRefresh(t *testing.T) {
	gw, calls, started, release := newBlockedModelGateway(t)
	gw.modelCache = &cachedModels{
		rename: map[string]string{"cached-free": "cached"}, redirect: map[string]string{"cached": "cached-free"},
		loadedAt: time.Now().Add(-modelCacheTTL),
	}
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			rename, redirect := gw.modelMaps(context.Background(), 0)
			if rename["cached-free"] != "cached" || redirect["cached"] != "cached-free" {
				t.Error("stale cache was not returned")
			}
			rename["cached-free"] = "local mutation"
		}()
	}
	workers.Wait()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stale cache did not trigger refresh")
	}
	done := gw.startModelRefresh()
	release()
	<-done
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent stale requests triggered %d refreshes", got)
	}
}

func TestFailedModelRefreshKeepsUsableMapsAndRetries(t *testing.T) {
	for _, stale := range []bool{false, true} {
		name := "cold"
		if stale {
			name = "stale"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			var recovered atomic.Bool
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if !recovered.Load() {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				_, _ = w.Write([]byte(`{"data":[{"id":"recovered-free"}]}`))
			}))
			defer upstream.Close()
			cfg := config{project: currentProject()}
			cfg.project.upstream = upstream.URL
			cfg.project.specialModels = map[string]string{"special-free": "special"}
			gw := newGateway(cfg)
			defer gw.direct.CloseIdleConnections()
			expected := "special"
			if stale {
				expected = "cached"
				gw.modelCache = &cachedModels{
					rename: map[string]string{"cached-free": "cached"}, redirect: map[string]string{"cached": "cached-free"},
					loadedAt: time.Now().Add(-modelCacheTTL),
				}
			}
			done := gw.startModelRefresh()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("failed refresh did not finish")
			}
			rename, redirect := gw.modelMaps(context.Background(), 0)
			if rename[expected+"-free"] != expected || redirect[expected] != expected+"-free" {
				t.Fatalf("refresh failure lost usable maps: %v / %v", rename, redirect)
			}
			<-gw.startModelRefresh()
			if got := calls.Load(); got != 1 {
				t.Fatalf("failure cooldown did not suppress refresh: %d", got)
			}
			recovered.Store(true)
			gw.modelMu.Lock()
			gw.modelCache.loadedAt = time.Now().Add(-modelCacheTTL)
			gw.modelMu.Unlock()
			<-gw.startModelRefresh()
			rename, _ = gw.modelMaps(context.Background(), 0)
			if rename["recovered-free"] != "recovered" {
				t.Fatalf("cache failed to recover: %v", rename)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("expected one recovery refresh, got %d calls", got)
			}
		})
	}
}

func TestColdRewriteUsesListedModelMappingAfterShortWait(t *testing.T) {
	// 冷启动 + 模型列表很快返回：改写必须等到这次刷新，用列表里的真实映射，
	// 而不是把展示名原样发出去。
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/models" {
			_, _ = w.Write([]byte(`{"data":[{"id":"new-model-free"},{"id":"paid-model"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := config{project: projectSpec{
		upstream:    upstream.URL,
		modelPath:   "/v1/models",
		modelMode:   modelOpenCode,
		extraModels: []string{"big-pickle"},
	}}
	gw := newGateway(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	gw.rootContext = ctx
	t.Cleanup(func() {
		cancel()
		gw.direct.CloseIdleConnections()
	})

	rewritten := gw.rewriteModel(context.Background(), []byte(`{"model":"new-model","stream":false}`))
	if got, _ := parseJSONObject(rewritten)["model"].(string); got != "new-model-free" {
		t.Fatalf("冷启动改写后的模型 = %q, 期望 new-model-free", got)
	}

	// 非免费模型不在映射里，保持原样。
	unchanged := gw.rewriteModel(context.Background(), []byte(`{"model":"paid-model","stream":false}`))
	if got, _ := parseJSONObject(unchanged)["model"].(string); got != "paid-model" {
		t.Fatalf("非免费模型被改写为 %q", got)
	}
}
