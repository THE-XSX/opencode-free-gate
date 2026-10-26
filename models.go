package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

const modelCacheTTL = 60 * time.Second

// modelWarmWait 是冷启动时改写请求体愿意等待首次模型刷新的上限。模型列表本身
// （wait=0）可以立即用内置兜底返回，但改写请求体必须先有映射，否则会把展示名
// 当成上游模型名发出去；这里给正在进行的刷新一个有界窗口，超时仍用兜底映射。
const modelWarmWait = 800 * time.Millisecond

type cachedModels struct {
	rename   map[string]string
	redirect map[string]string
	loadedAt time.Time
}

// modelMaps 返回模型映射。wait 是冷启动时等待首次刷新落地的时间上限：
// 列模型（wait=0）拿到兜底结果就能用，改写请求体则需要真实映射才不会发错模型名。
func (g *gateway) modelMaps(ctx context.Context, wait time.Duration) (map[string]string, map[string]string) {
	g.modelMu.Lock()
	cached := g.modelCache
	if cached != nil {
		fresh := time.Since(cached.loadedAt) < modelCacheTTL
		rename := cloneStringMap(cached.rename)
		redirect := cloneStringMap(cached.redirect)
		g.modelMu.Unlock()
		if fresh {
			return rename, redirect
		}
		// 缓存过期：先返回旧值，同时单飞发起后台刷新，避免并发请求在锁上排队等 8 秒。
		g.startModelRefresh()
		return rename, redirect
	}
	g.modelMu.Unlock()

	// 冷缓存：刷新交给后台，任何时候都不占用整段请求预算。
	done := g.startModelRefresh()
	if wait > 0 {
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < wait {
				wait = remaining
			}
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-done:
			case <-timer.C:
			case <-ctx.Done():
			}
			timer.Stop()
		}
	}

	g.modelMu.Lock()
	defer g.modelMu.Unlock()
	if g.modelCache != nil {
		return cloneStringMap(g.modelCache.rename), cloneStringMap(g.modelCache.redirect)
	}
	return fallbackModelMaps(g.cfg)
}

// startModelRefresh 保证同一时刻只有一个模型表刷新在跑，返回该次刷新的完成信号。
func (g *gateway) startModelRefresh() <-chan struct{} {
	g.modelMu.Lock()
	defer g.modelMu.Unlock()
	if g.modelRefresh != nil {
		return g.modelRefresh
	}
	done := make(chan struct{})
	// 另一个请求可能已在读取缓存与进入此函数之间完成刷新。
	if g.modelCache != nil && time.Since(g.modelCache.loadedAt) < modelCacheTTL {
		close(done)
		return done
	}
	g.modelRefresh = done
	go func() {
		g.refreshModelCacheAsync()
		g.modelMu.Lock()
		g.modelRefresh = nil
		close(done)
		g.modelMu.Unlock()
	}()
	return done
}

// fallbackModelMaps 是模型表还没拿到时的最小可用映射：只有内置的模型别名。
func fallbackModelMaps(cfg config) (map[string]string, map[string]string) {
	rename := cloneStringMap(cfg.project.specialModels)
	return rename, buildRedirect(rename)
}

func (g *gateway) refreshModelCacheAsync() {
	parent := g.rootContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	_, _ = g.refreshModelCacheSync(ctx)
}

func (g *gateway) refreshModelCacheSync(ctx context.Context) (map[string]string, map[string]string) {
	rename, err := g.fetchModelMaps(ctx)
	g.modelMu.Lock()
	defer g.modelMu.Unlock()

	if err != nil {
		if g.modelCache != nil {
			log.Printf("[模型] 刷新失败，使用缓存: %v", err)
			g.modelCache.loadedAt = time.Now().Add(-(modelCacheTTL - 5*time.Second))
			return cloneStringMap(g.modelCache.rename), cloneStringMap(g.modelCache.redirect)
		}
		log.Printf("[模型] 刷新失败且无缓存: %v", err)
		rename, redirect := fallbackModelMaps(g.cfg)
		g.modelCache = &cachedModels{
			rename:   cloneStringMap(rename),
			redirect: cloneStringMap(redirect),
			loadedAt: time.Now().Add(-(modelCacheTTL - 5*time.Second)),
		}
		return rename, redirect
	}

	redirect := buildRedirect(rename)
	g.modelCache = &cachedModels{rename: rename, redirect: redirect, loadedAt: time.Now()}
	log.Printf("[模型] 已刷新 %d 个免费模型", len(rename))
	return cloneStringMap(rename), cloneStringMap(redirect)
}

func (g *gateway) fetchModelMaps(parent context.Context) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()

	target := g.cfg.project.modelPath
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = strings.TrimRight(g.cfg.project.upstream, "/") + target
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", opencodeUserAgent())
	req.Header.Set("X-Opencode-Client", opencodeClient())
	req.Header.Set("X-Opencode-Version", opencodeVersion())
	if auth := g.cfg.project.upstreamAuthorization; auth != "" {
		req.Header.Set("Authorization", auth)
	}

	client := &http.Client{Transport: controlTransport(8 * time.Second)}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("model API returned %d", res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	ids, err := extractModelIDs(body)
	if err != nil {
		return nil, err
	}

	rename := cloneStringMap(g.cfg.project.specialModels)
	if rename == nil {
		rename = make(map[string]string)
	}
	for _, id := range ids {
		switch g.cfg.project.modelMode {
		case modelKilo:
			if !strings.HasSuffix(id, ":free") {
				continue
			}
			trimmed := strings.TrimSuffix(id, ":free")
			parts := strings.Split(trimmed, "/")
			rename[id] = parts[len(parts)-1]
		case modelOpenCode:
			if strings.HasSuffix(id, "-free") {
				name := strings.TrimSuffix(id, "-free")
				if strings.HasPrefix(name, "jev") {
					continue
				}
				rename[id] = name
			}
		}
	}
	return rename, nil
}

func extractModelIDs(body []byte) ([]string, error) {
	var root any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, err
	}
	if object, ok := root.(map[string]any); ok {
		root = object["data"]
	}
	items, ok := root.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected model response")
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		switch value := item.(type) {
		case string:
			ids = append(ids, value)
		case map[string]any:
			if id, ok := value["id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

func (g *gateway) modelsResponse(ctx context.Context) *gatewayResponse {
	// 列模型不需要真实映射：兜底列表里的展示名本身可用。
	rename, _ := g.modelMaps(ctx, 0)
	unique := make(map[string]struct{}, len(rename)+len(g.cfg.project.extraModels))
	for _, display := range rename {
		unique[display] = struct{}{}
	}
	for _, model := range g.cfg.project.extraModels {
		unique[model] = struct{}{}
	}
	ids := make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	created := time.Now().Unix()
	models := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		models = append(models, map[string]any{
			"id":       id,
			"object":   "model",
			"created":  created,
			"owned_by": g.cfg.project.ownedBy,
		})
	}
	body, _ := json.Marshal(map[string]any{"object": "list", "data": models})
	return &gatewayResponse{
		status: http.StatusOK,
		header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		body:   body,
	}
}

func (g *gateway) rewriteModel(ctx context.Context, body []byte) []byte {
	_, redirect := g.modelMaps(ctx, modelWarmWait)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}
	model, _ := payload["model"].(string)
	upstream, exists := redirect[model]
	if !exists {
		switch strings.ToLower(strings.TrimSpace(model)) {
		case "nemotron-3-ultra", "nemotron-3.5-ultra", "nemotron-ultra", "nemotron":
			upstream = "nemotron-3.5-super-vl-free"
			exists = true
		case "mimo", "mimo-v2.6":
			upstream = "mimo-v2.6-flash-free"
			exists = true
		case "glm-5.3", "glm-5.3-chat":
			upstream = "glm-5.3-flash-free"
			exists = true
		case "deepseek-v4.1", "deepseek-v4-flash":
			upstream = "deepseek-v4.1-flash-free"
			exists = true
		case "jev", "jev-1.13":
			upstream = "mimo-v2.6-flash-free"
			exists = true
		}
	}
	if !exists {
		return body
	}
	payload["model"] = upstream
	rewritten, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	log.Printf("[模型重定向] %s -> %s", model, upstream)
	return rewritten
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func buildRedirect(rename map[string]string) map[string]string {
	upstreamIDs := make([]string, 0, len(rename))
	for upstream := range rename {
		upstreamIDs = append(upstreamIDs, upstream)
	}
	sort.Strings(upstreamIDs)
	redirect := make(map[string]string, len(rename))
	for _, upstream := range upstreamIDs {
		redirect[rename[upstream]] = upstream
	}
	return redirect
}
