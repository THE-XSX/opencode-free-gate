package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const maxRequestBody = 32 << 20

type app struct {
	gateway *gateway
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := loadConfig(currentProject())
	gw := newGateway(cfg)
	defer gw.direct.CloseIdleConnections()
	handler := &app{gateway: gw}

	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startUAFetcher(rootContext)
	gw.start(rootContext)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.port),
		Handler:           handler,
		ReadHeaderTimeout: cfg.hardTimeout,
		ReadTimeout:       cfg.hardTimeout,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	logStartup(cfg)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-rootContext.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[门] server failed: %v", err)
			stop()
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("[门] graceful shutdown failed: %v", err)
		_ = server.Close()
	}
}

func logStartup(cfg config) {
	log.Printf("[门] http://localhost:%d", cfg.port)
	log.Printf("[门] 项目:      %s", cfg.project.displayName)
	log.Printf("[门] 上游:      %s", cfg.project.upstream)
	if cfg.project.gatewayAuth {
		state := "未启用（任何人可访问）"
		if cfg.gatewayKey != "" {
			state = "已启用 GATEWAY_KEY"
		}
		log.Printf("[门] 认证:      %s", state)
	}
	log.Printf("[门] 模式:      %s", cfg.proxyMode)
	log.Printf("[门] 超时:      流式首字节 %s / 流式总预算 %s / 非流式最高 %s", cfg.firstByteTimeout, cfg.hardTimeout, cfg.nonStreamTimeout)
	layers := make([]string, 0, 4)
	for _, layer := range cfg.orderedLayers() {
		switch layer {
		case layerPublic:
			layers = append(layers, fmt.Sprintf("S级代理(%d槽,重试%d次)", cfg.slotCount, cfg.slotRetries))
		case layerZen:
			layers = append(layers, fmt.Sprintf("ZenProxy(%d次)", cfg.zenRetries))
		case layerCustom:
			layers = append(layers, "自定义代理")
		}
	}
	if cfg.project.directFallback {
		layers = append(layers, "直连")
	}
	log.Printf("[门] 策略:      %s", strings.Join(layers, " -> "))
}

func (a *app) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	trace := newRequestTrace()
	log.Printf("[>] %s %s", r.Method, r.URL.Path)
	if !a.authorized(r) {
		trace.finalStatus = http.StatusUnauthorized
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
		a.logCompletion(r, trace)
		return
	}

	if r.URL.Path == "/" || r.URL.Path == "/v1" {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":      "ok",
			"upstream":    a.gateway.cfg.project.upstream,
			"mode":        a.gateway.cfg.proxyMode,
			"slots":       a.gateway.slotAddresses(false),
			"customSlots": a.gateway.slotAddresses(true),
		})
		return
	}

	path, ok := normalizePath(a.gateway.cfg.project, r.URL.Path)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	deadline := time.Now().Add(a.gateway.cfg.hardTimeout)
	if path == "/v1/models" && r.Method == http.MethodGet {
		response, err := a.handleModels(r.Context(), r.Header, pathWithQuery(path, r.URL.RawQuery), deadline, trace)
		a.finish(w, r, trace, response, err)
		return
	}
	if _, allowed := a.gateway.cfg.project.postPaths[path]; allowed && r.Method == http.MethodPost {
		response, err := a.handlePost(w, r, pathWithQuery(path, r.URL.RawQuery), deadline, trace)
		a.finish(w, r, trace, response, err)
		return
	}

	writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
}

func (a *app) authorized(r *http.Request) bool {
	if !a.gateway.cfg.project.gatewayAuth || a.gateway.cfg.gatewayKey == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	parts := strings.Fields(auth)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	token := parts[1]
	if len(token) != len(a.gateway.cfg.gatewayKey) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(a.gateway.cfg.gatewayKey)) == 1
}

func normalizePath(project projectSpec, raw string) (string, bool) {
	if len(project.prefixes) == 0 {
		if strings.HasPrefix(raw, "/v1/") {
			return raw, true
		}
		return "", false
	}
	for _, prefix := range project.prefixes {
		base := "/" + prefix
		if strings.HasPrefix(raw, base+"/v1/") {
			return strings.TrimPrefix(raw, base), true
		}
	}
	return "", false
}

func pathWithQuery(path, rawQuery string) string {
	if rawQuery == "" {
		return path
	}
	return path + "?" + rawQuery
}

func (a *app) handleModels(ctx context.Context, sourceHeaders http.Header, path string, deadline time.Time, trace *requestTrace) (*gatewayResponse, error) {
	if a.gateway.cfg.project.modelMode != modelPassthrough {
		modelContext, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		trace.finalProxy = "local"
		return a.gateway.modelsResponse(modelContext), nil
	}
	request := upstreamRequest{
		method:   http.MethodGet,
		path:     path,
		headers:  a.collectHeaders(sourceHeaders),
		deadline: deadline,
	}
	return a.gateway.dispatch(ctx, request, trace)
}

func (a *app) handlePost(w http.ResponseWriter, r *http.Request, path string, deadline time.Time, trace *requestTrace) (*gatewayResponse, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return jsonGatewayResponse(http.StatusRequestEntityTooLarge, "请求体过大"), nil
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return jsonGatewayResponse(http.StatusGatewayTimeout, "请求超时"), nil
		}
		return jsonGatewayResponse(http.StatusBadRequest, "读取请求体失败"), nil
	}

	payload := parseJSONObject(body)
	clientModel, _ := payload["model"].(string)
	ids := deriveRequestIDs(r.Header, payload)
	clientStream := wantsStream(r.Header, payload)
	if !clientStream {
		deadline = trace.start.Add(a.gateway.cfg.nonStreamTimeout)
	}
	if clientStream {
		body = ensureStream(body, payload)
	}
	if a.gateway.cfg.project.modelMode != modelPassthrough {
		modelContext, cancel := context.WithDeadline(r.Context(), deadline)
		body = a.gateway.rewriteModel(modelContext, body)
		cancel()
		payload = parseJSONObject(body)
	}

	upstreamModel, _ := payload["model"].(string)
	isChatPath := strings.HasPrefix(path, "/v1/chat/completions")
	bridgeToResponses := isChatPath && (a.gateway.shouldUseResponses(upstreamModel) || a.gateway.shouldUseResponses(clientModel))

	requestPath := path
	requestBody := body
	upstreamStream := clientStream

	if bridgeToResponses {
		requestPath = strings.Replace(path, "/v1/chat/completions", "/v1/responses", 1)
		requestBody = convertChatToResponsesBody(body, payload)
		upstreamStream = true
		log.Printf("[协议桥接] %s: 命中规则，走 /v1/responses", upstreamModel)
	} else if strings.HasPrefix(path, "/v1/responses") {
		requestBody = sanitizeResponsesBody(body, payload)
	}

	headers := a.collectHeaders(r.Header)
	applyRequestIDs(headers, ids)
	if strings.HasPrefix(requestPath, "/v1/messages") {
		applyAnthropicAuth(headers)
	}
	if headers.Get("Accept") == "" {
		headers.Set("Accept", "application/json, text/event-stream")
	}

	buildUpstreamReq := func(targetPath string, targetBody []byte, isStream bool) upstreamRequest {
		h := headers.Clone()
		if isStream {
			h.Set("Accept", "text/event-stream")
		}
		return upstreamRequest{
			method:    http.MethodPost,
			path:      targetPath,
			headers:   h,
			body:      targetBody,
			stream:    isStream,
			nonStream: !isStream,
			session:   ids.Session,
			deadline:  deadline,
		}
	}

	response, err := a.gateway.dispatch(r.Context(), buildUpstreamReq(requestPath, requestBody, upstreamStream), trace)

	// 自动协议探测与自愈：如果未来新模型走 /v1/chat/completions 返回 400 ModelProtocolUnsupported，
	// 自动将该模型记入内存并当场切换为 /v1/responses 重试，对下游客户端完全无感。
	if err == nil && response != nil && !bridgeToResponses && isChatPath &&
		response.status == http.StatusBadRequest && isProtocolUnsupported(response.body) {

		a.gateway.markResponsesModel(upstreamModel)
		a.gateway.markResponsesModel(clientModel)
		bridgeToResponses = true
		requestPath = strings.Replace(path, "/v1/chat/completions", "/v1/responses", 1)
		requestBody = convertChatToResponsesBody(body, payload)
		upstreamStream = true

		log.Printf("[协议自愈] 检测到模型 %s 不支持 Chat 协议，已自动记忆并无缝切换至 /v1/responses 重试", upstreamModel)
		retryIDs := deriveRequestIDs(r.Header, nil)
		headers.Set("X-Opencode-Request", randomID("req", 16))
		response, err = a.gateway.dispatch(r.Context(), buildUpstreamReq(requestPath, requestBody, upstreamStream), trace)
	}

	if err != nil || response == nil {
		return response, err
	}
	if bridgeToResponses && response.status >= 200 && response.status < 400 {
		if clientModel == "" {
			clientModel = upstreamModel
		}
		if clientStream {
			response.responsesBridge = true
			response.bridgeModel = clientModel
		} else {
			return aggregateResponsesToChat(response, deadline, clientModel)
		}
	}
	return response, nil
}

// applyRequestIDs 用网关派生的标识覆盖发往上游的 OpenCode 头。
func applyRequestIDs(headers http.Header, ids requestIDs) {
	headers.Set("X-Opencode-Session", ids.Session)
	headers.Set("X-Opencode-Session-Id", ids.Session)
	headers.Set("X-Opencode-Request", ids.Request)
	headers.Set("X-Opencode-Project", ids.Project)
	headers.Set("X-Opencode-Version", opencodeVersion())
	headers.Set("X-Session-Affinity", ids.Session)
	headers.Set("X-Session-Id", ids.Session)
}

// applyAnthropicAuth 让 /v1/messages 与真实 OpenCode 客户端一致：
// 使用 x-api-key 而非 Authorization Bearer，并补齐默认 anthropic-version。
func applyAnthropicAuth(headers http.Header) {
	token := strings.TrimPrefix(headers.Get("Authorization"), "Bearer ")
	headers.Del("Authorization")
	if token != "" {
		headers.Set("X-Api-Key", token)
	}
	if headers.Get("Anthropic-Version") == "" {
		headers.Set("Anthropic-Version", "2023-06-01")
	}
}

func (a *app) collectHeaders(source http.Header) http.Header {
	result := make(http.Header)
	for _, key := range a.gateway.cfg.project.forwardHeaders {
		if source == nil {
			continue
		}
		for _, value := range source.Values(key) {
			result.Add(key, value)
		}
	}
	if auth := a.gateway.cfg.project.upstreamAuthorization; auth != "" {
		result.Set("Authorization", auth)
	}
	client := a.gateway.cfg.project.defaultClientHeader
	if client == "" {
		client = opencodeClient()
	}
	result.Set("X-Opencode-Client", client)
	result.Set("X-Opencode-Version", opencodeVersion())
	result.Set("User-Agent", opencodeUserAgent())
	if result.Get("Content-Type") == "" {
		result.Set("Content-Type", "application/json")
	}
	return result
}

func parseJSONObject(body []byte) map[string]any {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return nil
	}
	return payload
}

func wantsStream(headers http.Header, payload map[string]any) bool {
	if strings.Contains(strings.ToLower(headers.Get("Accept")), "event-stream") {
		return true
	}
	stream, _ := payload["stream"].(bool)
	return stream
}

func ensureStream(body []byte, payload map[string]any) []byte {
	if payload == nil {
		return body
	}
	if stream, _ := payload["stream"].(bool); stream {
		return body
	}
	payload["stream"] = true
	rewritten, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return rewritten
}

func (a *app) finish(w http.ResponseWriter, r *http.Request, trace *requestTrace, response *gatewayResponse, err error) {
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		switch {
		case errors.Is(err, errRequestTimeout), errors.Is(err, errAttemptTimeout), errors.Is(err, context.DeadlineExceeded):
			response = jsonGatewayResponse(http.StatusGatewayTimeout, "请求超时")
		case errors.Is(err, errNoProxy):
			response = jsonGatewayResponse(http.StatusBadGateway, "没有可用代理")
		default:
			response = jsonGatewayResponse(http.StatusBadGateway, "上游请求失败")
		}
		log.Printf("[请求错] %s %s: %v", r.Method, r.URL.Path, err)
	}
	if response == nil {
		response = jsonGatewayResponse(http.StatusBadGateway, "上游请求失败")
	}
	trace.finalStatus = response.status
	if trace.finalProxy == "" && trace.attempts == 0 {
		trace.finalProxy = "local"
	}
	// 完成日志放在转发真正结束之后：流式请求要等上游与客户端都结束，
	// 耗时和结束原因才是这一次请求的真实结果。
	outcome := writeGatewayResponse(w, r, response, a.gateway.cfg.streamIdle)
	trace.sentBytes = outcome.bytes
	trace.streamNote = outcome.note
	a.logCompletion(r, trace)
}

func (a *app) logCompletion(r *http.Request, trace *requestTrace) {
	status := trace.finalStatus
	result := "完成"
	if status < 200 || status >= 400 {
		result = "失败"
	}
	retries := trace.attempts - 1
	if retries < 0 {
		retries = 0
	}
	proxy := trace.finalProxy
	if proxy == "" {
		proxy = "unknown"
	}
	line := fmt.Sprintf("[%s] %s %s | 状态:%d | 重试:%d | 代理IP:%d | 耗时:%dms | 代理:%s | 发送:%dB",
		result, r.Method, r.URL.Path, status, retries, len(trace.proxies), time.Since(trace.start).Milliseconds(), proxy, trace.sentBytes)
	if trace.streamNote != "" {
		line += " | 流:" + trace.streamNote
	}
	log.Print(line)
}

// streamOutcome 记录一次转发的实际结果：写出字节数与异常结束原因。
type streamOutcome struct {
	bytes int64
	note  string
}

// deadlineWriter 在每次写入前刷新下游写截止时间。客户端停止读取时 Write/Flush 会一直阻塞，
// 既占住客户端连接也占住上游连接；服务端固定的 WriteTimeout 会把长流整体掐断，
// 因此这里按每次写入刷新，语义与上游读取的 idle 上限一致。
type deadlineWriter struct {
	w          io.Writer
	controller *http.ResponseController
	timeout    time.Duration
	written    int64
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	if d.timeout > 0 {
		_ = d.controller.SetWriteDeadline(time.Now().Add(d.timeout))
	}
	n, err := d.w.Write(p)
	d.written += int64(n)
	return n, err
}

func writeGatewayResponse(w http.ResponseWriter, r *http.Request, response *gatewayResponse, streamIdle time.Duration) streamOutcome {
	for key, values := range response.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if response.live != nil {
		w.Header().Del("Content-Length")
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		}
		if w.Header().Get("Cache-Control") == "" {
			w.Header().Set("Cache-Control", "no-cache")
		}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.WriteHeader(response.status)
	if response.live == nil {
		// 非流式响应体同样给下游写入一个空闲上限：客户端停止读取时，
		// 一次 Write 也可能一直阻塞并长期占住连接。
		body := &deadlineWriter{w: w, controller: http.NewResponseController(w), timeout: streamIdle}
		written, _ := body.Write(response.body)
		return streamOutcome{bytes: int64(written)}
	}
	if response.responsesBridge {
		return streamResponsesToChat(w, r.Context(), response.live, streamIdle, response.bridgeModel)
	}
	return streamResponse(w, r.Context(), response.live, streamIdle)
}

func streamResponse(w http.ResponseWriter, ctx context.Context, live *liveResponse, idle time.Duration) streamOutcome {
	defer live.Close()
	flusher, _ := w.(http.Flusher)
	downstream := &deadlineWriter{w: w, controller: http.NewResponseController(w), timeout: idle}
	if flusher != nil {
		flusher.Flush()
	}
	buffer := make([]byte, 32<<10)
	for {
		idleElapsed := make(chan struct{})
		timer := time.AfterFunc(idle, func() {
			close(idleElapsed)
			live.cancel()
		})
		n, err := live.response.Body.Read(buffer)
		stopped := timer.Stop()
		if n > 0 {
			if _, writeErr := downstream.Write(buffer[:n]); writeErr != nil {
				return streamOutcome{bytes: downstream.written, note: "客户端写入超时或断开"}
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if !stopped {
			<-idleElapsed
			log.Printf("[流] %s 内无数据，已关闭连接", idle)
			return streamOutcome{bytes: downstream.written, note: fmt.Sprintf("上游 %s 无数据", idle)}
		}
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				return streamOutcome{bytes: downstream.written}
			case ctx.Err() != nil:
				return streamOutcome{bytes: downstream.written, note: "客户端断开"}
			default:
				log.Printf("[流] upstream stream ended: %v", err)
				return streamOutcome{bytes: downstream.written, note: "上游中断"}
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// --- Responses API 协议探测与双向转换实现 ---

func isProtocolUnsupported(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	text := string(body)
	return strings.Contains(text, "ModelProtocolUnsupported") ||
		strings.Contains(text, "Model does not support this protocol")
}

func sanitizeResponsesBody(body []byte, payload map[string]any) []byte {
	if payload == nil {
		return body
	}
	model, _ := payload["model"].(string)
	if !strings.Contains(strings.ToLower(model), "muse-spark") {
		return body
	}
	delete(payload, "max_tokens")
	delete(payload, "max_completion_tokens")
	delete(payload, "max_output_tokens")
	delete(payload, "stream_options")
	if include, ok := payload["include"].([]any); ok {
		filtered := make([]any, 0, len(include))
		for _, item := range include {
			if s, ok := item.(string); ok && strings.Contains(s, "encrypted_content") {
				continue
			}
			filtered = append(filtered, item)
		}
		if len(filtered) > 0 {
			payload["include"] = filtered
		} else {
			delete(payload, "include")
		}
	}
	rewritten, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return rewritten
}

func convertChatToResponsesBody(body []byte, payload map[string]any) []byte {
	if payload == nil {
		return body
	}
	out := make(map[string]any)
	if model, ok := payload["model"]; ok {
		out["model"] = model
	}
	out["stream"] = true

	for _, key := range []string{"temperature", "top_p", "reasoning"} {
		if val, ok := payload[key]; ok {
			out[key] = val
		}
	}

	var instructions []string
	var inputItems []any
	if messages, ok := payload["messages"].([]any); ok {
		for _, rawMsg := range messages {
			msg, ok := rawMsg.(map[string]any)
			if !ok {
				continue
			}
			role, _ := msg["role"].(string)
			switch role {
			case "system", "developer":
				if text := extractMessageText(msg["content"]); text != "" {
					instructions = append(instructions, text)
				}
			case "assistant":
				if text := extractMessageText(msg["content"]); text != "" {
					inputItems = append(inputItems, map[string]any{
						"type": "message",
						"role": "assistant",
						"content": []map[string]any{
							{"type": "output_text", "text": text},
						},
					})
				}
				if toolCalls, ok := msg["tool_calls"].([]any); ok {
					for _, tcRaw := range toolCalls {
						tc, ok := tcRaw.(map[string]any)
						if !ok {
							continue
						}
						callID, _ := tc["id"].(string)
						fn, _ := tc["function"].(map[string]any)
						name, _ := fn["name"].(string)
						args, _ := fn["arguments"].(string)
						if name != "" {
							inputItems = append(inputItems, map[string]any{
								"type":      "function_call",
								"call_id":   callID,
								"name":      name,
								"arguments": args,
							})
						}
					}
				}
			case "tool":
				callID, _ := msg["tool_call_id"].(string)
				outputText := extractMessageText(msg["content"])
				inputItems = append(inputItems, map[string]any{
					"type":    "function_call_output",
					"call_id": callID,
					"output":  outputText,
				})
			default:
				parts := convertUserContentParts(msg["content"])
				if len(parts) > 0 {
					inputItems = append(inputItems, map[string]any{
						"type":    "message",
						"role":    "user",
						"content": parts,
					})
				}
			}
		}
	}

	if len(instructions) > 0 {
		out["instructions"] = strings.Join(instructions, "\n\n")
	}
	if len(inputItems) == 0 {
		inputItems = append(inputItems, map[string]any{
			"type": "message",
			"role": "user",
			"content": []map[string]any{
				{"type": "input_text", "text": "Hello"},
			},
		})
	}
	out["input"] = inputItems

	if tools, ok := payload["tools"].([]any); ok && len(tools) > 0 {
		convertedTools := make([]any, 0, len(tools))
		for _, rawTool := range tools {
			toolMap, ok := rawTool.(map[string]any)
			if !ok {
				continue
			}
			if fn, ok := toolMap["function"].(map[string]any); ok {
				flat := map[string]any{"type": "function"}
				for k, v := range fn {
					flat[k] = v
				}
				convertedTools = append(convertedTools, flat)
			} else {
				convertedTools = append(convertedTools, toolMap)
			}
		}
		if len(convertedTools) > 0 {
			out["tools"] = convertedTools
		}
	}
	if tc, ok := payload["tool_choice"]; ok {
		out["tool_choice"] = tc
	}

	rewritten, err := json.Marshal(out)
	if err != nil {
		return body
	}
	return rewritten
}

func extractMessageText(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		var sb strings.Builder
		for _, item := range v {
			if part, ok := item.(map[string]any); ok {
				if text, ok := part["text"].(string); ok {
					sb.WriteString(text)
				}
			}
		}
		return sb.String()
	default:
		return ""
	}
}

func convertUserContentParts(content any) []map[string]any {
	switch v := content.(type) {
	case string:
		if v == "" {
			return nil
		}
		return []map[string]any{{"type": "input_text", "text": v}}
	case []any:
		var parts []map[string]any
		for _, raw := range v {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			pType, _ := part["type"].(string)
			switch pType {
			case "text", "input_text":
				if text, ok := part["text"].(string); ok {
					parts = append(parts, map[string]any{"type": "input_text", "text": text})
				}
			case "image_url", "input_image":
				if imgObj, ok := part["image_url"].(map[string]any); ok {
					if u, ok := imgObj["url"].(string); ok && u != "" {
						parts = append(parts, map[string]any{"type": "input_image", "image_url": u})
					}
				} else if u, ok := part["image_url"].(string); ok && u != "" {
					parts = append(parts, map[string]any{"type": "input_image", "image_url": u})
				}
			}
		}
		return parts
	default:
		return nil
	}
}

func streamResponsesToChat(w http.ResponseWriter, ctx context.Context, live *liveResponse, idle time.Duration, model string) streamOutcome {
	defer live.Close()
	flusher, _ := w.(http.Flusher)
	downstream := &deadlineWriter{w: w, controller: http.NewResponseController(w), timeout: idle}
	if flusher != nil {
		flusher.Flush()
	}
	note := ""

	chatID := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	created := time.Now().Unix()
	sentRole := false
	sentText := false
	finishSent := false
	hasToolCall := false
	toolIndex := -1

	writeChunk := func(delta map[string]any, finishReason any) bool {
		if !sentRole && finishReason == nil {
			delta["role"] = "assistant"
			sentRole = true
		}
		chunk := map[string]any{
			"id":      chatID,
			"object":  "chat.completion.chunk",
			"created": created,
			"model":   model,
			"choices": []map[string]any{
				{
					"index":         0,
					"delta":         delta,
					"finish_reason": finishReason,
				},
			},
		}
		b, _ := json.Marshal(chunk)
		if _, err := fmt.Fprintf(downstream, "data: %s\n\n", b); err != nil {
			note = "客户端写入超时或断开"
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}

	scanner := bufio.NewScanner(live.response.Body)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)

	timer := time.AfterFunc(idle, func() {
		live.cancel()
	})
	defer timer.Stop()

	for scanner.Scan() {
		timer.Reset(idle)
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		dataStr := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if dataStr == "" {
			continue
		}
		if dataStr == "[DONE]" {
			break
		}

		var ev map[string]any
		if json.Unmarshal([]byte(dataStr), &ev) != nil {
			continue
		}
		evType, _ := ev["type"].(string)
		switch evType {
		case "response.output_text.delta":
			if delta, _ := ev["delta"].(string); delta != "" {
				sentText = true
				if !writeChunk(map[string]any{"content": delta}, nil) {
					return streamOutcome{bytes: downstream.written, note: note}
				}
			}
		case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
			if delta, _ := ev["delta"].(string); delta != "" {
				if !writeChunk(map[string]any{"reasoning_content": delta}, nil) {
					return streamOutcome{bytes: downstream.written, note: note}
				}
			}
		case "response.output_item.added":
			if item, ok := ev["item"].(map[string]any); ok {
				if itemType, _ := item["type"].(string); itemType == "function_call" {
					hasToolCall = true
					toolIndex++
					callID, _ := item["call_id"].(string)
					name, _ := item["name"].(string)
					tcDelta := map[string]any{
						"tool_calls": []map[string]any{
							{
								"index": toolIndex,
								"id":    callID,
								"type":  "function",
								"function": map[string]any{
									"name":      name,
									"arguments": "",
								},
							},
						},
					}
					if !writeChunk(tcDelta, nil) {
						return streamOutcome{bytes: downstream.written, note: note}
					}
				}
			}
		case "response.function_call_arguments.delta":
			if delta, _ := ev["delta"].(string); delta != "" {
				idx := toolIndex
				if idx < 0 {
					idx = 0
				}
				tcDelta := map[string]any{
					"tool_calls": []map[string]any{
						{
							"index": idx,
							"function": map[string]any{
								"arguments": delta,
							},
						},
					},
				}
				if !writeChunk(tcDelta, nil) {
					return streamOutcome{bytes: downstream.written, note: note}
				}
			}
		case "response.completed", "response.done":
			if !sentText {
				if respObj, ok := ev["response"].(map[string]any); ok {
					if fullText := extractCompletedResponseText(respObj); fullText != "" {
						sentText = true
						if !writeChunk(map[string]any{"content": fullText}, nil) {
							return streamOutcome{bytes: downstream.written, note: note}
						}
					}
				}
			}
			reason := "stop"
			if hasToolCall {
				reason = "tool_calls"
			}
			finishSent = true
			if !writeChunk(map[string]any{}, reason) {
				return streamOutcome{bytes: downstream.written, note: note}
			}
		}
	}

	if !finishSent {
		reason := "stop"
		if hasToolCall {
			reason = "tool_calls"
		}
		_ = writeChunk(map[string]any{}, reason)
	}
	_, _ = io.WriteString(downstream, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
	switch {
	case scanner.Err() != nil && ctx.Err() == nil:
		log.Printf("[流] responses stream ended: %v", scanner.Err())
		note = "上游中断"
	case ctx.Err() != nil && note == "":
		note = "客户端断开"
	case scanner.Err() != nil:
		note = "客户端断开"
	}
	return streamOutcome{bytes: downstream.written, note: note}
}

func aggregateResponsesToChat(response *gatewayResponse, deadline time.Time, model string) (*gatewayResponse, error) {
	var raw []byte
	var err error
	if response.live != nil {
		raw, err = response.live.readAll(deadline)
		response.live = nil
		if err != nil {
			return nil, err
		}
	} else {
		raw = response.body
	}

	var contentBuilder strings.Builder
	var reasoningBuilder strings.Builder
	var completedText string

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		dataStr := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if dataStr == "" || dataStr == "[DONE]" {
			continue
		}
		var ev map[string]any
		if json.Unmarshal([]byte(dataStr), &ev) != nil {
			continue
		}
		evType, _ := ev["type"].(string)
		switch evType {
		case "response.output_text.delta":
			if delta, _ := ev["delta"].(string); delta != "" {
				contentBuilder.WriteString(delta)
			}
		case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
			if delta, _ := ev["delta"].(string); delta != "" {
				reasoningBuilder.WriteString(delta)
			}
		case "response.completed", "response.done":
			if respObj, ok := ev["response"].(map[string]any); ok {
				completedText = extractCompletedResponseText(respObj)
			}
		}
	}

	finalText := contentBuilder.String()
	if finalText == "" {
		finalText = completedText
	}
	if finalText == "" {
		var respObj map[string]any
		if json.Unmarshal(raw, &respObj) == nil {
			finalText = extractCompletedResponseText(respObj)
		}
	}

	message := map[string]any{
		"role":    "assistant",
		"content": finalText,
	}
	if reasoningBuilder.Len() > 0 {
		message["reasoning_content"] = reasoningBuilder.String()
	}

	chatResp := map[string]any{
		"id":      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{
			{
				"index":         0,
				"message":       message,
				"finish_reason": "stop",
			},
		},
		"usage": map[string]any{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
	}

	body, _ := json.Marshal(chatResp)
	header := response.header.Clone()
	header.Set("Content-Type", "application/json; charset=utf-8")
	header.Del("Content-Length")
	return &gatewayResponse{
		status: http.StatusOK,
		header: header,
		body:   body,
	}, nil
}

func extractCompletedResponseText(respObj map[string]any) string {
	output, ok := respObj["output"].([]any)
	if !ok {
		return ""
	}
	var sb strings.Builder
	for _, rawItem := range output {
		item, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		if contentList, ok := item["content"].([]any); ok {
			for _, rawPart := range contentList {
				part, ok := rawPart.(map[string]any)
				if !ok {
					continue
				}
				if text, ok := part["text"].(string); ok {
					sb.WriteString(text)
				}
			}
		}
	}
	return sb.String()
}
