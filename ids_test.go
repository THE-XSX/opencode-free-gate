package main

import (
	"net/http"
	"strings"
	"testing"
)

func TestDeriveRequestIDsStableAcrossTurns(t *testing.T) {
	turn1 := map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "hello world"},
		},
	}
	turn2 := map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "hello world"},
			map[string]any{"role": "assistant", "content": "hi"},
			map[string]any{"role": "user", "content": "second turn"},
		},
	}
	first := deriveRequestIDs(http.Header{}, turn1)
	second := deriveRequestIDs(http.Header{}, turn2)
	if first.Session != second.Session {
		t.Fatalf("same conversation must keep one session: %q vs %q", first.Session, second.Session)
	}
	if first.Request == second.Request {
		t.Fatal("each client request must get a fresh request id")
	}

	other := deriveRequestIDs(http.Header{}, map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "different opener"}},
	})
	if other.Session == first.Session {
		t.Fatal("different conversations must not share a session")
	}
}

func TestDeriveRequestIDsHonorsExplicitSession(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Session-Id", "session-a")
	bodyA := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "same opener"}}}
	bodyB := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "same opener"}}}

	withHeader := deriveRequestIDs(headers, bodyA)
	otherHeader := http.Header{}
	otherHeader.Set("X-Session-Id", "session-b")
	withOther := deriveRequestIDs(otherHeader, bodyB)
	if withHeader.Session == withOther.Session {
		t.Fatal("explicit session ids must separate identical openers")
	}
	if !strings.HasPrefix(withHeader.Session, "ses_") {
		t.Fatalf("session id must use the opencode format: %q", withHeader.Session)
	}
	if !strings.HasPrefix(withHeader.Request, "req_") || !strings.HasPrefix(withHeader.Project, "prj_") {
		t.Fatalf("unexpected id formats: %q %q", withHeader.Request, withHeader.Project)
	}
}

func TestDeriveRequestIDsResponsesInput(t *testing.T) {
	viaInput := deriveRequestIDs(http.Header{}, map[string]any{"input": "codex prompt"})
	again := deriveRequestIDs(http.Header{}, map[string]any{"input": "codex prompt"})
	if viaInput.Session != again.Session {
		t.Fatal("responses input string must yield a stable session")
	}

	viaPrevious := deriveRequestIDs(http.Header{}, map[string]any{"previous_response_id": "resp_123"})
	againPrevious := deriveRequestIDs(http.Header{}, map[string]any{"previous_response_id": "resp_123"})
	if viaPrevious.Session != againPrevious.Session {
		t.Fatal("previous_response_id must yield a stable session")
	}
}

func TestApplyAnthropicAuth(t *testing.T) {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer public")
	applyAnthropicAuth(headers)
	if headers.Get("Authorization") != "" {
		t.Fatal("anthropic requests must not carry Authorization")
	}
	if got := headers.Get("X-Api-Key"); got != "public" {
		t.Fatalf("anthropic requests must use x-api-key: %q", got)
	}
	if got := headers.Get("Anthropic-Version"); got != "2023-06-01" {
		t.Fatalf("missing default anthropic-version: %q", got)
	}

	custom := http.Header{}
	custom.Set("Authorization", "Bearer public")
	custom.Set("Anthropic-Version", "2024-01-01")
	applyAnthropicAuth(custom)
	if got := custom.Get("Anthropic-Version"); got != "2024-01-01" {
		t.Fatalf("client anthropic-version must be preserved: %q", got)
	}
}

func TestSessionAffinityPrefersStableSlot(t *testing.T) {
	gw := newGateway(config{})
	for _, address := range []string{"proxy-1", "proxy-2", "proxy-3", "proxy-4", "proxy-5"} {
		gw.slots = append(gw.slots, slot{addr: address})
	}

	first, ok := gw.nextSlot(false, map[string]struct{}{}, "ses_alpha", 0)
	if !ok {
		t.Fatal("expected a slot for the session")
	}
	for range 10 {
		again, ok := gw.nextSlot(false, map[string]struct{}{}, "ses_alpha", 0)
		if !ok || again.addr != first.addr {
			t.Fatalf("session must stick to %s, got %s", first.addr, again.addr)
		}
	}

	tried := map[string]struct{}{first.addr: {}}
	fallback, ok := gw.nextSlot(false, tried, "ses_alpha", 1)
	if !ok || fallback.addr == first.addr {
		t.Fatalf("failed slot must be skipped, got %s", fallback.addr)
	}

	distinct := make(map[string]struct{})
	for _, session := range []string{"ses_a", "ses_b", "ses_c", "ses_d", "ses_e", "ses_f", "ses_g", "ses_h"} {
		selected, ok := gw.nextSlot(false, map[string]struct{}{}, session, 0)
		if !ok {
			t.Fatal("expected a slot")
		}
		distinct[selected.addr] = struct{}{}
	}
	if len(distinct) < 2 {
		t.Fatal("different sessions should spread across slots")
	}
}

func TestDeriveRequestIDsPreservesValidOpenCodeIDs(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Opencode-Session", "ses_custom_session_1234567890")
	headers.Set("X-Opencode-Request", "req_custom_request_9876543210")
	headers.Set("X-Opencode-Project", "global")

	ids := deriveRequestIDs(headers, nil)
	if ids.Session != "ses_custom_session_1234567890" {
		t.Fatalf("expected preserved session id, got %q", ids.Session)
	}
	if ids.Request != "req_custom_request_9876543210" {
		t.Fatalf("expected preserved request id, got %q", ids.Request)
	}
	if ids.Project != "global" {
		t.Fatalf("expected preserved global project, got %q", ids.Project)
	}

	// Test msg_ request prefix and prj_ project prefix preservation
	headers2 := http.Header{}
	headers2.Set("X-Opencode-Session", "ses_turn_alpha")
	headers2.Set("X-Opencode-Request", "msg_message_112233")
	headers2.Set("X-Opencode-Project", "prj_my_workspace")

	ids2 := deriveRequestIDs(headers2, nil)
	if ids2.Session != "ses_turn_alpha" {
		t.Fatalf("expected preserved session, got %q", ids2.Session)
	}
	if ids2.Request != "msg_message_112233" {
		t.Fatalf("expected preserved msg_ request, got %q", ids2.Request)
	}
	if ids2.Project != "prj_my_workspace" {
		t.Fatalf("expected preserved prj_ project, got %q", ids2.Project)
	}
}

func TestClientFingerprintEnvOverrides(t *testing.T) {
	t.Cleanup(func() {
		initClientFingerprint()
	})

	t.Setenv("OPENCODE_VERSION", "2.1.0")
	t.Setenv("OPENCODE_PLATFORM", "darwin arm64")
	t.Setenv("OPENCODE_CLIENT", "desktop")
	t.Setenv("OPENCODE_PROJECT", "global")
	t.Setenv("OPENCODE_UA", "")

	initClientFingerprint()

	if got := opencodeVersion(); got != "2.1.0" {
		t.Fatalf("expected version 2.1.0, got %q", got)
	}
	if got := opencodeClient(); got != "desktop" {
		t.Fatalf("expected client desktop, got %q", got)
	}
	if got := opencodeDefaultProject(); got != "global" {
		t.Fatalf("expected default project global, got %q", got)
	}
	if got := opencodeUserAgent(); got != "opencode/2.1.0 (darwin arm64)" {
		t.Fatalf("expected UA opencode/2.1.0 (darwin arm64), got %q", got)
	}

	// Explicit OPENCODE_UA override
	t.Setenv("OPENCODE_UA", "custom-opencode-agent/9.9.9")
	initClientFingerprint()
	if got := opencodeUserAgent(); got != "custom-opencode-agent/9.9.9" {
		t.Fatalf("expected custom UA override, got %q", got)
	}
}

