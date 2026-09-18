package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kimnt93/gorouter/pkg/entities"
)

func TestAntigravitySendUsesCloudCodeEnvelopeAndConvertsResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1internal:generateContent" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer google-token" {
			t.Errorf("authorization = %q", got)
		}
		var envelope map[string]any
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if envelope["model"] != "gemini-3.1-pro-preview" || envelope["project"] != "project-1" {
			t.Errorf("envelope = %#v", envelope)
		}
		request := envelope["request"].(map[string]any)
		generation := request["generationConfig"].(map[string]any)
		thinking := generation["thinkingConfig"].(map[string]any)
		if thinking["thinkingLevel"] != "MEDIUM" {
			t.Errorf("thinking config = %#v", thinking)
		}
		if len(request["tools"].([]any)) != 1 {
			t.Errorf("tools = %#v", request["tools"])
		}
		_, _ = io.WriteString(w, `{"response":{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":13,"candidatesTokenCount":1,"cachedContentTokenCount":10,"thoughtsTokenCount":2}}}`)
	}))
	defer server.Close()

	request := []byte(`{"messages":[{"role":"system","content":"be helpful"},{"role":"user","content":"hi"}],"reasoning":{"effort":"medium"},"tools":[{"type":"function","function":{"name":"lookup","description":"lookup","parameters":{"type":"object"}}}]}`)
	result, err := (&AntigravityAdapter{HTTP: server.Client()}).Send(context.Background(), &entities.CredentialRuntime{
		BaseURL:     server.URL,
		OAuthAccess: "google-token",
		OAuthMeta:   entities.OAuthMetadata{ProjectID: "project-1"},
	}, "gemini-3.1-pro-preview", request)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Body.Close()
	var response Response
	if err := json.NewDecoder(result.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Choices[0].Message.Content != "hello" || response.Usage.PromptTokens != 3 || response.Usage.CacheReadTokens != 10 || response.Usage.CompletionTokens != 3 {
		t.Fatalf("converted response = %+v", response)
	}
}

func TestAntigravityStreamConvertsTextAndToolCalls(t *testing.T) {
	upstream := io.NopCloser(strings.NewReader("data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"},{\"functionCall\":{\"id\":\"call-1\",\"name\":\"lookup\",\"args\":{\"q\":\"x\"}}}]}}]}}\n\n"))
	body, err := io.ReadAll(antigravityStream(upstream, "gemini-3.1-pro-preview"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{`"content":"hi"`, `"name":"lookup"`, `"finish_reason":"tool_calls"`, "data: [DONE]"} {
		if !strings.Contains(text, want) {
			t.Fatalf("stream omitted %q: %s", want, text)
		}
	}
}

func TestAntigravityDiscoveryUsesAuthenticatedLiveCatalog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1internal:fetchAvailableModels" || r.Method != http.MethodPost {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer google-token" {
			t.Fatalf("authorization=%q", got)
		}
		var body struct {
			Project string `json:"project"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Project != "project-1" {
			t.Fatalf("body=%+v err=%v", body, err)
		}
		_, _ = io.WriteString(w, `{"models":{"gemini-3.7-flash-high":{"displayName":"Gemini Flash","contextWindow":1048576,"maxOutputTokens":65536},"claude-sonnet-4-6":{"displayName":"Claude Sonnet"},"imagen-4":{"displayName":"Imagen"},"internal":{"isInternal":true}}}`)
	}))
	defer server.Close()

	models, err := (&AntigravityAdapter{HTTP: server.Client()}).DiscoverModels(context.Background(), &entities.CredentialRuntime{
		BaseURL: server.URL, OAuthAccess: "google-token", OAuthMeta: entities.OAuthMetadata{ProjectID: "project-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || models[0].ID != "claude-sonnet-4-6" || models[1].ID != "gemini-3.7-flash-high" || models[1].ContextLength != 1048576 {
		t.Fatalf("models=%+v", models)
	}
}

func TestAntigravityLabelsAndWireProfiles(t *testing.T) {
	// 1. Flash model with known enum
	dummyContent := []map[string]any{{"role": "user", "parts": []map[string]any{{"text": "hello"}}}}
	labelsFlash := buildAntigravityLabels("gemini-3.7-flash-medium", 3, dummyContent)
	if labelsFlash["model_enum"] != "MODEL_PLACEHOLDER_M132" {
		t.Errorf("expected MODEL_PLACEHOLDER_M132, got %q", labelsFlash["model_enum"])
	}
	if labelsFlash["last_step_index"] != "2" {
		t.Errorf("expected last_step_index=2, got %q", labelsFlash["last_step_index"])
	}
	if labelsFlash["trajectory_id"] == "" {
		t.Errorf("expected non-empty trajectory_id")
	}
	if _, ok := labelsFlash["used_claude"]; ok {
		t.Errorf("expected no used_claude for gemini model")
	}

	// 2. Claude alias model
	labelsClaude := buildAntigravityLabels("claude-3-7-sonnet", 1, dummyContent)
	if labelsClaude["used_claude"] != "1" {
		t.Errorf("expected used_claude=1 for claude model")
	}
	if labelsClaude["last_step_index"] != "0" {
		t.Errorf("expected last_step_index=0, got %q", labelsClaude["last_step_index"])
	}
}

func TestAntigravitySystemInstructionNormalization(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		contains []string
		omits    []string
	}{
		{
			name:  "Claude Code and SDK prompt",
			input: "You are a Claude agent, built on Anthropic's Claude Agent SDK.\nFollow RFC 2119 strictly.",
			contains: []string{
				"Follow RFC 2119 strictly.",
			},
			omits: []string{
				"You are a Claude agent, built on Anthropic's Claude Agent SDK.",
			},
		},
		{
			name:  "OpenCode, Oh-My-Pi, and Pi harness prompt",
			input: "You are Pi, a fast and rigorous autonomous coding agent harness.\nYou are an agent in Oh My Pi.\nUse OpenCode conventions and z.ai tools.",
			contains: []string{
				"Use Antigravity conventions and Google DeepMind tools.",
			},
			omits: []string{
				"You are Pi, a fast and rigorous autonomous coding agent harness",
				"You are an agent in Oh My Pi",
				"OpenCode",
				"z.ai",
			},
		},
		{
			name:  "ZCode and proxy prefixes",
			input: "google-antigravity/model-spec for zcode agents.",
			contains: []string{
				"model-spec for antigravity agents.",
			},
			omits: []string{
				"google-antigravity/",
				"zcode",
			},
		},
		{
			name:  "Codex prompt",
			input: "You are Codex, a coding agent from OpenAI.\nImplement the function.",
			contains: []string{
				"Implement the function.",
			},
			omits: []string{
				"You are Codex, a coding agent from OpenAI.",
			},
		},
		{
			name:  "Word boundary preservation (no accidental partial word replacement)",
			input: "Check compute_opcode() and zcode_variable.",
			contains: []string{
				"compute_opcode()",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeAntigravitySystemInstruction(tc.input)
			for _, want := range tc.contains {
				if !strings.Contains(got, want) {
					t.Errorf("expected normalized to contain %q, got: %q", want, got)
				}
			}
			for _, unwanted := range tc.omits {
				if strings.Contains(got, unwanted) {
					t.Errorf("expected normalized to omit %q, got: %q", unwanted, got)
				}
			}
		})
	}
}

func TestAntigravityBoundedFailover(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Transient 503 error on primary tier
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Succeed on secondary tier
		_, _ = io.WriteString(w, `{"response":{"candidates":[{"content":{"parts":[{"text":"recovered"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":2}}}`)
	}))
	defer server2.Close()

	adapter := &AntigravityAdapter{
		HTTP:          server1.Client(),
		FailoverBases: []string{server1.URL, server2.URL},
	}
	result, err := adapter.Send(context.Background(), &entities.CredentialRuntime{
		OAuthAccess: "token-1",
	}, "gemini-3.7-flash-medium", []byte(`{"messages":[{"role":"user","content":"ping"}]}`))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Body.Close()
	if result.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", result.StatusCode)
	}
}

func TestAntigravityFailoverStopsOnHard429AndClientErrors(t *testing.T) {
	// 1. Test 403 Forbidden halts immediately without contacting second server
	server1Calls := 0
	server2Calls := 0
	server403 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server1Calls++
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"message":"permission denied"}}`)
	}))
	defer server403.Close()

	serverBackup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server2Calls++
		_, _ = io.WriteString(w, `{"response":{"candidates":[{"content":{"parts":[{"text":"should not reach"}]}}]}}`)
	}))
	defer serverBackup.Close()

	adapter := &AntigravityAdapter{
		HTTP:          server403.Client(),
		FailoverBases: []string{server403.URL, serverBackup.URL},
	}
	result403, err := adapter.Send(context.Background(), &entities.CredentialRuntime{
		OAuthAccess: "token-1",
	}, "gemini-3.7-flash-medium", []byte(`{"messages":[{"role":"user","content":"test"}]}`))
	if err != nil {
		t.Fatalf("expected result, got error: %v", err)
	}
	if result403.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 status, got %d", result403.StatusCode)
	}
	if server1Calls != 1 {
		t.Errorf("expected 1 call to server1, got %d", server1Calls)
	}
	if server2Calls != 0 {
		t.Errorf("expected 0 calls to backup server on 403, got %d", server2Calls)
	}
	result403.Body.Close()

	// 2. Test 429 with Retry-After > 15s halts immediately without contacting second server
	server1Calls = 0
	server2Calls = 0
	server429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server1Calls++
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"quota exceeded"}}`)
	}))
	defer server429.Close()

	adapter429 := &AntigravityAdapter{
		HTTP:          server429.Client(),
		FailoverBases: []string{server429.URL, serverBackup.URL},
	}
	result429, err := adapter429.Send(context.Background(), &entities.CredentialRuntime{
		OAuthAccess: "token-1",
	}, "gemini-3.7-flash-medium", []byte(`{"messages":[{"role":"user","content":"test"}]}`))
	if err != nil {
		t.Fatalf("expected result, got error: %v", err)
	}
	if result429.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected 429 status, got %d", result429.StatusCode)
	}
	if server1Calls != 1 {
		t.Errorf("expected 1 call to server1, got %d", server1Calls)
	}
	if server2Calls != 0 {
		t.Errorf("expected 0 calls to backup server on hard 429, got %d", server2Calls)
	}
	result429.Body.Close()
}

func TestAntigravityOAuthReplayBudget(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			// First attempt returns 401 Unauthorized
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// Second attempt after token refresh succeeds
		_, _ = io.WriteString(w, `{"response":{"candidates":[{"content":{"parts":[{"text":"refreshed"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":2}}}`)
	}))
	defer server.Close()

	refreshed := false
	adapter := &AntigravityAdapter{
		HTTP:          server.Client(),
		FailoverBases: []string{server.URL},
		Refresh: func(ctx context.Context, cr *entities.CredentialRuntime) error {
			refreshed = true
			cr.OAuthAccess = "new-token"
			return nil
		},
		Persister: &testOAuthPersister{},
	}

	result, err := adapter.Send(context.Background(), &entities.CredentialRuntime{
		OAuthAccess: "old-token",
	}, "gemini-3.7-flash-medium", []byte(`{"messages":[{"role":"user","content":"test"}]}`))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Body.Close()
	if !refreshed {
		t.Fatal("expected refresh callback to be called")
	}
	if attempts != 2 {
		t.Fatalf("expected exactly 2 attempts (initial + 1 replay), got %d", attempts)
	}
}

type testOAuthPersister struct{}

func (p *testOAuthPersister) UpdateOAuthTokens(ctx context.Context, id, access, refresh string) error {
	return nil
}

func TestAntigravityPromptAuditLogging(t *testing.T) {
	tmpDir := t.TempDir()
	auditPath := filepath.Join(tmpDir, "antigravity_prompts.jsonl")
	t.Setenv("ANTIGRAVITY_PROMPT_AUDIT_FILE", auditPath)
	t.Setenv("ANTIGRAVITY_AUDIT_INCLUDE_PROMPT", "true")

	prompt := "You are a Claude agent, built on Anthropic's Claude Agent SDK.\nHelp me with Go code."
	_, err := antigravityRequest(ChatRequest{
		Messages: []Message{
			{Role: "system", Content: []byte(jsonMustMarshal(prompt))},
			{Role: "user", Content: []byte(`"hello"`)},
		},
	}, "gemini-3.7-flash-medium", "project-1")

	if err != nil {
		t.Fatalf("antigravityRequest failed: %v", err)
	}

	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("failed to read audit file: %v", err)
	}

	var entry AntigravityPromptAuditEntry
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil {
		t.Fatalf("failed to unmarshal audit entry: %v", err)
	}

	if entry.Harness != "claude-code" {
		t.Errorf("expected harness claude-code, got %q", entry.Harness)
	}
	if entry.Model != "gemini-3.7-flash-medium" {
		t.Errorf("expected model gemini-3.7-flash-medium, got %q", entry.Model)
	}
	if entry.OriginalPrompt != prompt {
		t.Errorf("expected original prompt recorded, got %q", entry.OriginalPrompt)
	}
	if entry.NormalizedPrompt != "Help me with Go code." {
		t.Errorf("expected normalized prompt 'Help me with Go code.', got %q", entry.NormalizedPrompt)
	}
	if entry.OriginalSHA256 == "" || entry.NormalizedSHA256 == "" {
		t.Error("expected non-empty SHA256 hashes")
	}
}

func jsonMustMarshal(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestAntigravityToolSchemaCleaning(t *testing.T) {
	cases := []struct {
		name     string
		tools    string
		validate func(t *testing.T, declarations []any, request map[string]any)
	}{
		{
			name:  "strips unsupported JSON Schema keywords",
			tools: `[{"type":"function","function":{"name":"lookup","description":"lookup","parameters":{"type":"object","$schema":"http://json-schema.org/draft-07/schema#","strict":true,"title":"Lookup","x-custom":"meta","properties":{"q":{"type":"string","format":"date-time","default":"now","title":"Query","x-inner":1}},"required":["q"]}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				params := antigravityDeclParams(t, antigravityFirstDecl(t, declarations))
				antigravityAssertKeysAbsentEverywhere(t, params, "$schema", "strict", "format", "default", "title", "x-custom", "x-inner")
				q, ok := params["properties"].(map[string]any)["q"].(map[string]any)
				if !ok || q["type"] != "string" {
					t.Errorf("properties.q = %#v", params["properties"])
				}
				required, _ := params["required"].([]any)
				if len(required) != 1 || required[0] != "q" {
					t.Errorf("required = %#v", params["required"])
				}
			},
		},
		{
			name:  "resolves $ref against $defs",
			tools: `[{"type":"function","function":{"name":"deliver","parameters":{"type":"object","$defs":{"Addr":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}},"properties":{"home":{"$ref":"#/$defs/Addr"}},"required":["home"]}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				params := antigravityDeclParams(t, antigravityFirstDecl(t, declarations))
				antigravityAssertKeysAbsentEverywhere(t, params, "$ref", "$defs")
				home, ok := params["properties"].(map[string]any)["home"].(map[string]any)
				if !ok {
					t.Fatalf("properties.home = %#v", params["properties"])
				}
				if home["type"] != "object" {
					t.Errorf("home.type = %#v", home["type"])
				}
				city, ok := home["properties"].(map[string]any)["city"].(map[string]any)
				if !ok || city["type"] != "string" {
					t.Errorf("home.properties.city = %#v", home["properties"])
				}
				required, _ := home["required"].([]any)
				if len(required) != 1 || required[0] != "city" {
					t.Errorf("home.required = %#v", home["required"])
				}
			},
		},
		{
			name:  "flattens anyOf null union onto object variant",
			tools: `[{"type":"function","function":{"name":"move","parameters":{"anyOf":[{"type":"null"},{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}]}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				params := antigravityDeclParams(t, antigravityFirstDecl(t, declarations))
				antigravityAssertKeysAbsentEverywhere(t, params, "anyOf")
				if params["type"] != "object" {
					t.Errorf("type = %#v", params["type"])
				}
				a, ok := params["properties"].(map[string]any)["a"].(map[string]any)
				if !ok || a["type"] != "string" {
					t.Errorf("properties.a = %#v", params["properties"])
				}
				required, _ := params["required"].([]any)
				if len(required) != 1 || required[0] != "a" {
					t.Errorf("required = %#v", params["required"])
				}
			},
		},
		{
			name:  "flattens type arrays and converts const to enum",
			tools: `[{"type":"function","function":{"name":"describe","parameters":{"type":["string","null"]}}},{"type":"function","function":{"name":"set_mode","parameters":{"type":"object","properties":{"mode":{"const":"x"}},"required":["mode"]}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				if len(declarations) != 2 {
					t.Fatalf("declarations = %#v", declarations)
				}
				first := antigravityDeclParams(t, antigravityFirstDecl(t, declarations))
				if first["type"] != "string" {
					t.Errorf("describe.type = %#v", first["type"])
				}
				second := antigravityDeclParams(t, declarations[1].(map[string]any))
				mode, ok := second["properties"].(map[string]any)["mode"].(map[string]any)
				if !ok {
					t.Fatalf("set_mode properties.mode = %#v", second["properties"])
				}
				enum, ok := mode["enum"].([]any)
				if !ok || len(enum) != 1 || enum[0] != "x" {
					t.Errorf("mode.enum = %#v", mode["enum"])
				}
				if _, hasConst := mode["const"]; hasConst {
					t.Errorf("mode.const survived: %#v", mode)
				}
			},
		},
		{
			name:  "drops required entries missing from properties",
			tools: `[{"type":"function","function":{"name":"claim","parameters":{"type":"object","properties":{"a":{"type":"string"}},"required":["a","ghost"]}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				params := antigravityDeclParams(t, antigravityFirstDecl(t, declarations))
				required, _ := params["required"].([]any)
				if len(required) != 1 || required[0] != "a" {
					t.Errorf("required = %#v", params["required"])
				}
			},
		},
		{
			name:  "placeholders missing parameters",
			tools: `[{"type":"function","function":{"name":"bare"}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				antigravityAssertReasonPlaceholder(t, antigravityDeclParams(t, antigravityFirstDecl(t, declarations)))
			},
		},
		{
			name:  "placeholders empty parameters",
			tools: `[{"type":"function","function":{"name":"empty","parameters":{}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				antigravityAssertReasonPlaceholder(t, antigravityDeclParams(t, antigravityFirstDecl(t, declarations)))
			},
		},
		{
			name:  "sanitizes tool names",
			tools: `[{"type":"function","function":{"name":"mcp/foo bar","parameters":{"type":"object","properties":{"x":{"type":"string"}}}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				if decl := antigravityFirstDecl(t, declarations); decl["name"] != "mcp_foo_bar" {
					t.Errorf("name = %#v", decl["name"])
				}
			},
		},
		{
			name:  "deduplicates sanitized names keeping the first",
			tools: `[{"type":"function","function":{"name":"mcp/foo","parameters":{"type":"object","properties":{"first":{"type":"string"}}}}},{"type":"function","function":{"name":"mcp_foo","parameters":{"type":"object","properties":{"second":{"type":"string"}}}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				if len(declarations) != 1 {
					t.Fatalf("declarations = %#v", declarations)
				}
				decl := antigravityFirstDecl(t, declarations)
				if decl["name"] != "mcp_foo" {
					t.Errorf("name = %#v", decl["name"])
				}
				params := antigravityDeclParams(t, decl)
				if _, ok := params["properties"].(map[string]any)["first"]; !ok {
					t.Errorf("expected first tool's properties kept, got %#v", params)
				}
			},
		},
		{
			name:  "sends VALIDATED tool config mode",
			tools: `[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]`,
			validate: func(t *testing.T, declarations []any, request map[string]any) {
				toolConfig, ok := request["toolConfig"].(map[string]any)
				if !ok {
					t.Fatalf("toolConfig = %#v", request["toolConfig"])
				}
				calling, ok := toolConfig["functionCallingConfig"].(map[string]any)
				if !ok {
					t.Fatalf("functionCallingConfig = %#v", toolConfig["functionCallingConfig"])
				}
				if calling["mode"] != "VALIDATED" {
					t.Errorf("mode = %#v, want VALIDATED", calling["mode"])
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var captured map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var envelope map[string]any
				if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
					t.Errorf("decode envelope: %v", err)
					return
				}
				captured, _ = envelope["request"].(map[string]any)
				_, _ = io.WriteString(w, `{"response":{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}}`)
			}))
			defer server.Close()

			payload := `{"messages":[{"role":"user","content":"hi"}],"tools":` + tc.tools + `}`
			result, err := (&AntigravityAdapter{HTTP: server.Client()}).Send(context.Background(), &entities.CredentialRuntime{
				BaseURL:     server.URL,
				OAuthAccess: "google-token",
			}, "gemini-3.7-flash-medium", []byte(payload))
			if err != nil {
				t.Fatal(err)
			}
			defer result.Body.Close()
			if captured == nil {
				t.Fatal("outbound request not captured")
			}
			tools, _ := captured["tools"].([]any)
			if len(tools) != 1 {
				t.Fatalf("tools = %#v", captured["tools"])
			}
			declarations, _ := tools[0].(map[string]any)["functionDeclarations"].([]any)
			tc.validate(t, declarations, captured)
		})
	}
}

func TestAntigravityToolResponseNameResolution(t *testing.T) {
	cases := []struct {
		name     string
		messages string
		wantName string
	}{
		{
			name: "tool message without name resolves via tool_call_id",
			messages: `[
				{"role":"user","content":"run ls"},
				{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"mcp/bash tool","arguments":"{}"}}]},
				{"role":"tool","tool_call_id":"call_1","content":"ok"}
			]`,
			wantName: "mcp_bash_tool",
		},
		{
			name: "tool message with explicit name is sanitized",
			messages: `[
				{"role":"user","content":"run ls"},
				{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]},
				{"role":"tool","tool_call_id":"call_1","name":"mcp/bash tool","content":"ok"}
			]`,
			wantName: "mcp_bash_tool",
		},
		{
			name: "unresolvable tool message falls back to _unknown",
			messages: `[
				{"role":"user","content":"run ls"},
				{"role":"tool","tool_call_id":"call_missing","content":"ok"}
			]`,
			wantName: "_unknown",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := `{"messages":` + tc.messages + `}`
			var input ChatRequest
			if err := json.Unmarshal([]byte(payload), &input); err != nil {
				t.Fatal(err)
			}
			raw, err := antigravityRequest(input, "gemini-3.8-flash-high", "project")
			if err != nil {
				t.Fatal(err)
			}
			var envelope map[string]any
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			request, _ := envelope["request"].(map[string]any)
			contents, _ := request["contents"].([]any)
			var fnResp map[string]any
			for _, c := range contents {
				for _, p := range c.(map[string]any)["parts"].([]any) {
					if fr, ok := p.(map[string]any)["functionResponse"].(map[string]any); ok {
						fnResp = fr
					}
				}
			}
			if fnResp == nil {
				t.Fatal("no functionResponse part emitted")
			}
			if fnResp["name"] != tc.wantName {
				t.Errorf("functionResponse.name = %#v, want %q", fnResp["name"], tc.wantName)
			}
		})
	}
}

func TestAntigravityThoughtSignatureBackfill(t *testing.T) {
	payload := `{"messages":[
		{"role":"user","content":"run"},
		{"role":"assistant","tool_calls":[
			{"id":"call_1","type":"function","function":{"name":"Bash","arguments":"{}"}},
			{"id":"call_2","type":"function","function":{"name":"Read","arguments":"{}"}}
		]},
		{"role":"tool","tool_call_id":"call_1","content":"ok"},
		{"role":"tool","tool_call_id":"call_2","content":"ok"}
	]}`
	var input ChatRequest
	if err := json.Unmarshal([]byte(payload), &input); err != nil {
		t.Fatal(err)
	}
	raw, err := antigravityRequest(input, "gemini-3.8-flash-high", "project")
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	request, _ := envelope["request"].(map[string]any)
	contents, _ := request["contents"].([]any)

	var callParts []map[string]any
	for _, c := range contents {
		for _, p := range c.(map[string]any)["parts"].([]any) {
			part := p.(map[string]any)
			if _, ok := part["functionCall"]; ok {
				callParts = append(callParts, part)
			}
		}
	}
	if len(callParts) != 2 {
		t.Fatalf("functionCall parts = %d, want 2", len(callParts))
	}
	sig, ok := callParts[0]["thoughtSignature"].(string)
	if !ok || sig == "" {
		t.Errorf("first functionCall part missing thoughtSignature: %#v", callParts[0])
	}
	if _, ok := callParts[1]["thoughtSignature"]; ok {
		t.Errorf("sibling functionCall part should stay unsigned: %#v", callParts[1])
	}
}

func antigravityFirstDecl(t *testing.T, declarations []any) map[string]any {
	t.Helper()
	if len(declarations) == 0 {
		t.Fatal("expected at least one function declaration")
	}
	decl, ok := declarations[0].(map[string]any)
	if !ok {
		t.Fatalf("declaration[0] = %#v", declarations[0])
	}
	return decl
}

func antigravityDeclParams(t *testing.T, decl map[string]any) map[string]any {
	t.Helper()
	params, ok := decl["parameters"].(map[string]any)
	if !ok {
		t.Fatalf("parameters = %#v", decl["parameters"])
	}
	return params
}

func antigravityAssertReasonPlaceholder(t *testing.T, params map[string]any) {
	t.Helper()
	if params["type"] != "object" {
		t.Errorf("type = %#v", params["type"])
	}
	properties, _ := params["properties"].(map[string]any)
	reason, ok := properties["reason"].(map[string]any)
	if !ok || reason["type"] != "string" || reason["description"] != "Brief explanation of why you are calling this tool" {
		t.Errorf("properties.reason = %#v", properties["reason"])
	}
	required, _ := params["required"].([]any)
	if len(required) != 1 || required[0] != "reason" {
		t.Errorf("required = %#v", params["required"])
	}
}

func antigravityAssertKeysAbsentEverywhere(t *testing.T, node any, keys ...string) {
	t.Helper()
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			for _, banned := range keys {
				if key == banned {
					t.Errorf("expected key %q to be stripped, found in %#v", banned, typed)
				}
			}
			antigravityAssertKeysAbsentEverywhere(t, value, keys...)
		}
	case []any:
		for _, item := range typed {
			antigravityAssertKeysAbsentEverywhere(t, item, keys...)
		}
	}
}
