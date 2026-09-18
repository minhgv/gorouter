package llm

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kimnt93/gorouter/pkg/credential"
	"github.com/kimnt93/gorouter/pkg/entities"
)

const (
	antigravityRuntimeBaseURL  = "https://daily-cloudcode-pa.googleapis.com"
	antigravitySandboxBaseURL  = "https://daily-cloudcode-pa.sandbox.googleapis.com"
	antigravityFallbackBaseURL = "https://cloudcode-pa.googleapis.com"
	antigravityUserAgent       = "antigravity/ide/2.11.0 darwin/arm64"
	antigravityClientSource    = "local"
	antigravityClientMetadata  = "ideType=ANTIGRAVITY,platform=MACOS,pluginType=GEMINI"
)

var antigravityWireProfiles = map[string]string{
	"gemini-3.5-flash-low":    "MODEL_PLACEHOLDER_M20",
	"gemini-3-flash-agent":    "MODEL_PLACEHOLDER_M132",
	"gemini-3.7-flash-high":   "MODEL_PLACEHOLDER_M132",
	"gemini-3.7-flash-medium": "MODEL_PLACEHOLDER_M132",
	"gemini-3.7-flash-low":    "MODEL_PLACEHOLDER_M132",
	"gemini-3.1-pro-low":      "MODEL_PLACEHOLDER_M36",
	"gemini-pro-agent":        "MODEL_PLACEHOLDER_M16",
	"gemini-3.1-pro-preview":  "MODEL_PLACEHOLDER_M16",
}

type AntigravityAdapter struct {
	Refresh       func(context.Context, *entities.CredentialRuntime) error
	HTTP          *http.Client
	Persister     OAuthTokenPersister
	ClientID      string
	ClientSecret  string
	FailoverBases []string
}

func (a *AntigravityAdapter) refreshRuntime(ctx context.Context, cr *entities.CredentialRuntime) error {
	if a.Refresh != nil {
		return a.Refresh(ctx, cr)
	}
	return refreshOAuthForm(ctx, a.HTTP, a.Persister, cr, "https://oauth2.googleapis.com/token", a.ClientID, a.ClientSecret, nil)
}

func (a *AntigravityAdapter) client() *http.Client {
	if a.HTTP != nil {
		return a.HTTP
	}
	return NewHTTPClient()
}

func (a *AntigravityAdapter) Send(ctx context.Context, cr *entities.CredentialRuntime, model string, raw []byte) (*entities.UpstreamResult, error) {
	return a.sendWithBudget(ctx, cr, model, raw, 3)
}

func (a *AntigravityAdapter) sendWithBudget(ctx context.Context, cr *entities.CredentialRuntime, model string, raw []byte, maxAttempts int) (*entities.UpstreamResult, error) {
	var input ChatRequest
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, fmt.Errorf("parse Antigravity request: %w", err)
	}
	payload, err := antigravityRequest(input, model, cr.OAuthMeta.ProjectID)
	if err != nil {
		return nil, err
	}
	path := "/v1internal:generateContent"
	if input.Stream {
		path = "/v1internal:streamGenerateContent?alt=sse"
	}

	configuredBase := strings.TrimRight(cr.BaseURL, "/")
	var bases []string
	if len(a.FailoverBases) > 0 {
		bases = a.FailoverBases
	} else if configuredBase == "" || configuredBase == antigravityRuntimeBaseURL || configuredBase == antigravityFallbackBaseURL || configuredBase == antigravitySandboxBaseURL {
		bases = []string{antigravityRuntimeBaseURL, antigravitySandboxBaseURL, antigravityFallbackBaseURL}
	} else {
		bases = []string{configuredBase}
	}
	headers := map[string]string{
		"Authorization":    "Bearer " + cr.OAuthAccess,
		"Accept":           "application/json",
		"User-Agent":       antigravityUserAgent,
		"x-request-source": antigravityClientSource,
		"Client-Metadata":  antigravityClientMetadata,
	}
	if project := strings.TrimSpace(cr.OAuthMeta.ProjectID); project != "" && project != "aicode-consumers" {
		headers["x-goog-user-project"] = project
	}

	var (
		lastResult *entities.UpstreamResult
		lastErr    error
	)

	for attempt := 0; attempt < maxAttempts && attempt < len(bases); attempt++ {
		base := bases[attempt]
		if ctx.Err() != nil {
			if lastResult != nil && lastResult.Body != nil {
				_ = lastResult.Body.Close()
			}
			return nil, ctx.Err()
		}

		result, err := postJSON(ctx, a.client(), base+path, headers, payload)
		if err == nil && result.StatusCode == http.StatusUnauthorized && a.Persister != nil && canRetryOAuth(ctx) {
			result.Body.Close()
			if refreshErr := a.refreshRuntime(ctx, cr); refreshErr != nil {
				return nil, refreshErr
			}
			remaining := maxAttempts - (attempt + 1)
			if remaining > 0 {
				return a.sendWithBudget(markOAuthRetry(ctx), cr, model, raw, remaining)
			}
			return result, nil
		}

		if err == nil && result.StatusCode >= 200 && result.StatusCode < 300 {
			if lastResult != nil && lastResult.Body != nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(lastResult.Body, 64<<10))
				_ = lastResult.Body.Close()
			}
			if input.Stream {
				result.Body = antigravityStream(result.Body, model)
				result.Header["Content-Type"] = []string{"text/event-stream"}
				return result, nil
			}
			defer result.Body.Close()
			body, readErr := io.ReadAll(io.LimitReader(result.Body, 32<<20))
			if readErr != nil {
				return nil, readErr
			}
			converted, convErr := antigravityResponse(body, model)
			if convErr != nil {
				return nil, convErr
			}
			result.Body = io.NopCloser(bytes.NewReader(converted))
			return result, nil
		}

		// Drain and close superseded intermediate failure before storing new one
		if lastResult != nil && lastResult.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(lastResult.Body, 64<<10))
			_ = lastResult.Body.Close()
		}
		lastResult = result
		lastErr = err

		if err == nil && result != nil && result.StatusCode >= 400 {
			auditAntigravityUpstreamError(model, result)
		}

		// Evaluate failover eligibility
		if result != nil {
			// Do not hop on client errors (400, 401, 403)
			if result.StatusCode == http.StatusBadRequest || result.StatusCode == http.StatusUnauthorized || result.StatusCode == http.StatusForbidden {
				break
			}
			// For 429 Quota Exhausted, check if Retry-After is large
			if result.StatusCode == http.StatusTooManyRequests {
				retryAfter := http.Header(result.Header).Get("Retry-After")
				if retryAfter != "" && retryAfterSeconds(retryAfter) > 15 {
					break
				}
			}
		}
	}

	if lastResult != nil {
		return lastResult, lastErr
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("Antigravity dispatches exhausted")
}

// antigravityUnsupportedSchemaKeys lists JSON Schema keywords the Gemini
// function-declaration proto rejects with "Unknown name ...: Cannot find
// field", plus Gemini meta keywords verified live against
// /v1internal:generateContent. Port of 9router cleanJSONSchemaForAntigravity.
var antigravityUnsupportedSchemaKeys = map[string]bool{
	"minLength": true, "maxLength": true, "exclusiveMinimum": true, "exclusiveMaximum": true,
	"minItems": true, "maxItems": true, "format": true, "multipleOf": true,
	"uniqueItems": true, "contains": true, "unevaluatedProperties": true, "unevaluatedItems": true,
	"contentSchema": true, "prefixItems": true, "additionalItems": true,
	"default": true, "examples": true,
	"$schema": true, "$defs": true, "definitions": true, "const": true, "$ref": true, "$comment": true,
	"$id": true, "$anchor": true, "strict": true,
	"deprecated": true, "readOnly": true, "writeOnly": true,
	"additionalProperties": true, "propertyNames": true, "patternProperties": true, "enumDescriptions": true,
	"anyOf": true, "oneOf": true, "allOf": true, "not": true,
	"dependencies": true, "dependentSchemas": true, "dependentRequired": true,
	"title": true, "optional": true, "if": true, "then": true, "else": true,
	"contentMediaType": true, "contentEncoding": true,
	"cornerRadius": true, "fillColor": true, "fontFamily": true, "fontSize": true, "fontWeight": true,
	"gap": true, "padding": true, "strokeColor": true, "strokeThickness": true, "textColor": true,
}

var antigravityRefPattern = regexp.MustCompile(`#/(\$defs|definitions)/(.+)`)

var antigravityToolNameInvalidRunes = regexp.MustCompile(`[^a-zA-Z0-9_.:\-]`)

// dereferenceAntigravitySchema resolves $ref pointers against the nearest
// $defs/definitions scope before any stripping happens, so a bare-stripped
// $ref does not lose the tool's real shape. resolving holds the set of
// in-progress $ref names and guards against reference cycles. The input
// tree is never mutated; a fresh tree is returned.
func dereferenceAntigravitySchema(schema any, defs map[string]any, resolving map[string]bool) any {
	switch node := schema.(type) {
	case []any:
		out := make([]any, 0, len(node))
		for _, item := range node {
			out = append(out, dereferenceAntigravitySchema(item, defs, resolving))
		}
		return out
	case map[string]any:
		localDefs := make(map[string]any, len(defs)+2)
		for name, def := range defs {
			localDefs[name] = def
		}
		for _, key := range []string{"$defs", "definitions"} {
			if raw, ok := node[key]; ok {
				if scope, ok := raw.(map[string]any); ok {
					for name, def := range scope {
						localDefs[name] = def
					}
				}
			}
		}
		if raw, ok := node["$ref"].(string); ok {
			if match := antigravityRefPattern.FindStringSubmatch(raw); match != nil {
				name := match[2]
				if _, defined := localDefs[name]; defined && !resolving[name] {
					resolving[name] = true
					resolved := dereferenceAntigravitySchema(localDefs[name], localDefs, resolving)
					delete(resolving, name)
					rest := make(map[string]any, len(node))
					for key, value := range node {
						if key == "$ref" {
							continue
						}
						rest[key] = dereferenceAntigravitySchema(value, localDefs, resolving)
					}
					if resolvedMap, ok := resolved.(map[string]any); ok {
						merged := make(map[string]any, len(resolvedMap)+len(rest))
						for key, value := range resolvedMap {
							merged[key] = value
						}
						for key, value := range rest {
							merged[key] = value
						}
						return merged
					}
					return resolved
				}
			}
		}
		out := make(map[string]any, len(node))
		for key, value := range node {
			out[key] = dereferenceAntigravitySchema(value, localDefs, resolving)
		}
		return out
	default:
		return schema
	}
}

// walkAntigravitySchemaNodes applies phase to every schema object reachable
// from node, descending into maps and slices in pre-order.
func walkAntigravitySchemaNodes(node any, phase func(map[string]any)) {
	switch typed := node.(type) {
	case map[string]any:
		phase(typed)
		for _, value := range typed {
			walkAntigravitySchemaNodes(value, phase)
		}
	case []any:
		for _, item := range typed {
			walkAntigravitySchemaNodes(item, phase)
		}
	}
}

func antigravityConvertConstToEnum(node map[string]any) {
	if _, hasEnum := node["enum"]; hasEnum {
		return
	}
	constant, ok := node["const"]
	if !ok {
		return
	}
	node["enum"] = []any{constant}
	delete(node, "const")
}

func antigravityConvertEnumValuesToStrings(node map[string]any) {
	values, ok := node["enum"].([]any)
	if !ok {
		return
	}
	converted := make([]any, 0, len(values))
	for _, value := range values {
		converted = append(converted, fmt.Sprintf("%v", value))
	}
	node["enum"] = converted
	if _, hasType := node["type"]; !hasType {
		node["type"] = "string"
	}
}

func antigravityMergeAllOf(node map[string]any) {
	entries, ok := node["allOf"].([]any)
	if !ok {
		return
	}
	delete(node, "allOf")
	properties, _ := node["properties"].(map[string]any)
	required, _ := node["required"].([]any)
	seen := map[string]bool{}
	for _, entry := range required {
		if name, ok := entry.(string); ok {
			seen[name] = true
		}
	}
	for _, entry := range entries {
		sub, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if subProperties, ok := sub["properties"].(map[string]any); ok {
			if properties == nil {
				properties = map[string]any{}
				node["properties"] = properties
			}
			for name, schema := range subProperties {
				properties[name] = schema
			}
		}
		if subRequired, ok := sub["required"].([]any); ok {
			for _, name := range subRequired {
				if key, ok := name.(string); ok && !seen[key] {
					seen[key] = true
					required = append(required, key)
				}
			}
		}
	}
	if len(required) > 0 {
		node["required"] = required
	}
}

func antigravityConvertPrefixItems(node map[string]any) {
	entries, ok := node["prefixItems"].([]any)
	if !ok || len(entries) == 0 {
		return
	}
	delete(node, "prefixItems")
	if _, hasItems := node["items"]; hasItems {
		return
	}
	variants := make([]any, 0, len(entries))
	for _, entry := range entries {
		if sub, ok := entry.(map[string]any); ok && sub["type"] == "null" {
			continue
		}
		variants = append(variants, entry)
	}
	switch {
	case len(variants) == 1:
		node["items"] = variants[0]
	case len(variants) > 1:
		node["items"] = map[string]any{"anyOf": variants}
	}
}

func antigravityVariantScore(variant any) int {
	sub, ok := variant.(map[string]any)
	if !ok {
		return 0
	}
	if _, hasProperties := sub["properties"]; hasProperties || sub["type"] == "object" {
		return 3
	}
	if _, hasItems := sub["items"]; hasItems || sub["type"] == "array" {
		return 2
	}
	if _, isString := sub["type"].(string); isString {
		return 1
	}
	return 0
}

func antigravityFlattenAnyOfOneOf(node map[string]any) {
	for _, key := range []string{"anyOf", "oneOf"} {
		entries, ok := node[key].([]any)
		if !ok {
			continue
		}
		delete(node, key)
		var best any
		bestScore := -1
		for _, entry := range entries {
			if sub, ok := entry.(map[string]any); ok && sub["type"] == "null" {
				continue
			}
			if score := antigravityVariantScore(entry); score > bestScore {
				best = entry
				bestScore = score
			}
		}
		if selected, ok := best.(map[string]any); ok {
			for name, value := range selected {
				node[name] = value
			}
		}
	}
}

func antigravityFlattenTypeArrays(node map[string]any) {
	types, ok := node["type"].([]any)
	if !ok {
		return
	}
	flattened := "string"
	for _, value := range types {
		if name, ok := value.(string); ok && name != "null" {
			flattened = name
			break
		}
	}
	node["type"] = flattened
}

func antigravityEnsureObjectType(node map[string]any) {
	if _, hasProperties := node["properties"]; !hasProperties {
		return
	}
	if _, hasType := node["type"]; !hasType {
		node["type"] = "object"
	}
}

func antigravityEnsureArrayItems(node map[string]any) {
	if node["type"] != "array" {
		return
	}
	if _, hasItems := node["items"]; !hasItems {
		node["items"] = map[string]any{"type": "string"}
	}
}

func removeAntigravityUnsupportedKeywords(node any) {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			if antigravityUnsupportedSchemaKeys[key] || strings.HasPrefix(key, "x-") {
				delete(typed, key)
				continue
			}
			removeAntigravityUnsupportedKeywords(value)
		}
	case []any:
		for _, item := range typed {
			removeAntigravityUnsupportedKeywords(item)
		}
	}
}

func antigravityCleanupRequired(node map[string]any) {
	required, ok := node["required"].([]any)
	if !ok {
		return
	}
	properties, ok := node["properties"].(map[string]any)
	if !ok {
		return
	}
	kept := make([]any, 0, len(required))
	for _, entry := range required {
		name, ok := entry.(string)
		if !ok {
			continue
		}
		if _, present := properties[name]; present {
			kept = append(kept, name)
		}
	}
	if len(kept) == 0 {
		delete(node, "required")
		return
	}
	node["required"] = kept
}

func antigravityReasonProperty() map[string]any {
	return map[string]any{
		"type":        "string",
		"description": "Brief explanation of why you are calling this tool",
	}
}

func addAntigravitySchemaPlaceholders(node any) {
	object, ok := node.(map[string]any)
	if !ok {
		return
	}
	if len(object) == 0 {
		object["type"] = "object"
		object["properties"] = map[string]any{"reason": antigravityReasonProperty()}
		object["required"] = []any{"reason"}
	}
	if object["type"] == "object" {
		properties, _ := object["properties"].(map[string]any)
		if len(properties) == 0 {
			object["properties"] = map[string]any{"reason": antigravityReasonProperty()}
			object["required"] = []any{"reason"}
		}
	}
	for _, value := range object {
		addAntigravitySchemaPlaceholders(value)
	}
}

// cleanAntigravityToolSchema normalizes OpenAI-style tool parameter JSON into
// a schema the Antigravity function-declaration proto accepts: $ref
// resolution, const/enum/allOf/prefixItems/anyOf/oneOf/type-array flattening,
// unsupported keyword stripping, required cleanup, and placeholder properties
// for empty objects. The input map is never mutated.
func cleanAntigravityToolSchema(params map[string]any) map[string]any {
	if params == nil {
		params = map[string]any{}
	}
	root, ok := dereferenceAntigravitySchema(params, map[string]any{}, map[string]bool{}).(map[string]any)
	if !ok {
		root = map[string]any{}
	}
	for _, phase := range []func(map[string]any){
		antigravityConvertConstToEnum,
		antigravityConvertEnumValuesToStrings,
		antigravityMergeAllOf,
		antigravityConvertPrefixItems,
		antigravityFlattenAnyOfOneOf,
		antigravityFlattenTypeArrays,
		antigravityEnsureObjectType,
		antigravityEnsureArrayItems,
	} {
		walkAntigravitySchemaNodes(root, phase)
	}
	removeAntigravityUnsupportedKeywords(root)
	walkAntigravitySchemaNodes(root, antigravityCleanupRequired)
	addAntigravitySchemaPlaceholders(root)
	return root
}

// sanitizeAntigravityToolName enforces Gemini's
// ^[a-zA-Z_][a-zA-Z0-9_.:\-]{0,63}$ tool naming rule.
func sanitizeAntigravityToolName(name string) string {
	sanitized := antigravityToolNameInvalidRunes.ReplaceAllString(name, "_")
	if sanitized == "" {
		return "_unknown"
	}
	if first := sanitized[0]; !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		sanitized = "_" + sanitized
	}
	if len(sanitized) > 64 {
		sanitized = sanitized[:64]
	}
	return sanitized
}

// antigravityDefaultThoughtSignature is a captured valid Gemini 3 thought
// signature. Gemini 3+ rejects replayed functionCall parts that lack
// thoughtSignature; OpenAI clients never persist it, so the first
// functionCall part of each content is backfilled with this default
// (sibling parallel calls stay unsigned, matching IDE wire behavior).
const antigravityDefaultThoughtSignature = "EuwGCukGAXLI2nxwZIq54WWSoL/YN0P3TsDZ7zRnLi8g0S4aVr2HUGxvaHKySuY6HAVzcE0GPGjXrytLIldxthSvfxgUlJh6Qa9Z+Oj5QZBlYdg6HaJ6yuY5R7waE6rdwBsRf7Ft2j3DJ9rMi9qhWFqApewYtPhls3VHtuvND3l8Rm09+lbAXQs6KKWEWrxNLKTBkfpMgXhRERc/TQRMZu1twAablm6/Zk1tsYRvfWKLsNbeKF+CCojJdXJKvnR/8Ouuoa+Y2Ti20hcW7aZIIjZDFYPU//k6Ybmhg69J/imbFai2ckhfLaisqdDkdoIiBJScTOUvYqP6AE9d4MsydSC+UlhIMk4hoP76R8vUSCZRMkjOaDXstf/QoVZKbt94wyRZgAJ1G0BqI8L5ow86kLpA4wJEtxsRGymOE4bKUvApveBakYDNM9APkf+LbtbzWSseGjoZcSlycF9iN8Q2XNYKRrHbv3Lr5Y8JjdH/5y/6SHkNehTEZugaeGnSPSyCTWto1kQgHpxdWmhkLfJGNUGLmue7Mesj4TSms4J33mRpYVhNB/J333FCqIP0hr/E7BkkjEn7yZ4X7SQlh+xKPurapsnHRwiKmtsilmEFrnTE9iQr+pMr6M29qqFNv1tr5yumbaJw8JW9sB15tNsRv+dW6BjNanbsKz7HCgKUBc8tGy+7YuhXzAfViyRefcjK7eZW0Fbyt7AbybJTKz78W8NH7ye6LAwzOebXpeZ4D43fNIt8bKh26qgduSQv/7o+pAflkuqHZ99YWgHQ8h8OkZFi3eOiSYjsjhdZ/czWOdoPI/OnqIldzMPF5YlrKBLFX8VhRKVmqgsmWf5PHGulHhMkVlS+XG2UIseGy69ARa93D78Gsa+1n1kJr7EEB7Rh+27vUMxVYLdz1yMSvE5nalTAlg/ZeG8+XQ0cHuAI3KbQpHW2Q++RdXfm5JzD5WdJZUU+Zn8t8UUn85BH4RxZLeE0qJikgSsKoYVBc6YhiMjhPgkR95ReimY4Z0xCJdRo1gjexOFeODZMpQF6Yxnoic7IrdgsFA3iePTbFnPp3IAM1fAThWhXJUn3QInUOTd5o1qmTmn6REbL15g/JQNl+dqUoPkhleeb2V3kjqp1okmO3wMZbPknR3S1LZNmlS72/iBQUm+n2b/RCn4PjmM2"

// antigravityThoughtSignatures caches real thoughtSignature values returned by
// upstream, keyed by functionCall.id, so replayed tool calls carry the genuine
// signature instead of the degraded default. Process-local is sufficient: a
// miss falls back to the default signature (same as a cold start).
var antigravityThoughtSignatures = struct {
	sync.Mutex
	m map[string]antigravitySignatureEntry
}{m: map[string]antigravitySignatureEntry{}}

type antigravitySignatureEntry struct {
	sig string
	at  time.Time
}

const (
	antigravitySignatureTTL = time.Hour
	antigravitySignatureMax = 10000
)

func storeAntigravityThoughtSignature(callID, sig string) {
	if callID == "" || sig == "" {
		return
	}
	antigravityThoughtSignatures.Lock()
	defer antigravityThoughtSignatures.Unlock()
	if len(antigravityThoughtSignatures.m) >= antigravitySignatureMax {
		for id, e := range antigravityThoughtSignatures.m {
			if time.Since(e.at) > antigravitySignatureTTL {
				delete(antigravityThoughtSignatures.m, id)
			}
		}
		for len(antigravityThoughtSignatures.m) >= antigravitySignatureMax {
			for id := range antigravityThoughtSignatures.m {
				delete(antigravityThoughtSignatures.m, id)
				break
			}
		}
	}
	antigravityThoughtSignatures.m[callID] = antigravitySignatureEntry{sig: sig, at: time.Now()}
}

func getAntigravityThoughtSignature(callID string) string {
	antigravityThoughtSignatures.Lock()
	defer antigravityThoughtSignatures.Unlock()
	e, ok := antigravityThoughtSignatures.m[callID]
	if !ok || time.Since(e.at) > antigravitySignatureTTL {
		return ""
	}
	return e.sig
}

// resolveAntigravityThinkingLevel maps a wire model id to the thinking tier
// the official client always sends (gemini >= 3.x). MINIMAL is never emitted:
// it 400s upstream. (Parity: 9router resolveAntigravityThinkingLevel.)
func resolveAntigravityThinkingLevel(model string) string {
	id := strings.ToLower(strings.TrimSpace(model))
	if !strings.HasPrefix(id, "gemini-") || strings.Contains(id, "image") {
		return ""
	}
	if strings.HasSuffix(id, "-high") || id == "gemini-pro-agent" || id == "gemini-3-flash-agent" {
		return "HIGH"
	}
	if strings.HasSuffix(id, "-extra-low") || strings.HasSuffix(id, "-low") {
		return "LOW"
	}
	if strings.HasSuffix(id, "-medium") {
		return "MEDIUM"
	}
	return ""
}

func antigravityRequest(input ChatRequest, model, project string) ([]byte, error) {
	contents := make([]map[string]any, 0, len(input.Messages))
	var system []string
	stepCount := 0
	// tool_call_id -> sanitized function name, so tool messages that omit
	// `name` (allowed by the OpenAI spec) still produce a valid
	// functionResponse.name — Gemini rejects empty names with 400.
	toolCallNames := map[string]string{}
	for _, message := range input.Messages {
		if message.Role == "system" || message.Role == "developer" {
			text := rawText(message.Content)
			normalized := normalizeAntigravitySystemInstruction(text)
			auditAntigravityPrompt(model, text, normalized)
			if normalized != "" {
				system = append(system, normalized)
			}
			continue
		}
		stepCount++
		role := "user"
		if message.Role == "assistant" {
			role = "model"
		}
		parts := []map[string]any{}
		if text := rawText(message.Content); text != "" {
			parts = append(parts, map[string]any{"text": text})
		}
		firstFunctionCall := true
		for _, call := range message.ToolCalls {
			var args any = map[string]any{}
			_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
			name := sanitizeAntigravityToolName(call.Function.Name)
			if call.ID != "" {
				toolCallNames[call.ID] = name
			}
			part := map[string]any{"functionCall": map[string]any{"name": name, "args": args, "id": call.ID}}
			// Gemini 3+ rejects replayed functionCall parts without a
			// thoughtSignature. Prefer the real signature captured from the
			// upstream response; fall back to the default only for the first
			// call per content (sibling parallel calls stay unsigned).
			if sig := getAntigravityThoughtSignature(call.ID); sig != "" {
				part["thoughtSignature"] = sig
			} else if firstFunctionCall {
				part["thoughtSignature"] = antigravityDefaultThoughtSignature
			}
			firstFunctionCall = false
			parts = append(parts, part)
		}
		if message.Role == "tool" {
			name := message.Name
			if name == "" {
				name = toolCallNames[message.ToolCallID]
			}
			name = sanitizeAntigravityToolName(name)
			parts = []map[string]any{{"functionResponse": map[string]any{"name": name, "id": message.ToolCallID, "response": map[string]any{"result": rawText(message.Content)}}}}
		}
		if len(parts) > 0 {
			contents = append(contents, map[string]any{"role": role, "parts": parts})
		}
	}
	request := map[string]any{"contents": contents}
	generation := map[string]any{}
	if n := input.OutputLimit(); n > 0 {
		generation["maxOutputTokens"] = n
	}
	if input.Temperature != nil {
		generation["temperature"] = *input.Temperature
	}
	// The official client always sends a thinking tier for gemini >= 3.x;
	// client-requested effort wins, otherwise derive from the wire model id.
	thinkingLevel := resolveAntigravityThinkingLevel(model)
	if input.Reasoning != nil && input.Reasoning.Effort != "" {
		thinkingLevel = strings.ToUpper(input.Reasoning.Effort)
	}
	if thinkingLevel != "" {
		generation["thinkingConfig"] = map[string]any{"thinkingLevel": thinkingLevel, "includeThoughts": true}
	}
	if len(generation) > 0 {
		request["generationConfig"] = generation
	}
	if len(system) > 0 {
		request["systemInstruction"] = map[string]any{"role": "user", "parts": []map[string]any{{"text": strings.Join(system, "\n\n")}}}
	}
	if len(input.Tools) > 0 {
		declarations := make([]map[string]any, 0, len(input.Tools))
		seen := map[string]bool{}
		for _, tool := range input.Tools {
			name := sanitizeAntigravityToolName(tool.Function.Name)
			if seen[name] {
				continue
			}
			seen[name] = true
			var params map[string]any
			if len(tool.Function.Parameters) > 0 {
				_ = json.Unmarshal(tool.Function.Parameters, &params)
			}
			declarations = append(declarations, map[string]any{
				"name": name, "description": tool.Function.Description,
				"parameters": cleanAntigravityToolSchema(params),
			})
		}
		request["tools"] = []map[string]any{{"functionDeclarations": declarations}}
		request["toolConfig"] = map[string]any{"functionCallingConfig": map[string]any{"mode": "VALIDATED"}}
	}

	reqID := buildAntigravityRequestID(stepCount)
	labels := buildAntigravityLabels(model, stepCount, contents)
	if len(labels) > 0 {
		request["labels"] = labels
	}

	envelope := map[string]any{
		"model":       model,
		"userAgent":   "antigravity",
		"requestType": "agent",
		"requestId":   reqID,
		"request":     request,
	}
	if project != "" {
		envelope["project"] = project
	}
	return json.Marshal(envelope)
}

func buildAntigravityRequestID(stepCount int) string {
	now := time.Now()
	return fmt.Sprintf("agent/%d/%d/%x/%d", now.Unix(), now.UnixMilli(), now.UnixNano()&0xFFFFFF, stepCount)
}

func buildAntigravityLabels(model string, stepCount int, contents []map[string]any) map[string]string {
	labels := make(map[string]string)
	normalizedModel := strings.ToLower(strings.TrimSpace(model))
	if enum, ok := antigravityWireProfiles[normalizedModel]; ok {
		labels["model_enum"] = enum
	}
	if strings.Contains(normalizedModel, "claude") {
		labels["used_claude"] = "1"
	}
	lastStep := 0
	if stepCount > 0 {
		lastStep = stepCount - 1
	}
	labels["last_step_index"] = fmt.Sprintf("%d", lastStep)

	// Deterministic trajectory ID from conversation content or timestamp
	if len(contents) > 0 {
		raw, _ := json.Marshal(contents)
		h := sha256.Sum256(raw)
		labels["trajectory_id"] = hex.EncodeToString(h[:8])
	} else {
		labels["trajectory_id"] = fmt.Sprintf("%x", time.Now().UnixNano()&0xFFFFFFFF)
	}
	return labels
}

var (
	reOpenCode = regexp.MustCompile(`(?i)\bopencode\b`)
	reZCode    = regexp.MustCompile(`(?i)\bzcode\b`)
	reZAI      = regexp.MustCompile(`(?i)\bz\.ai\b`)
	reOhMyPi   = regexp.MustCompile(`(?i)\boh[- ]my[- ]pi\b`)
	reOMP      = regexp.MustCompile(`(?i)\bomp\b`)
)

func normalizeAntigravitySystemInstruction(text string) string {
	if strings.TrimSpace(text) == "" {
		return ""
	}
	out := text
	// 1. Strip Claude Code / Claude Agent SDK exact identification phrases
	out = strings.ReplaceAll(out, "You are a Claude agent, built on Anthropic's Claude Agent SDK.", "")
	out = strings.ReplaceAll(out, "You are Claude Code, Anthropic's official CLI for Claude.", "")
	// 2. Strip Codex / Pi / OMP persona phrases
	out = strings.ReplaceAll(out, "You are Codex, a coding agent from OpenAI.", "")
	out = strings.ReplaceAll(out, "You are Codex, a coding agent from OpenAI", "")
	out = strings.ReplaceAll(out, "You are Pi, a fast and rigorous autonomous coding agent harness.", "")
	out = strings.ReplaceAll(out, "You are Pi, a fast and rigorous autonomous coding agent harness", "")
	out = strings.ReplaceAll(out, "You are an agent in Oh My Pi.", "")
	out = strings.ReplaceAll(out, "You are an agent in Oh My Pi", "")
	out = strings.ReplaceAll(out, "You are Oh My Pi.", "")
	out = strings.ReplaceAll(out, "You are Oh My Pi", "")
	// 3. Strip generic proxy prefixes
	out = strings.ReplaceAll(out, "google-antigravity/", "")
	// 4. Word-boundary regex swaps for OpenCode, ZCode, Oh-My-Pi, z.ai
	out = reOpenCode.ReplaceAllStringFunc(out, func(m string) string {
		return matchCase(m, "Antigravity")
	})
	out = reZCode.ReplaceAllStringFunc(out, func(m string) string {
		return matchCase(m, "Antigravity")
	})
	out = reOhMyPi.ReplaceAllStringFunc(out, func(m string) string {
		return matchCase(m, "Antigravity")
	})
	out = reZAI.ReplaceAllString(out, "Google DeepMind")
	return strings.TrimSpace(out)
}

func matchCase(src, target string) string {
	if src == strings.ToUpper(src) {
		return strings.ToUpper(target)
	}
	if len(src) > 0 && strings.ToUpper(src[:1]) == src[:1] && strings.ToLower(src[1:]) == src[1:] {
		return target
	}
	if src == strings.ToLower(src) {
		return strings.ToLower(target)
	}
	return target
}

func retryAfterSeconds(v string) int {
	var s int
	if _, err := fmt.Sscanf(strings.TrimSpace(v), "%d", &s); err == nil {
		return s
	}
	return 0
}

type AntigravityPromptAuditEntry struct {
	Timestamp        time.Time `json:"timestamp"`
	Model            string    `json:"model"`
	Harness          string    `json:"harness"`
	OriginalSHA256   string    `json:"original_sha256"`
	NormalizedSHA256 string    `json:"normalized_sha256"`
	OriginalLength   int       `json:"original_length"`
	NormalizedLength int       `json:"normalized_length"`
	OriginalPrompt   string    `json:"original_prompt,omitempty"`
	NormalizedPrompt string    `json:"normalized_prompt,omitempty"`
}

var promptAuditMu sync.Mutex

func auditAntigravityPrompt(model, original, normalized string) {
	auditFile := strings.TrimSpace(os.Getenv("ANTIGRAVITY_PROMPT_AUDIT_FILE"))
	if auditFile == "" {
		return
	}
	hOrig := sha256.Sum256([]byte(original))
	hNorm := sha256.Sum256([]byte(normalized))
	entry := AntigravityPromptAuditEntry{
		Timestamp:        time.Now().UTC(),
		Model:            model,
		Harness:          detectHarness(original),
		OriginalSHA256:   hex.EncodeToString(hOrig[:]),
		NormalizedSHA256: hex.EncodeToString(hNorm[:]),
		OriginalLength:   len(original),
		NormalizedLength: len(normalized),
	}
	includeContent := os.Getenv("ANTIGRAVITY_AUDIT_INCLUDE_PROMPT")
	if includeContent == "true" || includeContent == "1" {
		entry.OriginalPrompt = original
		entry.NormalizedPrompt = normalized
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	promptAuditMu.Lock()
	defer promptAuditMu.Unlock()
	_ = os.MkdirAll(filepath.Dir(auditFile), 0700)
	f, err := os.OpenFile(auditFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(data, '\n'))
}

// auditAntigravityUpstreamError records a non-2xx upstream response (status +
// truncated body) to the same opt-in audit file so rejected wire payloads can
// be diagnosed locally. The body is restored for the caller afterwards.
func auditAntigravityUpstreamError(model string, result *entities.UpstreamResult) {
	auditFile := strings.TrimSpace(os.Getenv("ANTIGRAVITY_PROMPT_AUDIT_FILE"))
	if auditFile == "" || result == nil || result.Body == nil {
		return
	}
	body, err := io.ReadAll(io.LimitReader(result.Body, 64<<10))
	_ = result.Body.Close()
	result.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		return
	}
	entry := map[string]any{
		"timestamp": time.Now().UTC(),
		"type":      "upstream_error",
		"model":     model,
		"status":    result.StatusCode,
		"body":      string(body),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	promptAuditMu.Lock()
	defer promptAuditMu.Unlock()
	_ = os.MkdirAll(filepath.Dir(auditFile), 0700)
	f, err := os.OpenFile(auditFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(data, '\n'))
}

func detectHarness(text string) string {
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "claude agent sdk") || strings.Contains(lower, "claude code"):
		return "claude-code"
	case reOpenCode.MatchString(text):
		return "opencode"
	case reOhMyPi.MatchString(text) || strings.Contains(lower, "oh my pi") || strings.Contains(lower, "oh-my-pi"):
		return "oh-my-pi"
	case strings.Contains(lower, "you are pi"):
		return "pi"
	case reZCode.MatchString(text) || reZAI.MatchString(text):
		return "zcode"
	case strings.Contains(lower, "you are codex"):
		return "codex"
	default:
		return "generic"
	}
}

func rawText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var parts []map[string]any
	if json.Unmarshal(raw, &parts) == nil {
		var b strings.Builder
		for _, part := range parts {
			if value, ok := part["text"].(string); ok {
				b.WriteString(value)
			}
		}
		return b.String()
	}
	return string(raw)
}

func antigravityResponse(body []byte, model string) ([]byte, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	raw := body
	if value := envelope["response"]; len(value) > 0 {
		raw = value
	}
	var response struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text             string `json:"text"`
					Thought          bool   `json:"thought"`
					ThoughtSignature string `json:"thoughtSignature"`
					FunctionCall     *struct {
						Name string          `json:"name"`
						Args json.RawMessage `json:"args"`
						ID   string          `json:"id"`
					} `json:"functionCall"`
				} `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		Usage struct {
			Prompt     int64 `json:"promptTokenCount"`
			Completion int64 `json:"candidatesTokenCount"`
			Cached     int64 `json:"cachedContentTokenCount"`
			Thoughts   int64 `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	message := ResponseMessage{Role: "assistant"}
	for _, candidate := range response.Candidates {
		var pendingSignature string
		for _, part := range candidate.Content.Parts {
			if part.ThoughtSignature != "" {
				pendingSignature = part.ThoughtSignature
			}
			// Model-internal thinking is never surfaced as assistant content.
			if !part.Thought {
				message.Content += part.Text
			}
			if part.FunctionCall != nil {
				if pendingSignature != "" {
					storeAntigravityThoughtSignature(part.FunctionCall.ID, pendingSignature)
					pendingSignature = ""
				}
				message.ToolCalls = append(message.ToolCalls, ToolCall{ID: part.FunctionCall.ID, Type: "function", Function: ToolFunction{Name: part.FunctionCall.Name, Arguments: string(part.FunctionCall.Args)}})
			}
		}
	}
	finish := "stop"
	if len(message.ToolCalls) > 0 {
		finish = "tool_calls"
	}
	uncachedPrompt := max(int64(0), response.Usage.Prompt-response.Usage.Cached)
	out := Response{ID: fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()), Object: "chat.completion", Created: time.Now().Unix(), Model: model, Choices: []Choice{{Index: 0, Message: &message, FinishReason: finish}}, Usage: Usage{PromptTokens: uncachedPrompt, CompletionTokens: response.Usage.Completion + response.Usage.Thoughts, CacheReadTokens: response.Usage.Cached}}
	return json.Marshal(out)
}

func antigravityStream(upstream io.ReadCloser, model string) io.ReadCloser {
	reader, writer := io.Pipe()
	go func() {
		defer upstream.Close()
		defer writer.Close()
		scanner := bufio.NewScanner(upstream)
		scanner.Buffer(make([]byte, 64<<10), 16<<20)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "" {
				continue
			}
			converted, err := antigravityResponse([]byte(payload), model)
			if err != nil {
				continue
			}
			var response Response
			if json.Unmarshal(converted, &response) != nil || len(response.Choices) == 0 {
				continue
			}
			delta := Delta{Role: "assistant", Content: response.Choices[0].Message.Content, ToolCalls: response.Choices[0].Message.ToolCalls}
			chunk := Chunk{ID: response.ID, Object: "chat.completion.chunk", Created: response.Created, Model: model, Choices: []ChunkChoice{{Index: 0, Delta: delta, FinishReason: response.Choices[0].FinishReason}}, Usage: &response.Usage}
			encoded, _ := json.Marshal(chunk)
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", encoded)
		}
		_, _ = io.WriteString(writer, "data: [DONE]\n\n")
	}()
	return reader
}

func (a *AntigravityAdapter) Probe(ctx context.Context, cr *entities.CredentialRuntime) (int, error) {
	base := strings.TrimRight(cr.BaseURL, "/")
	if base == "" {
		base = antigravityRuntimeBaseURL
	}
	payload := []byte(`{"metadata":{"ideType":"ANTIGRAVITY","platform":"PLATFORM_UNSPECIFIED","pluginType":"GEMINI"}}`)
	res, err := postJSON(ctx, a.client(), base+"/v1internal:loadCodeAssist", map[string]string{"Authorization": "Bearer " + cr.OAuthAccess, "Accept": "application/json"}, payload)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	return res.StatusCode, nil
}
func (a *AntigravityAdapter) DiscoverModels(ctx context.Context, cr *entities.CredentialRuntime) ([]credential.ProviderModel, error) {
	if cr == nil || strings.TrimSpace(cr.OAuthAccess) == "" {
		return nil, fmt.Errorf("Antigravity OAuth token is unavailable")
	}
	configuredBase := strings.TrimRight(cr.BaseURL, "/")
	bases := []string{configuredBase}
	if configuredBase == "" || configuredBase == antigravityRuntimeBaseURL || configuredBase == antigravityFallbackBaseURL {
		bases = []string{antigravityRuntimeBaseURL, antigravityFallbackBaseURL, "https://daily-cloudcode-pa.sandbox.googleapis.com"}
	}
	body := []byte(`{}`)
	if project := strings.TrimSpace(cr.OAuthMeta.ProjectID); project != "" {
		body, _ = json.Marshal(struct {
			Project string `json:"project"`
		}{Project: project})
	}
	var lastStatus int
	for _, base := range bases {
		result, err := postJSON(ctx, a.client(), base+"/v1internal:fetchAvailableModels", map[string]string{
			"Authorization": "Bearer " + cr.OAuthAccess,
			"Accept":        "application/json",
			"User-Agent":    antigravityUserAgent,
		}, body)
		if err != nil {
			continue
		}
		lastStatus = result.StatusCode
		if result.StatusCode < 200 || result.StatusCode >= 300 {
			_, _ = io.Copy(io.Discard, io.LimitReader(result.Body, 1<<20))
			result.Body.Close()
			if result.StatusCode == http.StatusUnauthorized || result.StatusCode == http.StatusForbidden {
				break
			}
			continue
		}
		payload, readErr := io.ReadAll(io.LimitReader(result.Body, 8<<20))
		result.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		models, parseErr := parseAntigravityModels(payload)
		if parseErr != nil {
			return nil, parseErr
		}
		if len(models) > 0 {
			return models, nil
		}
	}
	if lastStatus != 0 {
		return nil, fmt.Errorf("Antigravity model discovery returned HTTP %d", lastStatus)
	}
	return nil, fmt.Errorf("Antigravity model discovery failed")
}

func parseAntigravityModels(payload []byte) ([]credential.ProviderModel, error) {
	var response struct {
		Models map[string]struct {
			DisplayName     string `json:"displayName"`
			Description     string `json:"description"`
			IsInternal      bool   `json:"isInternal"`
			ContextWindow   int64  `json:"contextWindow"`
			MaxOutputTokens int64  `json:"maxOutputTokens"`
		} `json:"models"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		return nil, fmt.Errorf("parse Antigravity models: %w", err)
	}
	ids := make([]string, 0, len(response.Models))
	for id, item := range response.Models {
		if item.IsInternal || !antigravityChatModel(id) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	models := make([]credential.ProviderModel, 0, len(ids))
	for _, id := range ids {
		item := response.Models[id]
		models = append(models, credential.ProviderModel{
			ID: id, Object: "model", OwnedBy: "google-antigravity", Name: item.DisplayName,
			Description: item.Description, ContextLength: item.ContextWindow, MaxOutputTokens: item.MaxOutputTokens,
			InputModalities: []string{"text", "image"}, OutputModalities: []string{"text"},
		})
	}
	return models, nil
}

func antigravityChatModel(id string) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return false
	}
	for _, token := range []string{"image", "imagen", "audio", "tts", "embedding", "embed", "video", "veo", "tab_"} {
		if strings.Contains(id, token) {
			return false
		}
	}
	return strings.Contains(id, "gemini") || strings.Contains(id, "claude") || strings.Contains(id, "gpt")
}

func (a *AntigravityAdapter) RefreshToken(ctx context.Context, cr *entities.CredentialRuntime) error {
	return refreshOAuthForm(ctx, a.HTTP, a.Persister, cr, "https://oauth2.googleapis.com/token", a.ClientID, a.ClientSecret, nil)
}
