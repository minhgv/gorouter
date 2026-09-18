# Cost cache accounting + API key endpoints + cache warning

## Context

User report (vi):
- API key page must list endpoints for easy copying.
- Cost is computed incorrectly; review by updating prices and fee calculation (must include cache).
- Warn when cache (provider cache-read share) is below 90%.

Findings from code inspection:
- `llm.Usage.UnmarshalJSON` only subtracts cached tokens from `prompt_tokens` when
  `prompt_tokens_details`/`input_tokens_details` are present. Providers reporting
  top-level `cache_read_input_tokens`, `cache_creation_input_tokens`, or
  `cached_tokens` get double-charged (input rate + cache rate).
- `entities.CalculateCost` charges $0 for cache tokens when the cache rate is 0,
  while `EstimateCosts` falls back to the input rate. Inconsistent.
- `/v1/responses` usage mapping drops `CacheWriteTokens` from `input_tokens` and
  `total_tokens` (non-stream and stream paths).
- Price catalog auto-syncs hourly (`cfg.Pricing.Enabled`); no manual trigger.
- CachePage already computes read share = read/(input+read); no warning.
- KeysPage has no endpoint list; ModelUsageModal has copy-button pattern.

Non-goals: no provider adapter changes; no schema/migration changes; no changes
to router response-cache semantics.

## Approach

Backend:
- `internal/platform/llm/types.go`: after details/miss/hit branches, subtract
  top-level `cacheRead+cacheWrite` from `totalInput` when none of the earlier
  branches applied (they are subsets of prompt/input_tokens).
- `pkg/entities/prices.go`: `CalculateCost` falls back to `InputPerM` for cache
  read/write when the specific rate is 0 but tokens exist — mirrors
  `EstimateCosts` "never present cache reads as free" rule.
- `internal/api/handlers/responses.go`: `input_tokens` = prompt+read+write;
  `total_tokens` = input+output; stream path adds `cache_write_tokens` detail.
- New `POST /admin/pricing/sync` (scope `models:manage`, master-only like
  `Price`): calls `PriceSyncer.Sync(ctx)`; 503 when pricing sync disabled.
  Wire `*pricing.CatalogService` through `routes.Dependencies` and `main.go`.

Frontend:
- `src/lib/clipboard.ts`: shared `copyText` (Clipboard API + execCommand
  fallback, extracted from SecretModal).
- KeysPage: "Gateway endpoints" panel listing base URL + `/v1/models`,
  `/v1/chat/completions`, `/v1/responses`, `/v1/messages` with copy buttons.
- CachePage: warning banner + amber accent when read share < 90% (only when
  denominator > 0).
- AnalysisPage HealthTable: cache-read cell gets `health-rate` tone
  (good >= 90%, warning < 90%).
- ModelsPage: "Sync prices" button calling the new endpoint; reloads catalog.
- CSS: `.warning-banner`, `.endpoint-list` styles.

## Critical files and ownership

- `internal/platform/llm/types.go`, `cache_test.go` — usage normalization
- `pkg/entities/prices.go`, `prices_test.go` — cost math
- `internal/api/handlers/responses.go`, `admin.go` — API surface
- `internal/api/routes/routes.go`, `cmd/gorouter/main.go` — wiring
- `src/pages/{KeysPage,CachePage,AnalysisPage,ModelsPage}.tsx`,
  `src/lib/clipboard.ts`, `src/api/client.ts`, `src/styles/app.css`
- Generated: `internal/docs/*` (swag), `internal/api/spa/dist` (npm build)

## Verification

- AC-01: `Usage{prompt_tokens:1000, cache_read_input_tokens:900}` →
  PromptTokens=100, CacheReadTokens=900 (unit test).
- AC-02: `CalculateCost` with cache tokens and zero cache rate charges input
  rate, not $0 (unit test).
- AC-03: `/v1/responses` total_tokens includes cache write tokens.
- AC-04: `POST /admin/pricing/sync` returns 503 without syncer, 200 with stub.
- AC-05: KeysPage renders endpoint list with copy buttons (Vitest).
- AC-06: CachePage shows warning when read share < 90% (Vitest).
- AC-07: `go test ./...`, `npm test -- --run`, `npm run build`, swag regen.

## Execution checklist

- [x] T-01 Fix usage normalization + test (AC-01)
- [x] T-02 CalculateCost cache fallback + test (AC-02)
- [x] T-03 /v1/responses token totals (AC-03)
- [x] T-04 Pricing sync endpoint + wiring + swagger (AC-04)
- [x] T-05 KeysPage endpoints panel + clipboard lib (AC-05)
- [x] T-06 Cache <90% warnings (AC-06)
- [x] T-07 ModelsPage sync button + client
- [x] T-08 Full verification suite + SPA build (AC-07)

## Evidence and handoff

- `go test ./...` — all packages pass (incl. new tests:
  `TestUsageNormalizesTopLevelCacheFields`, `TestUsageNormalizesTopLevelCachedTokens`,
  `TestCalculateCostFallsBackToInputRateForUnpricedCache`, `TestPricingSyncRequiresMasterAndSyncer`).
- `go vet ./...` — clean; `gofmt` applied to all touched Go files.
- `npm test -- --run` — 62/62 pass (new: CachePage 90% warning tests,
  KeysPage endpoint list test).
- `npm run build` — regenerated `internal/api/spa/dist` (committed bundle).
- Swag regen — `internal/docs/{docs.go,swagger.json,swagger.yaml}` include
  `POST /admin/pricing/sync`; `TestSwaggerDocumentsEveryJSONRoute` updated+passing.
- `git diff --check` — clean.

Residual risks:
- Providers emitting top-level `cache_read_tokens`/`cache_write_tokens` with
  subset semantics would now undercount input (those names are our normalized
  output fields; subtraction is restricted to provider-specific names to keep
  Usage round-trips idempotent).
- `CalculateCost` input-rate fallback may overcharge vs. a provider whose
  cache reads are genuinely free — conservative direction (never undercharge).

## Assumptions and contingencies


- "Cache dưới 90%" = provider cache-read share already shown on CachePage
  (read/(input+read)); warning also applied to HealthTable cache-read column.
- Top-level cache token fields are subsets of prompt/input_tokens (OpenAI wire
  convention); Anthropic wire is handled by adapters, not UnmarshalJSON.
