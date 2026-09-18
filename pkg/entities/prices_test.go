package entities

import "testing"

func TestCalculateCostAndMissingPrice(t *testing.T) {
	u := TokenUsage{PromptTokens: 1_000_000, CompletionTokens: 100_000, CacheReadTokens: 10, CacheWriteTokens: 20}
	missing := CalculateCost(nil, u)
	if missing.Priced || missing.USD != 0 {
		t.Fatalf("missing price must remain explicitly unpriced: %+v", missing)
	}
	p := &Price{InputPerM: 3, OutputPerM: 15, CachedInputPerM: 0.3, CacheWritePerM: 3.75}
	got := CalculateCost(p, u)
	want := 3.0 + 1.5 + 10*0.3/1e6 + 20*3.75/1e6
	if !got.Priced || got.USD != want {
		t.Fatalf("got %+v, want priced cost %.10f", got, want)
	}
}

func TestZeroPriceIsStillPriced(t *testing.T) {
	got := CalculateCost(&Price{}, TokenUsage{PromptTokens: 10})
	if !got.Priced || got.USD != 0 {
		t.Fatalf("zero-priced model must not be reported as unpriced: %+v", got)
	}
}

func TestCalculateCostFallsBackToInputRateForUnpricedCache(t *testing.T) {
	u := TokenUsage{PromptTokens: 100, CompletionTokens: 10, CacheReadTokens: 1_000_000, CacheWriteTokens: 500_000}
	got := CalculateCost(&Price{InputPerM: 2, OutputPerM: 8}, u)
	want := 100*2/1e6 + 10*8/1e6 + 2.0 + 1.0
	if !got.Priced || got.USD != want || got.CacheReadUSD != 2.0 || got.CacheWriteUSD != 1.0 {
		t.Fatalf("unpriced cache must fall back to input rate: %+v want %.10f", got, want)
	}
}

func TestEstimateCostsDerivesCachedAndUncachedTotals(t *testing.T) {
	p := &Price{InputPerM: 2, OutputPerM: 4, CachedInputPerM: 0.2}
	estimates := EstimateCosts(p, 1_000_000, 500_000, true)
	if estimates.WithoutCache.USD != 4 || estimates.WithCache.USD != 2.2 {
		t.Fatalf("estimates = %+v", estimates)
	}
	unsupported := EstimateCosts(p, 1_000_000, 500_000, false)
	if unsupported.WithCache.USD != unsupported.WithoutCache.USD {
		t.Fatalf("unsupported cache estimate = %+v", unsupported)
	}
}
