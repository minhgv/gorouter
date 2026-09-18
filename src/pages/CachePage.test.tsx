import { render, screen, within } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";
import { CachePage } from "./CachePage";

const api = vi.hoisted(() => ({
  getRouterCacheStats: vi.fn(),
  flushRouterCache: vi.fn(),
}));
const activity = vi.hoisted(() => ({ useActivity: vi.fn() }));
vi.mock("../api/client", () => api);
vi.mock("../hooks/useActivity", () => activity);
vi.mock("../hooks/useUsageFilters", () => ({
  useUsageFilters: () => ({
    filters: { range: "7d", groupBy: "hour", filterType: "user", userIds: [], apiKeyIds: [], organizationIds: [], since: "", until: "" },
    setFilters: vi.fn(), users: [], apiKeys: [], organizations: [], models: [],
  }),
}));
vi.mock("../context/SessionContext", () => ({
  useSession: () => ({ has: () => false }),
}));

const bucket = (over: object) => ({
  start: "2026-09-18T00:00:00Z",
  requests: 1,
  prompt_tokens: 10,
  completion_tokens: 5,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
  cost_usd: 0,
  input_cost_usd: 0,
  output_cost_usd: 0,
  cache_read_cost_usd: 0,
  cache_write_cost_usd: 0,
  user_id: "u",
  username: "u",
  ...over,
});

beforeEach(() => {
  api.getRouterCacheStats.mockResolvedValue({});
});

test("warns when provider cache-read share is below 90%", async () => {
  activity.useActivity.mockReturnValue({
    data: [bucket({ prompt_tokens: 400, cache_read_tokens: 100 })],
    loading: false,
    error: "",
    retry: vi.fn(),
  });
  render(<CachePage />);
  expect(
    await screen.findByText(/below the 90% target/),
  ).toBeInTheDocument();
  expect(screen.getAllByText("20.0%").length).toBeGreaterThan(0);
});

test("does not warn at or above the 90% target", async () => {
  activity.useActivity.mockReturnValue({
    data: [bucket({ prompt_tokens: 100, cache_read_tokens: 900 })],
    loading: false,
    error: "",
    retry: vi.fn(),
  });
  const { container } = render(<CachePage />);
  const page = within(container);
  expect((await page.findAllByText("90.0%")).length).toBeGreaterThan(0);
  expect(page.queryByText(/below the 90% target/)).not.toBeInTheDocument();
});
