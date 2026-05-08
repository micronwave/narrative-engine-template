/**
 * Audit Tests — API Client & Config
 *
 * Tests targeting issues found during code forensics audit of:
 *   next.config.mjs, api.ts, colors.ts, metrics.ts, tailwind.config.ts, layout.tsx
 *
 * URL Encoding:
 *   A-URL-1: fetchNarrativeDetail encodes path-traversal ID
 *   A-URL-2: fetchStockDetail encodes symbol with slash
 *   A-URL-3: fetchBrief encodes ticker with path traversal
 *   A-URL-5: fetchCorrelation encodes both narrativeId and ticker
 *   A-URL-6: removeFromWatchlist encodes itemId
 *   A-URL-7: deleteAlertRule encodes ruleId
 *   A-URL-8: toggleAlertRule encodes ruleId
 *   A-URL-9: markAlertRead encodes notificationId
 *   A-URL-10: exportNarrative encodes narrative ID
 *   A-URL-11: fetchNarrativeManipulation encodes narrative ID
 *   A-URL-12: fetchNarrativeHistory encodes narrative ID
 *   A-URL-13: fetchPriceHistory encodes symbol
 *   A-URL-15: fetchNarrativeCorrelations encodes narrative ID
 *   A-URL-16: fetchNarrativeSources encodes narrative ID
 *   A-URL-18: removePortfolioHolding encodes holdingId
 *   A-URL-20: analyzeNarrative encodes narrative ID
 *   A-URL-21: fetchNarrativeAssets encodes narrative ID
 *
 * Error Handling:
 *   A-ERR-1: markAlertRead throws on non-ok response
 *   A-ERR-2: markAllAlertsRead throws on non-ok response
 *   A-ERR-3: fetchAlertCount returns { unread: 0 } on failure (documented fallback)
 *   A-ERR-4: fetchUpcomingEarnings returns [] on failure (documented fallback)
 *   A-ERR-5: All standard fetch functions throw on non-ok response
 *
 * Colors:
 *   A-CLR-1: COLORS constants match expected hex values
 *   A-CLR-2: COLORS includes purple and muted
 *
 * Metrics:
 *   A-MET-1: METRIC_GLOSSARY has entries for all expected keys
 *   A-MET-2: Every glossary entry has label, computation, and interpretation
 *
 * Query Params:
 *   A-QP-1: fetchStocks builds query string correctly with all params
 *   A-QP-2: fetchStocks omits falsy params from query string
 *   A-QP-3: fetchStocks includes min_impact=0 (falsy but valid)
 *   A-QP-4: fetchManipulation builds query string correctly
 *
 * Adversarial:
 *   A-ADV-1: Narrative ID with "../" is encoded, not traversed
 *   A-ADV-2: Ticker with query injection (?foo=bar) is encoded
 *   A-ADV-4: Empty string ID is still encoded and sent (no silent skip)
 */

import "@testing-library/jest-dom";

// ---------------------------------------------------------------------------
// Mock fetch globally
// ---------------------------------------------------------------------------

const mockFetch = jest.fn();
global.fetch = mockFetch;

function mockOk(data: unknown = {}) {
  mockFetch.mockResolvedValueOnce({
    ok: true,
    status: 200,
    json: () => Promise.resolve(data),
    blob: () => Promise.resolve(new Blob()),
  });
}

function mockFail(status = 500) {
  mockFetch.mockResolvedValueOnce({
    ok: false,
    status,
    json: () => Promise.resolve({ error: "fail" }),
  });
}

beforeEach(() => {
  mockFetch.mockReset();
});

// ---------------------------------------------------------------------------
// Imports — after mock setup
// ---------------------------------------------------------------------------

import {
  fetchNarrativeDetail,
  fetchStockDetail,
  fetchBrief,
  fetchCorrelation,
  removeFromWatchlist,
  deleteAlertRule,
  toggleAlertRule,
  markAlertRead,
  markAllAlertsRead,
  exportNarrative,
  fetchNarrativeManipulation,
  fetchNarrativeHistory,
  fetchPriceHistory,
  fetchNarrativeCorrelations,
  fetchNarrativeSources,
  removePortfolioHolding,
  analyzeNarrative,
  fetchNarrativeAssets,
  fetchAlertCount,
  fetchUpcomingEarnings,
  fetchStocks,
  fetchManipulation,
  fetchNarratives,
  fetchTicker,
  fetchConstellation,
  fetchSignals,
  fetchAssetClasses,
  fetchSecurities,
  fetchActivity,
  fetchWatchlist,
  addToWatchlist,
  createAlertRule,
  fetchAlertRules,
  fetchCoordinationSummary,
  fetchCorrelationMatrix,
  fetchBufferStatus,
  fetchPortfolio,
  addPortfolioHolding,
  fetchPortfolioExposure,
  fetchMomentumLeaderboard,
  fetchNarrativeHistories,
  fetchNarrativeOverlap,
  fetchSectorConvergence,
  fetchLifecycleFunnel,
  fetchLeadTimeDistribution,
  fetchContrarianSignals,
  setApiToken,
  fetchAuthMe,
  logoutAuthSession,
} from "../lib/api";

import { COLORS } from "../lib/colors";
import { METRIC_GLOSSARY } from "../lib/metrics";

// ---------------------------------------------------------------------------
// URL Encoding Tests
// ---------------------------------------------------------------------------

describe("URL encoding — path parameters", () => {
  const DANGEROUS_ID = "../../admin";
  const SLASH_SYMBOL = "BRK/B";
  const QUERY_INJECT = "AAPL?injected=true";

  test("A-URL-1: fetchNarrativeDetail encodes path-traversal ID", async () => {
    mockOk({});
    await fetchNarrativeDetail(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toBe(`/api/narratives/${encodeURIComponent(DANGEROUS_ID)}`);
  });

  test("A-URL-2: fetchStockDetail encodes symbol with slash", async () => {
    mockOk({});
    await fetchStockDetail(SLASH_SYMBOL);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toBe(`/api/stocks/${encodeURIComponent(SLASH_SYMBOL)}`);
  });

  test("A-URL-3: fetchBrief encodes ticker with path traversal", async () => {
    mockOk({});
    await fetchBrief(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toBe(`/api/brief/${encodeURIComponent(DANGEROUS_ID)}`);
  });

  test("A-URL-5: fetchCorrelation encodes both narrativeId and ticker", async () => {
    mockOk({});
    await fetchCorrelation(DANGEROUS_ID, QUERY_INJECT, 3);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
    expect(url).toContain(encodeURIComponent(QUERY_INJECT));
    expect(url).toContain("lead_days=3");
  });

  test("A-URL-6: removeFromWatchlist encodes itemId", async () => {
    mockOk({});
    await removeFromWatchlist(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-7: deleteAlertRule encodes ruleId", async () => {
    mockOk({});
    await deleteAlertRule(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-8: toggleAlertRule encodes ruleId", async () => {
    mockOk({});
    await toggleAlertRule(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-9: markAlertRead encodes notificationId", async () => {
    mockOk();
    await markAlertRead(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-10: exportNarrative encodes narrative ID", async () => {
    mockOk();
    await exportNarrative(DANGEROUS_ID, "token");
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-11: fetchNarrativeManipulation encodes narrative ID", async () => {
    mockOk([]);
    await fetchNarrativeManipulation(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-12: fetchNarrativeHistory encodes narrative ID", async () => {
    mockOk([]);
    await fetchNarrativeHistory(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-13: fetchPriceHistory encodes symbol", async () => {
    mockOk({});
    await fetchPriceHistory(SLASH_SYMBOL);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(SLASH_SYMBOL));
  });

  test("A-URL-15: fetchNarrativeCorrelations encodes narrative ID", async () => {
    mockOk([]);
    await fetchNarrativeCorrelations(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-16: fetchNarrativeSources encodes narrative ID", async () => {
    mockOk([]);
    await fetchNarrativeSources(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-18: removePortfolioHolding encodes holdingId", async () => {
    mockOk({});
    await removePortfolioHolding(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-20: analyzeNarrative encodes narrative ID", async () => {
    mockOk({});
    await analyzeNarrative(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });

  test("A-URL-21: fetchNarrativeAssets encodes narrative ID", async () => {
    mockOk([]);
    await fetchNarrativeAssets(DANGEROUS_ID);
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain(encodeURIComponent(DANGEROUS_ID));
  });
});

// ---------------------------------------------------------------------------
// Error Handling Tests
// ---------------------------------------------------------------------------

describe("Error handling", () => {
  test("A-ERR-1: markAlertRead throws on non-ok response", async () => {
    mockFail(500);
    await expect(markAlertRead("abc")).rejects.toThrow("mark alert read failed: 500");
  });

  test("A-ERR-2: markAllAlertsRead throws on non-ok response", async () => {
    mockFail(500);
    await expect(markAllAlertsRead()).rejects.toThrow("mark all alerts read failed: 500");
  });

  test("A-ERR-3: fetchAlertCount returns { unread: 0 } on failure", async () => {
    mockFail(500);
    const result = await fetchAlertCount();
    expect(result).toEqual({ unread: 0 });
  });

  test("A-ERR-4: fetchUpcomingEarnings returns [] on failure", async () => {
    mockFail(500);
    const result = await fetchUpcomingEarnings();
    expect(result).toEqual([]);
  });

  test("A-ERR-5a: fetchNarratives throws on 401", async () => {
    setApiToken("token");
    mockFail(401);
    await expect(fetchNarratives()).rejects.toThrow("narratives fetch failed: 401");
    setApiToken(null);
  });

  test("A-ERR-5b: fetchTicker throws on 503", async () => {
    mockFail(503);
    await expect(fetchTicker()).rejects.toThrow("ticker fetch failed: 503");
  });

  test("A-ERR-5c: fetchConstellation throws on 500", async () => {
    mockFail(500);
    await expect(fetchConstellation()).rejects.toThrow("constellation fetch failed: 500");
  });

  test("A-ERR-5h: fetchSignals throws on 500", async () => {
    mockFail(500);
    await expect(fetchSignals()).rejects.toThrow("signals fetch failed: 500");
  });

  test("A-ERR-5i: fetchAssetClasses throws on 500", async () => {
    mockFail(500);
    await expect(fetchAssetClasses()).rejects.toThrow("asset classes fetch failed: 500");
  });

  test("A-ERR-5j: fetchSecurities throws on 500", async () => {
    mockFail(500);
    await expect(fetchSecurities()).rejects.toThrow("securities fetch failed: 500");
  });

  test("A-ERR-5k: exportNarrative throws on 403", async () => {
    mockFail(403);
    await expect(exportNarrative("n1", "token")).rejects.toThrow("export failed: 403");
  });

  test("A-ERR-5l: fetchActivity throws on 500", async () => {
    mockFail(500);
    await expect(fetchActivity()).rejects.toThrow("activity fetch failed: 500");
  });

  test("A-ERR-5m: fetchWatchlist throws on 500", async () => {
    mockFail(500);
    await expect(fetchWatchlist()).rejects.toThrow("watchlist fetch failed: 500");
  });

  test("A-ERR-5n: addToWatchlist throws on 400", async () => {
    mockFail(400);
    await expect(addToWatchlist("narrative", "id")).rejects.toThrow("watchlist add failed: 400");
  });

  test("A-ERR-5o: createAlertRule throws on 400", async () => {
    mockFail(400);
    await expect(createAlertRule("burst", "narrative", "n1")).rejects.toThrow(
      "alert rule create failed: 400"
    );
  });

  test("A-ERR-5p: fetchAlertRules throws on 500", async () => {
    mockFail(500);
    await expect(fetchAlertRules()).rejects.toThrow("alert rules fetch failed: 500");
  });

  test("A-ERR-5q: fetchCoordinationSummary throws on 500", async () => {
    mockFail(500);
    await expect(fetchCoordinationSummary()).rejects.toThrow("coordination summary failed: 500");
  });

  test("A-ERR-5r: fetchCorrelationMatrix throws on 500", async () => {
    mockFail(500);
    await expect(fetchCorrelationMatrix()).rejects.toThrow("correlation matrix failed: 500");
  });

  test("A-ERR-5s: fetchBufferStatus throws on 500", async () => {
    mockFail(500);
    await expect(fetchBufferStatus()).rejects.toThrow("buffer status failed: 500");
  });

  test("A-ERR-5t: fetchPortfolio throws on 500", async () => {
    mockFail(500);
    await expect(fetchPortfolio()).rejects.toThrow("portfolio fetch failed: 500");
  });

  test("A-ERR-5u: addPortfolioHolding throws on 400", async () => {
    mockFail(400);
    await expect(addPortfolioHolding("AAPL")).rejects.toThrow("holding add failed: 400");
  });

  test("A-ERR-5v: fetchPortfolioExposure throws on 500", async () => {
    mockFail(500);
    await expect(fetchPortfolioExposure()).rejects.toThrow("exposure fetch failed: 500");
  });

  test("A-ERR-5w: fetchMomentumLeaderboard throws on 500", async () => {
    mockFail(500);
    await expect(fetchMomentumLeaderboard()).rejects.toThrow("momentum-leaderboard failed: 500");
  });

  test("A-ERR-5x: fetchNarrativeHistories throws on 500", async () => {
    mockFail(500);
    await expect(fetchNarrativeHistories()).rejects.toThrow("narrative-histories failed: 500");
  });

  test("A-ERR-5y: fetchNarrativeOverlap throws on 500", async () => {
    mockFail(500);
    await expect(fetchNarrativeOverlap()).rejects.toThrow("narrative-overlap failed: 500");
  });

  test("A-ERR-5z: fetchSectorConvergence throws on 500", async () => {
    mockFail(500);
    await expect(fetchSectorConvergence()).rejects.toThrow("sector-convergence failed: 500");
  });

  test("A-ERR-5aa: fetchLifecycleFunnel throws on 500", async () => {
    mockFail(500);
    await expect(fetchLifecycleFunnel()).rejects.toThrow("lifecycle-funnel failed: 500");
  });

  test("A-ERR-5bb: fetchLeadTimeDistribution throws on 500", async () => {
    mockFail(500);
    await expect(fetchLeadTimeDistribution()).rejects.toThrow(
      "lead-time-distribution failed: 500"
    );
  });

  test("A-ERR-5cc: fetchContrarianSignals throws on 500", async () => {
    mockFail(500);
    await expect(fetchContrarianSignals()).rejects.toThrow("contrarian-signals failed: 500");
  });
});

// ---------------------------------------------------------------------------
// Colors Tests
// ---------------------------------------------------------------------------

describe("COLORS constants", () => {
  test("A-CLR-1: COLORS match expected hex values from globals.css", () => {
    expect(COLORS.bullish).toBe("#32A467");
    expect(COLORS.bearish).toBe("#E76A6E");
    expect(COLORS.alert).toBe("#EC9A3C");
    expect(COLORS.danger).toBe("#E76A6E");
    expect(COLORS.accent).toBe("#2D72D2");
  });

  test("A-CLR-2: COLORS includes purple and muted", () => {
    expect(COLORS.purple).toBe("#A854F7");
    expect(COLORS.muted).toBe("#738091");
  });
});

// ---------------------------------------------------------------------------
// Metrics Glossary Tests
// ---------------------------------------------------------------------------

describe("METRIC_GLOSSARY", () => {
  const EXPECTED_KEYS = [
    "ns_score",
    "velocity",
    "entropy",
    "cohesion",
    "burst_ratio",
    "polarization",
    "similarity_score",
    "correlation",
  ];

  test("A-MET-1: has entries for all expected metric keys", () => {
    for (const key of EXPECTED_KEYS) {
      expect(METRIC_GLOSSARY).toHaveProperty(key);
    }
  });

  test("A-MET-2: every entry has label, computation, and interpretation", () => {
    for (const [key, entry] of Object.entries(METRIC_GLOSSARY)) {
      expect(entry.label).toBeTruthy();
      expect(entry.computation).toBeTruthy();
      expect(entry.interpretation).toBeTruthy();
    }
  });
});

// ---------------------------------------------------------------------------
// Query Parameter Tests
// ---------------------------------------------------------------------------

describe("Query parameter building", () => {
  test("A-QP-1: fetchStocks builds query string with all params", async () => {
    mockOk([]);
    await fetchStocks({ sort_by: "impact", sort_order: "desc", asset_class: "tech", min_impact: 0.5 });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("sort_by=impact");
    expect(url).toContain("sort_order=desc");
    expect(url).toContain("asset_class=tech");
    expect(url).toContain("min_impact=0.5");
  });

  test("A-QP-2: fetchStocks omits falsy params from query string", async () => {
    mockOk([]);
    await fetchStocks({});
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toBe("/api/stocks");
  });

  test("A-QP-3: fetchStocks includes min_impact=0 (falsy but valid)", async () => {
    mockOk([]);
    await fetchStocks({ min_impact: 0 });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("min_impact=0");
  });

  test("A-QP-4: fetchManipulation builds query string correctly", async () => {
    mockOk([]);
    await fetchManipulation({ indicator_type: "bot_network", min_confidence: 0.8, status: "active" });
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("indicator_type=bot_network");
    expect(url).toContain("min_confidence=0.8");
    expect(url).toContain("status=active");
  });
});

// ---------------------------------------------------------------------------
// Adversarial Tests
// ---------------------------------------------------------------------------

describe("Adversarial inputs", () => {
  test("A-ADV-1: narrative ID with '../' is percent-encoded", async () => {
    mockOk({});
    await fetchNarrativeDetail("../../../etc/passwd");
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).not.toContain("../");
    expect(url).toContain("..%2F..%2F..%2Fetc%2Fpasswd");
  });

  test("A-ADV-2: ticker with query injection is encoded", async () => {
    mockOk({});
    await fetchStockDetail("AAPL?admin=true&drop=table");
    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).not.toContain("?admin=true");
    expect(url).toContain(encodeURIComponent("AAPL?admin=true&drop=table"));
  });

  test("A-ADV-4: empty string ID is encoded and sent", async () => {
    mockOk({});
    await fetchNarrativeDetail("");
    const url = mockFetch.mock.calls[0][0] as string;
    // encodeURIComponent("") === "", so URL becomes /api/narratives/
    expect(url).toBe("/api/narratives/");
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// Auth header tests
// ---------------------------------------------------------------------------

describe("Auth headers", () => {
  afterEach(() => {
    setApiToken(null);
  });

  test("fetchNarratives sends x-auth-token via module-level token", async () => {
    setApiToken("my-token");
    mockOk([]);
    await fetchNarratives();
    const opts = mockFetch.mock.calls[0][1] as RequestInit;
    expect((opts.headers as Record<string, string>)["x-auth-token"]).toBe("my-token");
  });

  test("fetchNarratives sends no auth header when token is null", async () => {
    setApiToken(null);
    mockOk([]);
    await fetchNarratives();
    const opts = mockFetch.mock.calls[0][1] as RequestInit;
    expect((opts.headers as Record<string, string>)["x-auth-token"]).toBeUndefined();
  });

  test("fetchTicker sends x-auth-token via module-level token", async () => {
    setApiToken("test-tok");
    mockOk([]);
    await fetchTicker();
    const opts = mockFetch.mock.calls[0][1] as RequestInit;
    expect((opts.headers as Record<string, string>)["x-auth-token"]).toBe("test-tok");
  });

  test("exportNarrative omits x-auth-token when token is not provided", async () => {
    mockOk();
    await exportNarrative("nar-001");
    const opts = mockFetch.mock.calls[0][1] as RequestInit;
    const headers = (opts.headers as Record<string, string>) || {};
    expect(headers["x-auth-token"]).toBeUndefined();
  });

  test("fetchAuthMe calls /api/auth/me with credentials include", async () => {
    mockOk({ auth_mode: "stub", user: { id: "u1" } });
    const result = await fetchAuthMe();
    const url = mockFetch.mock.calls[0][0] as string;
    const opts = mockFetch.mock.calls[0][1] as RequestInit;
    expect(url).toBe("/api/auth/me");
    expect(opts.credentials).toBe("include");
    expect(result).toEqual({ auth_mode: "stub", user: { id: "u1" } });
  });

  test("logoutAuthSession calls /api/auth/logout with POST and credentials include", async () => {
    mockOk({ ok: true });
    await logoutAuthSession();
    const url = mockFetch.mock.calls[0][0] as string;
    const opts = mockFetch.mock.calls[0][1] as RequestInit;
    expect(url).toBe("/api/auth/logout");
    expect(opts.method).toBe("POST");
    expect(opts.credentials).toBe("include");
  });
});
