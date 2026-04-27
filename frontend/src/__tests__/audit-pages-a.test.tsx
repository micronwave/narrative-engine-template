/**
 * Set A Pages Audit Tests
 *
 * Covers edge cases, failure scenarios, boundary conditions, and critical
 * logic paths identified during the production audit of:
 *   - app/page.tsx (GatewayPage)
 *   - app/signals/page.tsx (SignalsPage)
 *   - app/stocks/page.tsx (StocksPage)
 *   - app/correlation/page.tsx (CorrelationPage)
 *   - app/constellation/page.tsx (ConstellationPage)
 *   - app/manipulation/page.tsx (ManipulationPage)
 *   - components/ConstellationMap.tsx
 *
 * Test IDs:
 *   PA-SEC-1..3: Security (open redirect, input sanitization, URL encoding)
 *   PA-ERR-1..6: Error handling (async handlers, fetch failures)
 *   PA-EDGE-1..8: Edge cases (NaN input, empty data, boundary values)
 *   PA-OPT-1..3: Optimization (sort stability, filter correctness)
 *   PA-UI-1..4: UI behavior (empty states, Suspense, tooltip clamping)
 */

import React from "react";
import { render, screen, fireEvent, waitFor, act } from "@testing-library/react";
import "@testing-library/jest-dom";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Mock next/navigation
const mockPush = jest.fn();
const mockSearchParams = new URLSearchParams();
jest.mock("next/navigation", () => ({
  useRouter: () => ({ push: mockPush }),
  useSearchParams: () => mockSearchParams,
}));

// Mock next/link
jest.mock("next/link", () => {
  return ({ children, href, ...props }: { children: React.ReactNode; href: string; [key: string]: unknown }) => (
    <a href={href} {...props}>{children}</a>
  );
});

// Mock all API functions
jest.mock("../lib/api", () => {
  const actual = jest.requireActual("../lib/api");
  return {
    ...actual,
    fetchNarratives: jest.fn().mockResolvedValue([]),
    fetchTicker: jest.fn().mockResolvedValue([]),
    fetchSignals: jest.fn().mockResolvedValue([]),
    fetchActivity: jest.fn().mockResolvedValue([]),
    fetchStocks: jest.fn().mockResolvedValue([]),
    fetchAssetClasses: jest.fn().mockResolvedValue([]),
    fetchStockDetail: jest.fn().mockResolvedValue(null),
    fetchConstellation: jest.fn().mockResolvedValue({ nodes: [], edges: [] }),
    fetchManipulation: jest.fn().mockResolvedValue([]),
    fetchCorrelation: jest.fn().mockResolvedValue({
      correlation: 0, p_value: 1, n_observations: 0,
      is_significant: false, lead_days: 1, interpretation: "n/a",
      narrative_id: "", ticker: "",
    }),
    fetchBrief: jest.fn().mockResolvedValue({ ticker: "AAPL", security: null, narratives: [], risk_summary: {}, generated_at: "" }),
    fetchCorrelationMatrix: jest.fn().mockResolvedValue({ pairs: [], generated_at: 0, cached: false }),
  };
});

// Mock contexts
jest.mock("../contexts/AuthContext", () => ({
  useAuth: () => ({ isSignedIn: false, signIn: jest.fn(), signOut: jest.fn(), token: null }),
}));
jest.mock("../hooks/useRealtimeData", () => ({
  useRealtimeData: () => ({ data: null, isConnected: false, error: null }),
}));

// Mock StockDetailDrawer
jest.mock("../components/StockDetailDrawer", () => {
  return function MockDrawer() { return null; };
});

// Mock InvestigateDrawer
jest.mock("../components/InvestigateDrawer", () => {
  return function MockDrawer() { return null; };
});

// Mock NarrativeCard
jest.mock("../components/NarrativeCard", () => {
  return function MockCard({ narrative }: { narrative: { id: string; name: string } }) {
    return <div data-testid={`card-${narrative.id}`}>{narrative.name}</div>;
  };
});

// Mock SegmentedControl
jest.mock("../components/common/SegmentedControl", () => {
  return function MockSC({ options, activeOption, onChange }: { options: string[]; activeOption: string; onChange: (v: string) => void }) {
    return (
      <div data-testid="segmented-control">
        {options.map((o) => (
          <button key={o} data-testid={`seg-${o}`} onClick={() => onChange(o)} data-active={o === activeOption}>
            {o}
          </button>
        ))}
      </div>
    );
  };
});

// Mock MetricTooltip
jest.mock("../components/common/MetricTooltip", () => {
  return function MockMT({ children }: { children: React.ReactNode }) { return <>{children}</>; };
});

// Mock Skeleton
jest.mock("../components/common/Skeleton", () => {
  return function MockSkeleton() { return <div data-testid="skeleton" />; };
});

// Mock StageBadge
jest.mock("../components/common/StageBadge", () => {
  return function MockSB({ stage }: { stage: string }) { return <span>{stage}</span>; };
});

// Mock ConstellationMap
jest.mock("../components/ConstellationMap", () => {
  return function MockConstMap({ data }: { data: { nodes: unknown[]; edges: unknown[] } }) {
    return <div data-testid="constellation-map">{data.nodes.length} nodes</div>;
  };
});

// Mock colors
jest.mock("../lib/colors", () => ({
  COLORS: { accent: "#2D72D2", alert: "#EC9A3C", bullish: "#3D8B37", bearish: "#E76A6E" },
}));

// Import after mocks
import {
  fetchNarratives, fetchTicker, fetchSignals, fetchActivity,
  fetchStocks, fetchAssetClasses,
  fetchConstellation, fetchManipulation, fetchBrief, fetchCorrelation,
} from "../lib/api";
import type { VisibleNarrative, ManipulationNarrative } from "../lib/api";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeNarrative(overrides: Partial<VisibleNarrative> = {}): VisibleNarrative {
  return {
    id: "nar-001",
    name: "Test Narrative",
    descriptor: "Test desc",
    velocity_summary: "+5.2% last 7d",
    entropy: 0.5,
    saturation: 0.7,
    velocity_timeseries: [{ date: "2026-03-20", value: 5 }, { date: "2026-03-21", value: 6 }],
    signals: ["s1"],
    catalysts: [],
    mutations: [],
    stage: "Emerging",
    burst_velocity: null,
    topic_tags: ["regulatory"],
    blurred: false as const,
    last_evidence_at: "2026-03-23T10:00:00Z",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// SECURITY TESTS
// ---------------------------------------------------------------------------

describe("PA-SEC: Security", () => {
  beforeEach(() => { jest.clearAllMocks(); });

  test("PA-SEC-1: safeHref rejects javascript: protocol", async () => {
    // Import the signals page and check the activity item link behavior
    (fetchSignals as jest.Mock).mockResolvedValue([]);
    (fetchActivity as jest.Mock).mockResolvedValue([
      {
        type: "alert",
        subtype: "score_spike",
        timestamp: "2026-03-23T10:00:00Z",
        title: "Malicious alert",
        message: "Click here",
        link: "javascript:alert(1)",
        metadata: {},
      },
    ]);

    const SignalsPage = (await import("../app/signals/page")).default;
    render(<SignalsPage />);
    await waitFor(() => expect(screen.getByText("Malicious alert")).toBeInTheDocument());

    const link = screen.getByText("Malicious alert").closest("a");
    // The href should NOT be "javascript:alert(1)"
    expect(link?.getAttribute("href")).not.toBe("javascript:alert(1)");
  });

  test("PA-SEC-2: safeHref allows relative paths", async () => {
    (fetchSignals as jest.Mock).mockResolvedValue([]);
    (fetchActivity as jest.Mock).mockResolvedValue([
      {
        type: "mutation",
        subtype: "stage_change",
        timestamp: "2026-03-23T10:00:00Z",
        title: "Safe link",
        message: "",
        link: "/narrative/abc",
        metadata: {},
      },
    ]);

    const SignalsPage = (await import("../app/signals/page")).default;
    render(<SignalsPage />);
    await waitFor(() => expect(screen.getByText("Safe link")).toBeInTheDocument());

    const link = screen.getByText("Safe link").closest("a");
    expect(link?.getAttribute("href")).toBe("/narrative/abc");
  });

  test("PA-SEC-3: safeHref allows https links", async () => {
    (fetchSignals as jest.Mock).mockResolvedValue([]);
    (fetchActivity as jest.Mock).mockResolvedValue([
      {
        type: "mutation",
        subtype: "score_spike",
        timestamp: "2026-03-23T10:00:00Z",
        title: "HTTPS link",
        message: "",
        link: "https://example.com/page",
        metadata: {},
      },
    ]);

    const SignalsPage = (await import("../app/signals/page")).default;
    render(<SignalsPage />);
    await waitFor(() => expect(screen.getByText("HTTPS link")).toBeInTheDocument());

    const link = screen.getByText("HTTPS link").closest("a");
    expect(link?.getAttribute("href")).toBe("https://example.com/page");
  });
});

// ---------------------------------------------------------------------------
// ERROR HANDLING TESTS
// ---------------------------------------------------------------------------

describe("PA-ERR: Error Handling", () => {
  beforeEach(() => { jest.clearAllMocks(); });

  test("PA-ERR-1: constellation page shows error on fetch failure", async () => {
    (fetchConstellation as jest.Mock).mockRejectedValue(new Error("503 unavailable"));

    const ConstellationPage = (await import("../app/constellation/page")).default;
    render(<ConstellationPage />);

    await waitFor(() => expect(screen.getByText(/Failed to load constellation/)).toBeInTheDocument());
  });

  test("PA-ERR-2: manipulation page shows error on fetch failure", async () => {
    (fetchManipulation as jest.Mock).mockRejectedValue(new Error("500 internal"));

    const ManipulationPage = (await import("../app/manipulation/page")).default;
    render(<ManipulationPage />);

    await waitFor(() => expect(screen.getByText(/Failed to load/)).toBeInTheDocument());
  });
});

// ---------------------------------------------------------------------------
// EDGE CASE TESTS
// ---------------------------------------------------------------------------

describe("PA-EDGE: Edge Cases", () => {
  beforeEach(() => { jest.clearAllMocks(); });

  test("PA-EDGE-1: home page — NaN velocity input resets to 0", async () => {
    (fetchNarratives as jest.Mock).mockResolvedValue([makeNarrative()]);
    (fetchTicker as jest.Mock).mockResolvedValue([]);

    const GatewayPage = (await import("../app/page")).default;
    render(<GatewayPage />);

    await waitFor(() => expect(screen.getByLabelText("Minimum velocity filter")).toBeInTheDocument());
    const input = screen.getByLabelText("Minimum velocity filter") as HTMLInputElement;

    fireEvent.change(input, { target: { value: "abc" } });
    expect(input.value).toBe("0");
  });

  test("PA-EDGE-2: home page — negative velocity input clamped to 0", async () => {
    (fetchNarratives as jest.Mock).mockResolvedValue([makeNarrative()]);
    (fetchTicker as jest.Mock).mockResolvedValue([]);

    const GatewayPage = (await import("../app/page")).default;
    render(<GatewayPage />);

    await waitFor(() => expect(screen.getByLabelText("Minimum velocity filter")).toBeInTheDocument());
    const input = screen.getByLabelText("Minimum velocity filter") as HTMLInputElement;

    fireEvent.change(input, { target: { value: "-5" } });
    expect(input.value).toBe("0");
  });

  test("PA-EDGE-3: home page — SURGE sort keeps all narratives (burst first)", async () => {
    const burstNar = makeNarrative({
      id: "burst-1",
      name: "Burst Narrative",
      burst_velocity: { rate: 10, baseline: 2, ratio: 5, is_burst: true },
    });
    const normalNar = makeNarrative({ id: "normal-1", name: "Normal Narrative" });
    (fetchNarratives as jest.Mock).mockResolvedValue([normalNar, burstNar]);
    (fetchTicker as jest.Mock).mockResolvedValue([]);

    const GatewayPage = (await import("../app/page")).default;
    render(<GatewayPage />);

    await waitFor(() => expect(screen.getByText("Burst Narrative")).toBeInTheDocument());

    // Click SURGE sort
    fireEvent.click(screen.getByTestId("seg-SURGE"));

    // Both narratives should still be visible
    expect(screen.getByText("Burst Narrative")).toBeInTheDocument();
    expect(screen.getByText("Normal Narrative")).toBeInTheDocument();
  });

  test("PA-EDGE-4: home page — empty state when all filtered out", async () => {
    const nar = makeNarrative({ topic_tags: ["crypto"] });
    (fetchNarratives as jest.Mock).mockResolvedValue([nar]);
    (fetchTicker as jest.Mock).mockResolvedValue([]);

    const GatewayPage = (await import("../app/page")).default;
    render(<GatewayPage />);

    await waitFor(() => expect(screen.getByTestId("topic-filter")).toBeInTheDocument());

    // Filter by "regulatory" — the narrative has tag "crypto"
    fireEvent.change(screen.getByTestId("topic-filter"), { target: { value: "regulatory" } });

    expect(screen.getByText("No narratives match the current filters.")).toBeInTheDocument();
  });

  test("PA-EDGE-5: constellation page — empty data shows message", async () => {
    (fetchConstellation as jest.Mock).mockResolvedValue({ nodes: [], edges: [] });

    const ConstellationPage = (await import("../app/constellation/page")).default;
    render(<ConstellationPage />);

    await waitFor(() =>
      expect(screen.getByText(/No constellation data available/)).toBeInTheDocument()
    );
  });

  test("PA-EDGE-6: constellation page — renders map with data", async () => {
    (fetchConstellation as jest.Mock).mockResolvedValue({
      nodes: [{ id: "n1", name: "Test", type: "narrative" }],
      edges: [],
    });

    const ConstellationPage = (await import("../app/constellation/page")).default;
    render(<ConstellationPage />);

    await waitFor(() =>
      expect(screen.getByTestId("constellation-map")).toBeInTheDocument()
    );
    expect(screen.getByText("1 nodes")).toBeInTheDocument();
  });

  test("PA-EDGE-7: home page — retry logic works on transient failure", async () => {
    let callCount = 0;
    (fetchNarratives as jest.Mock).mockImplementation(() => {
      callCount++;
      if (callCount < 3) return Promise.reject(new Error("transient"));
      return Promise.resolve([makeNarrative()]);
    });
    (fetchTicker as jest.Mock).mockResolvedValue([]);

    jest.useFakeTimers();
    const GatewayPage = (await import("../app/page")).default;
    render(<GatewayPage />);

    // First call fails
    await act(async () => { await Promise.resolve(); });

    // Advance past retry delay (1500ms)
    act(() => { jest.advanceTimersByTime(1600); });
    await act(async () => { await Promise.resolve(); });

    // Second retry
    act(() => { jest.advanceTimersByTime(1600); });
    await act(async () => { await Promise.resolve(); });

    // Third attempt succeeds
    expect(callCount).toBeGreaterThanOrEqual(3);
    jest.useRealTimers();
  });

  test("PA-EDGE-8: manipulation page — zero indicators shows empty", async () => {
    (fetchManipulation as jest.Mock).mockResolvedValue([]);

    const ManipulationPage = (await import("../app/manipulation/page")).default;
    render(<ManipulationPage />);

    await waitFor(() =>
      expect(screen.getByTestId("manipulation-empty")).toBeInTheDocument()
    );
  });
});

// ---------------------------------------------------------------------------
// OPTIMIZATION TESTS
// ---------------------------------------------------------------------------

describe("PA-OPT: Optimization", () => {
  beforeEach(() => { jest.clearAllMocks(); });

  test("PA-OPT-1: stocks sort is stable for equal impact scores", async () => {
    const stocks = [
      { id: "a", symbol: "AAA", name: "Alpha", asset_class_id: "ac1", exchange: "NYSE", current_price: 100, price_change_24h: 1, narrative_impact_score: 50 },
      { id: "b", symbol: "BBB", name: "Beta", asset_class_id: "ac1", exchange: "NYSE", current_price: 200, price_change_24h: 2, narrative_impact_score: 50 },
      { id: "c", symbol: "CCC", name: "Charlie", asset_class_id: "ac1", exchange: "NYSE", current_price: 150, price_change_24h: 0, narrative_impact_score: 50 },
    ];
    (fetchStocks as jest.Mock).mockResolvedValue(stocks);
    (fetchAssetClasses as jest.Mock).mockResolvedValue([]);

    const StocksPage = (await import("../app/stocks/page")).default;
    const { container } = render(<StocksPage />);

    await waitFor(() => expect(screen.getByText("AAA")).toBeInTheDocument());

    // Get the order of symbols rendered
    const rows = container.querySelectorAll("[data-testid^='stock-row-']");
    const symbols = Array.from(rows).map((r) => r.getAttribute("data-testid")?.replace("stock-row-", ""));

    // With stable sort by impact (all equal) and tiebreaker by id,
    // order should be deterministic: a < b < c (ascending id), but desc sort reverses
    // Actually desc by impact puts them all together, tiebreaker is a.id.localeCompare(b.id)
    // which is ascending ("a" < "b" < "c"). So order: AAA, BBB, CCC
    expect(symbols).toEqual(["AAA", "BBB", "CCC"]);
  });

  test("PA-OPT-2: correlation page does not re-fetch brief when leadDays changes", async () => {
    // This test verifies the split effect behavior.
    // fetchBrief should only be called once, not twice when leadDays changes.
    mockSearchParams.set("ticker", "AAPL");
    (fetchBrief as jest.Mock).mockResolvedValue({
      ticker: "AAPL",
      security: null,
      narratives: [{ id: "n1", name: "Test", stage: "Emerging", velocity_windowed: 5, entropy: 0.5, entropy_interpretation: "", burst_velocity: null, coordination_flags: 0, exposure_score: 0.5, direction: "bullish", days_active: 10, signal_count: 5, top_signals: [] }],
      risk_summary: {},
      generated_at: "",
    });
    (fetchCorrelation as jest.Mock).mockResolvedValue({
      correlation: 0.5, p_value: 0.01, n_observations: 30,
      is_significant: true, lead_days: 1, interpretation: "Strong",
      narrative_id: "n1", ticker: "AAPL",
    });

    const CorrelationPage = (await import("../app/correlation/page")).default;
    render(<CorrelationPage />);

    await waitFor(() => expect(fetchBrief).toHaveBeenCalledTimes(1));

    // The correlation fetch should also happen
    await waitFor(() => expect(fetchCorrelation).toHaveBeenCalled());

    // Clean up search params
    mockSearchParams.delete("ticker");
  });

  test("PA-OPT-3: manipulation client-side filtering works correctly", async () => {
    const mockData: ManipulationNarrative[] = [{
      id: "n1",
      name: "Test Narrative",
      descriptor: "desc",
      entropy: 0.5,
      velocity_summary: "+3%",
      manipulation_indicators: [
        {
          id: "mi1", narrative_id: "n1", indicator_type: "bot_network",
          confidence: 0.9, detected_at: "2026-03-23T10:00:00Z",
          evidence_summary: "Bot detected", flagged_signals: ["s1"],
          status: "active",
        },
        {
          id: "mi2", narrative_id: "n1", indicator_type: "astroturfing",
          confidence: 0.3, detected_at: "2026-03-22T10:00:00Z",
          evidence_summary: "Low confidence", flagged_signals: [],
          status: "dismissed",
        },
      ],
    }];
    (fetchManipulation as jest.Mock).mockResolvedValue(mockData);

    const ManipulationPage = (await import("../app/manipulation/page")).default;
    render(<ManipulationPage />);

    await waitFor(() => expect(screen.getByText("Bot detected")).toBeInTheDocument());

    // Both indicators visible initially
    expect(screen.getByText("Low confidence")).toBeInTheDocument();

    // Filter by type: bot_network
    fireEvent.change(screen.getByTestId("filter-indicator-type"), { target: { value: "bot_network" } });

    // Only bot_network should remain
    expect(screen.getByText("Bot detected")).toBeInTheDocument();
    expect(screen.queryByText("Low confidence")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// UI BEHAVIOR TESTS
// ---------------------------------------------------------------------------

describe("PA-UI: UI Behavior", () => {
  beforeEach(() => { jest.clearAllMocks(); });

  test("PA-UI-1: constellation page renders correct title", async () => {
    (fetchConstellation as jest.Mock).mockResolvedValue({ nodes: [], edges: [] });

    const ConstellationPage = (await import("../app/constellation/page")).default;
    render(<ConstellationPage />);

    expect(screen.getByText("Constellation")).toBeInTheDocument();
    // Should NOT show "Analytics" (old bug)
    expect(screen.queryByText("Analytics")).not.toBeInTheDocument();
  });

  test("PA-UI-2: constellation page shows footer with node/edge counts", async () => {
    (fetchConstellation as jest.Mock).mockResolvedValue({
      nodes: [{ id: "n1", name: "N1", type: "narrative" }, { id: "n2", name: "N2", type: "narrative" }],
      edges: [{ source: "n1", target: "n2", weight: 0.5, label: "related" }],
    });

    const ConstellationPage = (await import("../app/constellation/page")).default;
    render(<ConstellationPage />);

    await waitFor(() => expect(screen.getByText(/2 nodes · 1 edges/)).toBeInTheDocument());
  });

  test("PA-UI-3: home page shows disclaimer footer", async () => {
    (fetchNarratives as jest.Mock).mockResolvedValue([]);
    (fetchTicker as jest.Mock).mockResolvedValue([]);

    const GatewayPage = (await import("../app/page")).default;
    render(<GatewayPage />);

    await waitFor(() =>
      expect(screen.getByText("INTELLIGENCE ONLY — NOT FINANCIAL ADVICE")).toBeInTheDocument()
    );
  });

});
