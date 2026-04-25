/**
 * C3 Frontend Tests
 *
 * Unit:
 *   C3-U1: NarrativeCard visible — renders name, descriptor, velocity_summary; aria-label present
 *   C3-U2: NarrativeCard blurred — renders neutral placeholder with no paywall copy
 *   C3-U3: NarrativeCard — switches visible/blurred based on prop
 *   C3-U4: VelocitySparkline — 7 points → SVG polyline; last>first = green stroke
 *   C3-U5: SaturationMeter — saturation 0.45 → ~45% width; saturation 0.8 → red color
 *   C3-U6: InvestigateDrawer — opens with narrativeId; renders name + evidence
 *
 * Integration:
 *   C3-I1: Ticker real-time updates — fake timers advance 11s; hook data updates from polling
 *   C3-I2: InvestigateDrawer fetch — narrativeId="nar-001" → fetch called with /api/narratives/nar-001
 */

import React from "react";
import {
  render,
  screen,
  act,
  waitFor,
} from "@testing-library/react";
import "@testing-library/jest-dom";
import { renderHook } from "@testing-library/react";

import NarrativeCard from "../components/NarrativeCard";
import VelocitySparkline from "../components/VelocitySparkline";
import SaturationMeter from "../components/SaturationMeter";
import InvestigateDrawer from "../components/InvestigateDrawer";
import { useRealtimeData } from "../hooks/useRealtimeData";
import { AuthContext } from "../contexts/AuthContext";
import type { VisibleNarrative, NarrativeDetail } from "../lib/api";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const MOCK_TIMESERIES = [
  { date: "2026-03-10", value: 0.58 },
  { date: "2026-03-11", value: 0.62 },
  { date: "2026-03-12", value: 0.67 },
  { date: "2026-03-13", value: 0.71 },
  { date: "2026-03-14", value: 0.74 },
  { date: "2026-03-15", value: 0.80 },
  { date: "2026-03-16", value: 0.86 },
];

const MOCK_VISIBLE: VisibleNarrative = {
  id: "nar-001",
  name: "Semiconductor Reshoring Acceleration",
  descriptor: "US chip manufacturing policy is catalyzing a supply-chain realignment.",
  velocity_summary: "+14.0% signal velocity over 7d",
  entropy: 0.72,
  saturation: 0.45,
  velocity_timeseries: MOCK_TIMESERIES,
  signals: ["sig-001", "sig-002"],
  catalysts: ["cat-001"],
  mutations: ["mut-001"],
  blurred: false,
};

const MOCK_DETAIL: NarrativeDetail = {
  id: "nar-001",
  name: "Semiconductor Reshoring Acceleration",
  descriptor: "US chip manufacturing policy is catalyzing a supply-chain realignment.",
  velocity_summary: "+14.0% signal velocity over 7d",
  entropy: 0.72,
  saturation: 0.45,
  velocity_timeseries: MOCK_TIMESERIES,
  signals: [
    {
      id: "sig-001",
      narrative_id: "nar-001",
      headline: "TSMC announces Arizona expansion ahead of schedule",
      source: {
        id: "reuters-com",
        name: "reuters.com",
        type: "news",
        url: "https://reuters.com",
        credibility_score: 0.92,
      },
      timestamp: "2026-03-10T12:00:00Z",
      sentiment: 0.7,
      coordination_flag: false,
    },
  ],
  catalysts: [
    {
      id: "cat-001",
      narrative_id: "nar-001",
      description: "Stage change: emerging → accelerating",
      timestamp: "2026-03-12T08:00:00Z",
      impact_score: 0.85,
    },
  ],
  mutations: [],
  entropy_detail: {
    narrative_id: "nar-001",
    score: 0.72,
    components: {
      source_diversity: 0.6,
      temporal_spread: 0.5,
      sentiment_variance: 0.3,
    },
  },
  blurred: false,
};

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

const guestAuth = {
  isSignedIn: false,
  token: null,
  signIn: jest.fn(),
  signOut: jest.fn(),
};

const signedInAuth = {
  isSignedIn: true,
  token: "stub-auth-token",
  signIn: jest.fn(),
  signOut: jest.fn(),
};

function renderWithContexts(
  ui: React.ReactElement,
  auth = guestAuth as typeof guestAuth | typeof signedInAuth
) {
  return render(
    <AuthContext.Provider value={auth}>{ui}</AuthContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// C3-U1: NarrativeCard visible renders required fields
// ---------------------------------------------------------------------------

describe("C3-U1: NarrativeCard visible fields", () => {
  it("renders name, descriptor, velocity_summary, and aria-label", () => {
    renderWithContexts(<NarrativeCard narrative={MOCK_VISIBLE} />, guestAuth);

    expect(screen.getByText("Semiconductor Reshoring Acceleration")).toBeInTheDocument();
    expect(
      screen.getByText(/US chip manufacturing policy/i)
    ).toBeInTheDocument();
    expect(screen.getByText("+14.0% signal velocity over 7d")).toBeInTheDocument();

    const article = screen.getByRole("article");
    expect(article).toHaveAttribute(
      "aria-label",
      "Narrative: Semiconductor Reshoring Acceleration"
    );
  });
});

// ---------------------------------------------------------------------------
// C3-U2: NarrativeCard blurred renders neutral placeholder
// ---------------------------------------------------------------------------

describe("C3-U2: NarrativeCard blurred overlay", () => {
  it("renders a neutral placeholder without paywall copy", () => {
    renderWithContexts(
      <NarrativeCard
        narrative={{ id: "nar-002", blurred: true }}
      />,
      guestAuth
    );

    expect(screen.getByTestId("blurred-card")).toBeInTheDocument();
    expect(
      screen.queryByText(/sign up to unlock/i)
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /unlock/i })
    ).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// C3-U3: NarrativeCard switches between visible and blurred
// ---------------------------------------------------------------------------

describe("C3-U3: NarrativeCard visible/blurred switch", () => {
  it("renders visible card when blurred=false", () => {
    renderWithContexts(<NarrativeCard narrative={MOCK_VISIBLE} />, guestAuth);
    expect(screen.getByRole("article")).toBeInTheDocument();
    expect(screen.queryByText(/sign up to unlock/i)).not.toBeInTheDocument();
  });

  it("renders blurred card when blurred=true", () => {
    renderWithContexts(
      <NarrativeCard narrative={{ id: "nar-002", blurred: true }} />,
      guestAuth
    );
    expect(screen.getByTestId("blurred-card")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /unlock/i })).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// C3-U4: VelocitySparkline — SVG polyline, green when last > first
// ---------------------------------------------------------------------------

describe("C3-U4: VelocitySparkline SVG", () => {
  it("renders an SVG path for 7-point timeseries with green stroke when trending up", () => {
    render(<VelocitySparkline timeseries={MOCK_TIMESERIES} />);

    const wrapper = screen.getByTestId("velocity-sparkline");
    expect(wrapper).toBeInTheDocument();

    const svg = wrapper.querySelector("svg");
    expect(svg).toBeInTheDocument();

    // Smooth bezier path (was polyline)
    const paths = svg!.querySelectorAll("path");
    const linePath = Array.from(paths).find((p) => p.getAttribute("stroke") === "#32A467");
    expect(linePath).toBeTruthy();
    expect(linePath!.getAttribute("stroke")).toBe("#32A467");
  });

  it("renders red stroke when last value < first (downward trend)", () => {
    const downtrend = [
      { date: "2026-03-10", value: 0.90 },
      { date: "2026-03-11", value: 0.80 },
      { date: "2026-03-12", value: 0.70 },
      { date: "2026-03-13", value: 0.60 },
      { date: "2026-03-14", value: 0.55 },
      { date: "2026-03-15", value: 0.50 },
      { date: "2026-03-16", value: 0.45 },
    ];
    render(<VelocitySparkline timeseries={downtrend} />);
    const svg = screen.getByTestId("velocity-sparkline").querySelector("svg");
    const linePath = Array.from(svg!.querySelectorAll("path")).find(
      (p) => p.getAttribute("stroke") === "#E76A6E"
    );
    expect(linePath).toBeTruthy();
    expect(linePath!.getAttribute("stroke")).toBe("#E76A6E");
  });

  it("carries data-timeseries attribute with full timeseries array", () => {
    render(<VelocitySparkline timeseries={MOCK_TIMESERIES} />);
    const wrapper = screen.getByTestId("velocity-sparkline");
    const tsData = wrapper.getAttribute("data-timeseries");
    expect(tsData).toBeTruthy();
    const parsed = JSON.parse(tsData!);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed).toHaveLength(7);
    expect(parsed[0]).toHaveProperty("date");
    expect(parsed[0]).toHaveProperty("value");
  });
});

// ---------------------------------------------------------------------------
// C3-U5: SaturationMeter — width and color
// ---------------------------------------------------------------------------

describe("C3-U5: SaturationMeter color and width", () => {
  it("saturation 0.45 → fill width ~45% and amber color", () => {
    render(<SaturationMeter saturation={0.45} />);
    const fill = screen.getByTestId("saturation-fill");
    expect(fill).toHaveStyle({ width: "45%", backgroundColor: "#EC9A3C" });
  });

  it("saturation 0.8 → fill width 80% and red color", () => {
    render(<SaturationMeter saturation={0.8} />);
    const fill = screen.getByTestId("saturation-fill");
    expect(fill).toHaveStyle({ width: "80%", backgroundColor: "#E76A6E" });
  });

  it("saturation 0.2 → blue color", () => {
    render(<SaturationMeter saturation={0.2} />);
    const fill = screen.getByTestId("saturation-fill");
    expect(fill).toHaveStyle({ backgroundColor: "#2D72D2" });
  });

  it("renders percentage label when label=true", () => {
    render(<SaturationMeter saturation={0.45} label />);
    expect(screen.getByText("45%")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// C3-U6: InvestigateDrawer — opens with narrativeId, renders data
// ---------------------------------------------------------------------------

describe("C3-U6: InvestigateDrawer renders narrative data", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("renders narrative name and evidence headline on open", async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => MOCK_DETAIL,
    } as Response);

    renderWithContexts(
      <InvestigateDrawer narrativeId="nar-001" onClose={jest.fn()} />,
      signedInAuth
    );

    // Narrative name and evidence headline appear after fetch resolves
    await waitFor(() => {
      expect(
        screen.getByText("Semiconductor Reshoring Acceleration")
      ).toBeInTheDocument();
    });

    expect(
      screen.getByText("TSMC announces Arizona expansion ahead of schedule")
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// C3-I1: useRealtimeData hook — polling updates data after interval
// ---------------------------------------------------------------------------

describe("C3-I1: useRealtimeData polling", () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it("fetches immediately and re-fetches after interval elapses", async () => {
    const tickerData = [{ name: "Signal A", velocity_summary: "+5.0% signal velocity over 7d" }];
    const updatedData = [{ name: "Signal A (updated)", velocity_summary: "+6.0% signal velocity over 7d" }];

    const mockFetch = jest.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => tickerData } as Response)
      .mockResolvedValueOnce({ ok: true, json: async () => updatedData } as Response);
    global.fetch = mockFetch;

    const { result } = renderHook(() =>
      useRealtimeData<typeof tickerData>({
        endpoint: "/api/ticker",
        interval: 10000,
      })
    );

    // Initial fetch fires immediately
    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.data).toEqual(tickerData);
    expect(result.current.isConnected).toBe(true);

    // Advance timer by 11 seconds → second poll fires
    await act(async () => {
      jest.advanceTimersByTime(11000);
      await Promise.resolve();
    });

    expect(result.current.data).toEqual(updatedData);
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });
});

// ---------------------------------------------------------------------------
// C3-I2: InvestigateDrawer fetch — calls /api/narratives/{id}
// ---------------------------------------------------------------------------

describe("C3-I2: InvestigateDrawer fetches correct URL", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("fetches /api/narratives/nar-001 when narrativeId=nar-001", async () => {
    const mockFetch = (global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => MOCK_DETAIL,
    } as Response) as jest.Mock);

    renderWithContexts(
      <InvestigateDrawer narrativeId="nar-001" onClose={jest.fn()} />,
      signedInAuth
    );

    await waitFor(() => {
      // The fetch for narrative detail should be called with the right URL
      const calls = mockFetch.mock.calls.map((c) => c[0] as string);
      expect(calls.some((url) => url.includes("/api/narratives/nar-001"))).toBe(true);
    });
  });
});
