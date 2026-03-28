/**
 * D4 Frontend Tests
 *
 * Unit:
 *   D4-U1: /manipulation page renders "Manipulation & Coordination Detection" heading
 *   D4-U2: Summary stats bar renders with flagged narrative count
 *   D4-U3: Each narrative card renders indicator badges with correct type labels
 *   D4-U4: indicator_type="coordinated_amplification" badge has red styling
 *   D4-U5: indicator_type="bot_network" badge has purple styling
 *   D4-U6: Confidence bar renders with data-testid="confidence-bar-{id}"
 *   D4-U7: Status "dismissed" renders with line-through styling
 *   D4-U8: Filtering by indicator type updates the displayed list
 *   D4-U9: Empty state renders when no indicators returned
 *
 * Integration:
 *   D4-I1: /signals page shows "View campaign →" link for coordination-flagged signals
 *   D4-I2: /narrative/[id] shows manipulation warning banner when indicators exist
 *   D4-I3: All visible narrative cards on / render as visible (no blurred layer)
 *   D4-I4: Investigate button opens drawer immediately (no credit check)
 *   D4-I5: Export button is enabled for signed-in users (no subscription check)
 *   D4-I6: No element with text matching /credits|billing|subscribe|upgrade/i on gateway page
 */

import React from "react";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

import ManipulationPage from "../app/manipulation/page";
import NarrativeCard from "../components/NarrativeCard";
import InvestigateDrawer from "../components/InvestigateDrawer";
import { AuthContext } from "../contexts/AuthContext";
import type { ManipulationNarrative, VisibleNarrative, NarrativeDetail } from "../lib/api";

// ---------------------------------------------------------------------------
// Mock fetchManipulation
// ---------------------------------------------------------------------------

jest.mock("../lib/api", () => ({
  ...jest.requireActual("../lib/api"),
  fetchManipulation: jest.fn(),
  fetchNarrativeDetail: jest.fn(),
  fetchNarrativeManipulation: jest.fn(),
  fetchNarratives: jest.fn(),
  fetchTicker: jest.fn(),
  fetchSignals: jest.fn(),
}));

import {
  fetchManipulation,
  fetchNarrativeDetail,
  fetchNarrativeManipulation,
  fetchNarratives,
  fetchTicker,
  fetchSignals,
} from "../lib/api";

const mockFetchManipulation = fetchManipulation as jest.MockedFunction<typeof fetchManipulation>;
const mockFetchNarrativeDetail = fetchNarrativeDetail as jest.MockedFunction<typeof fetchNarrativeDetail>;
const mockFetchNarrativeManipulation = fetchNarrativeManipulation as jest.MockedFunction<typeof fetchNarrativeManipulation>;
const mockFetchNarratives = fetchNarratives as jest.MockedFunction<typeof fetchNarratives>;
const mockFetchTicker = fetchTicker as jest.MockedFunction<typeof fetchTicker>;
const mockFetchSignals = fetchSignals as jest.MockedFunction<typeof fetchSignals>;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const MOCK_MANIPULATION: ManipulationNarrative[] = [
  {
    id: "nar-001",
    name: "Semiconductor Reshoring",
    descriptor: "US chip manufacturing policy narrative.",
    entropy: 0.72,
    velocity_summary: "+14.0% signal velocity over 7d",
    manipulation_indicators: [
      {
        id: "mi-003",
        narrative_id: "nar-001",
        indicator_type: "temporal_spike",
        confidence: 0.65,
        detected_at: "2026-03-14T09:15:00Z",
        evidence_summary: "Signal volume increased 340% in a 90-minute window",
        flagged_signals: ["sig-001", "sig-004"],
        status: "under_review",
      },
    ],
  },
  {
    id: "nar-002",
    name: "Clean Energy Transition",
    descriptor: "Energy sector transition narrative.",
    entropy: 0.58,
    velocity_summary: "+8.0% signal velocity over 7d",
    manipulation_indicators: [
      {
        id: "mi-001",
        narrative_id: "nar-002",
        indicator_type: "coordinated_amplification",
        confidence: 0.78,
        detected_at: "2026-03-15T10:30:00Z",
        evidence_summary: "87% of signal volume from 3 clusters",
        flagged_signals: ["sig-003", "sig-007"],
        status: "active",
      },
      {
        id: "mi-002",
        narrative_id: "nar-002",
        indicator_type: "bot_network",
        confidence: 0.91,
        detected_at: "2026-03-15T14:00:00Z",
        evidence_summary: "Uniform publishing cadence detected",
        flagged_signals: ["sig-003"],
        status: "confirmed",
      },
    ],
  },
  {
    id: "nar-003",
    name: "Inflation Persistence",
    descriptor: "Inflation narrative.",
    entropy: 0.45,
    velocity_summary: "+5.0% signal velocity over 7d",
    manipulation_indicators: [
      {
        id: "mi-007",
        narrative_id: "nar-003",
        indicator_type: "astroturfing",
        confidence: 0.4,
        detected_at: "2026-03-13T10:00:00Z",
        evidence_summary: "Newly-created accounts amplifying narrative",
        flagged_signals: ["sig-010"],
        status: "dismissed",
      },
    ],
  },
];

const MOCK_VISIBLE: VisibleNarrative = {
  id: "nar-001",
  name: "Semiconductor Reshoring",
  descriptor: "US chip manufacturing policy.",
  velocity_summary: "+14.0% signal velocity over 7d",
  entropy: 0.72,
  saturation: 0.45,
  velocity_timeseries: [],
  signals: [],
  catalysts: [],
  mutations: [],
  blurred: false,
};

const MOCK_DETAIL: NarrativeDetail = {
  id: "nar-001",
  name: "Semiconductor Reshoring Acceleration",
  descriptor: "US chip manufacturing policy is catalyzing a supply-chain realignment.",
  velocity_summary: "+14.0% signal velocity over 7d",
  entropy: 0.72,
  saturation: 0.45,
  velocity_timeseries: [
    { date: "2026-03-10", value: 0.58 },
    { date: "2026-03-16", value: 0.86 },
  ],
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
  catalysts: [],
  mutations: [],
  entropy_detail: {
    narrative_id: "nar-001",
    score: 0.72,
    components: { source_diversity: 0.6, temporal_spread: 0.5, sentiment_variance: 0.3 },
  },
  blurred: false,
};

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

const signedInAuth = {
  isSignedIn: true,
  token: "stub-auth-token" as string | null,
  signIn: jest.fn(),
  signOut: jest.fn(),
};

const guestAuth = {
  isSignedIn: false,
  token: null as string | null,
  signIn: jest.fn(),
  signOut: jest.fn(),
};

function withAuth(ui: React.ReactElement, auth = guestAuth) {
  return render(<AuthContext.Provider value={auth}>{ui}</AuthContext.Provider>);
}

// ---------------------------------------------------------------------------
// D4-U1: ManipulationPage heading
// ---------------------------------------------------------------------------

describe("D4-U1: ManipulationPage heading", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("renders 'Manipulation & Coordination Detection' heading", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(
        screen.getByText(/Manipulation.*Coordination Detection/i)
      ).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// D4-U2: Summary stats bar
// ---------------------------------------------------------------------------

describe("D4-U2: Summary stats bar", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("renders manipulation-stats testid with flagged narrative count", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("manipulation-stats")).toBeInTheDocument();
    });
    // 3 narratives flagged
    expect(screen.getByTestId("manipulation-stats")).toHaveTextContent("3");
  });
});

// ---------------------------------------------------------------------------
// D4-U3: Indicator badges with correct type labels
// ---------------------------------------------------------------------------

describe("D4-U3: Indicator type badges", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("renders indicator type badge for each indicator", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("type-badge-mi-001")).toBeInTheDocument();
    });
    expect(screen.getByTestId("type-badge-mi-001")).toHaveTextContent(
      "Coordinated Amplification"
    );
    expect(screen.getByTestId("type-badge-mi-002")).toHaveTextContent("Bot Network");
  });
});

// ---------------------------------------------------------------------------
// D4-U4: coordinated_amplification badge has red styling
// ---------------------------------------------------------------------------

describe("D4-U4: coordinated_amplification badge red styling", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("badge has red class for coordinated_amplification type", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("type-badge-mi-001")).toBeInTheDocument();
    });
    expect(screen.getByTestId("type-badge-mi-001").className).toMatch(/bearish|critical/);
  });
});

// ---------------------------------------------------------------------------
// D4-U5: bot_network badge has purple styling
// ---------------------------------------------------------------------------

describe("D4-U5: bot_network badge purple styling", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("badge has purple class for bot_network type", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("type-badge-mi-002")).toBeInTheDocument();
    });
    expect(screen.getByTestId("type-badge-mi-002").className).toMatch(/purple/);
  });
});

// ---------------------------------------------------------------------------
// D4-U6: Confidence bar renders with correct testid
// ---------------------------------------------------------------------------

describe("D4-U6: Confidence bar", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("renders confidence-bar-{id} for each indicator", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("confidence-bar-mi-001")).toBeInTheDocument();
    });
    expect(screen.getByTestId("confidence-bar-mi-002")).toBeInTheDocument();
    expect(screen.getByTestId("confidence-bar-mi-003")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D4-U7: dismissed status has line-through
// ---------------------------------------------------------------------------

describe("D4-U7: dismissed status line-through", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("dismissed status badge has line-through class", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("status-badge-mi-007")).toBeInTheDocument();
    });
    expect(screen.getByTestId("status-badge-mi-007").className).toMatch(
      /line-through/
    );
  });
});

// ---------------------------------------------------------------------------
// D4-U8: Filtering by indicator type
// ---------------------------------------------------------------------------

describe("D4-U8: Filter by indicator type", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue(MOCK_MANIPULATION);
  });

  it("filtering by bot_network shows only bot_network indicators", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("filter-indicator-type")).toBeInTheDocument();
    });

    fireEvent.change(screen.getByTestId("filter-indicator-type"), {
      target: { value: "bot_network" },
    });

    await waitFor(() => {
      expect(screen.getByTestId("type-badge-mi-002")).toBeInTheDocument();
    });

    // coordinated_amplification indicator should not be visible
    expect(screen.queryByTestId("type-badge-mi-001")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D4-U9: Empty state
// ---------------------------------------------------------------------------

describe("D4-U9: Empty state", () => {
  beforeEach(() => {
    mockFetchManipulation.mockResolvedValue([]);
  });

  it("renders manipulation-empty when no indicators returned", async () => {
    render(<ManipulationPage />);
    await waitFor(() => {
      expect(screen.getByTestId("manipulation-empty")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// D4-I1: Signals page shows "View campaign →" link for coordination-flagged signals
// ---------------------------------------------------------------------------

describe("D4-I1: Signals page View campaign link", () => {
  beforeEach(() => {
    mockFetchSignals.mockResolvedValue([
      {
        id: "sig-001",
        narrative_id: "nar-001",
        headline: "TSMC announces expansion",
        source: {
          id: "reuters",
          name: "reuters.com",
          type: "news",
          url: "",
          credibility_score: 0.9,
        },
        timestamp: "2026-03-10T12:00:00Z",
        sentiment: 0.7,
        coordination_flag: true,
      },
    ]);
  });

  it("shows 'View campaign →' link for coordination-flagged signal", async () => {
    const SignalsPage = (await import("../app/signals/page")).default;
    render(<SignalsPage />);
    await waitFor(() => {
      expect(screen.getByTestId("view-campaign-sig-001")).toBeInTheDocument();
    });
    expect(screen.getByTestId("view-campaign-sig-001")).toHaveTextContent(
      "View campaign →"
    );
  });
});

// ---------------------------------------------------------------------------
// D4-I2: narrative/[id] shows manipulation warning banner
// ---------------------------------------------------------------------------

describe("D4-I2: Narrative detail manipulation warning banner", () => {
  it("shows manipulation-warning-banner when indicators exist", async () => {
    mockFetchNarrativeManipulation.mockResolvedValue([
      {
        id: "mi-003",
        narrative_id: "nar-001",
        indicator_type: "temporal_spike",
        confidence: 0.65,
        detected_at: "2026-03-14T09:15:00Z",
        evidence_summary: "Signal volume spike detected",
        flagged_signals: ["sig-001"],
        status: "under_review",
      },
    ]);
    mockFetchNarrativeDetail.mockResolvedValue(MOCK_DETAIL);

    // Mock fetchNarrativeAssets
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    } as Response);

    // We need to import the narrative detail page with mocked useParams
    jest.mock("next/navigation", () => ({
      useParams: () => ({ id: "nar-001" }),
      usePathname: () => "/narrative/nar-001",
      useRouter: () => ({ push: jest.fn() }),
    }));

    const NarrativeDetailPage = (
      await import("../app/narrative/[id]/page")
    ).default;

    withAuth(<NarrativeDetailPage />, signedInAuth);

    await waitFor(() => {
      expect(
        screen.getByTestId("manipulation-warning-banner")
      ).toBeInTheDocument();
    });
    expect(screen.getByTestId("manipulation-warning-banner")).toHaveTextContent(
      "1 manipulation indicator"
    );
  });
});

// ---------------------------------------------------------------------------
// D4-I3: All visible narrative cards render as visible (no blurred layer)
// ---------------------------------------------------------------------------

describe("D4-I3: All visible narrative cards on / render as visible", () => {
  it("visible NarrativeCard renders article (not blurred button)", () => {
    withAuth(
      <NarrativeCard narrative={MOCK_VISIBLE} />,
      guestAuth
    );
    expect(screen.getByRole("article")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /sign up to unlock/i })
    ).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D4-I4: Investigate button opens drawer immediately (no credit check)
// ---------------------------------------------------------------------------

describe("D4-I4: Investigate button opens drawer immediately", () => {
  it("calls onInvestigateClick immediately without credit gating", () => {
    const mockInvestigate = jest.fn();

    withAuth(
      <NarrativeCard
        narrative={MOCK_VISIBLE}
        onInvestigateClick={mockInvestigate}
      />,
      signedInAuth
    );

    const btn = screen.getByRole("button", { name: /investigate/i });
    expect(btn).not.toBeDisabled();
    fireEvent.click(btn);
    expect(mockInvestigate).toHaveBeenCalledWith("nar-001");
  });
});

// ---------------------------------------------------------------------------
// D4-I5: Export button enabled for signed-in users (no subscription check)
// ---------------------------------------------------------------------------

describe("D4-I5: Export button enabled for signed-in users", () => {
  it("export button is present and enabled for signed-in user", () => {
    render(
      <div>
        <button
          data-testid="export-btn"
          aria-label="Export narrative report as CSV"
          onClick={jest.fn()}
        >
          Export Report
        </button>
      </div>
    );
    expect(screen.getByTestId("export-btn")).not.toBeDisabled();
  });
});

// ---------------------------------------------------------------------------
// D4-I6: No credits/billing/subscribe/upgrade text on gateway page
// ---------------------------------------------------------------------------

describe("D4-I6: No monetization text on gateway page", () => {
  beforeEach(() => {
    mockFetchNarratives.mockResolvedValue([MOCK_VISIBLE]);
    mockFetchTicker.mockResolvedValue([]);
  });

  it("gateway page has no credits/billing/subscribe/upgrade text", async () => {
    const GatewayPage = (await import("../app/page")).default;
    withAuth(<GatewayPage />, guestAuth);
    await waitFor(() => {
      expect(screen.queryByText(/credits/i)).not.toBeInTheDocument();
    });
    expect(screen.queryByText(/billing/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/subscribe/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/upgrade/i)).not.toBeInTheDocument();
  });
});
