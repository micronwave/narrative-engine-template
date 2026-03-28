/**
 * F3 Frontend Tests — Intelligence Brief Pages
 *
 * F3-U1: /brief/TSM page renders "TSM Intelligence Brief" heading
 * F3-U2: Risk summary panel renders with data-testid="brief-risk-summary"
 * F3-U3: Narrative cards render with stage badge and entropy interpretation
 * F3-U4: /brief index page renders list of tracked securities
 */

import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";

jest.mock("next/navigation", () => ({
  useParams: () => ({ ticker: "TSM" }),
  usePathname: () => "/brief/TSM",
  useRouter: () => ({ push: jest.fn() }),
}));

jest.mock("../lib/api", () => ({
  ...jest.requireActual("../lib/api"),
  fetchBrief: jest.fn(),
  fetchSecurities: jest.fn(),
}));

import { fetchBrief, fetchSecurities } from "../lib/api";

const mockFetchBrief = fetchBrief as jest.MockedFunction<typeof fetchBrief>;
const mockFetchSecurities = fetchSecurities as jest.MockedFunction<typeof fetchSecurities>;

const MOCK_BRIEF = {
  ticker: "TSM",
  security: {
    id: "ts-001",
    symbol: "TSM",
    name: "Taiwan Semiconductor Manufacturing",
    asset_class_id: "ac-001",
    exchange: "NYSE",
    current_price: 142.35,
    price_change_24h: 1.23,
    narrative_impact_score: 85,
  },
  narratives: [
    {
      id: "nar-001",
      name: "Semiconductor Reshoring",
      stage: "Growing",
      velocity_windowed: 0.14,
      entropy: 1.82,
      entropy_interpretation: "Multi-source coverage — diverse perspectives",
      burst_velocity: null,
      coordination_flags: 0,
      exposure_score: 0.85,
      direction: "bullish",
      days_active: 12,
      signal_count: 23,
      top_signals: [
        { headline: "TSMC announces Arizona expansion", source: "reuters.com", timestamp: "2026-03-15T12:00:00Z" },
      ],
    },
  ],
  risk_summary: {
    coordination_detected: false,
    highest_burst_ratio: 0,
    dominant_direction: "bullish",
    narrative_count: 1,
    avg_entropy: 1.82,
    entropy_assessment: "Multi-source coverage — diverse perspectives",
  },
  generated_at: "2026-03-17T12:00:00Z",
};

const MOCK_SECURITIES = [
  {
    id: "ts-001",
    symbol: "TSM",
    name: "Taiwan Semiconductor Manufacturing",
    asset_class_id: "ac-001",
    exchange: "NYSE",
    current_price: 142.35,
    price_change_24h: 1.23,
    narrative_impact_score: 85,
  },
  {
    id: "ts-002",
    symbol: "NVDA",
    name: "NVIDIA Corporation",
    asset_class_id: "ac-001",
    exchange: "NASDAQ",
    current_price: 875.50,
    price_change_24h: -12.40,
    narrative_impact_score: 90,
  },
];

// ---------------------------------------------------------------------------
// F3-U1: Brief page renders heading
// ---------------------------------------------------------------------------

describe("F3-U1: /brief/TSM renders heading", () => {
  it("renders the ticker intelligence brief heading", async () => {
    mockFetchBrief.mockResolvedValue(MOCK_BRIEF as any);

    const BriefPage = (await import("../app/brief/[ticker]/page")).default;
    render(<BriefPage />);

    await waitFor(() => {
      expect(screen.getByText("TSM Intelligence Brief")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// F3-U2: Risk summary panel renders
// ---------------------------------------------------------------------------

describe("F3-U2: Risk summary panel", () => {
  it("renders with data-testid='brief-risk-summary'", async () => {
    mockFetchBrief.mockResolvedValue(MOCK_BRIEF as any);

    const BriefPage = (await import("../app/brief/[ticker]/page")).default;
    render(<BriefPage />);

    await waitFor(() => {
      expect(screen.getByTestId("brief-risk-summary")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// F3-U3: Narrative cards render with stage badge and entropy interpretation
// ---------------------------------------------------------------------------

describe("F3-U3: Narrative cards in brief", () => {
  it("renders entropy interpretation text", async () => {
    mockFetchBrief.mockResolvedValue(MOCK_BRIEF as any);

    const BriefPage = (await import("../app/brief/[ticker]/page")).default;
    render(<BriefPage />);

    await waitFor(() => {
      const matches = screen.getAllByText("Multi-source coverage — diverse perspectives");
      expect(matches.length).toBeGreaterThanOrEqual(1);
    });
  });

  it("renders stage badge", async () => {
    mockFetchBrief.mockResolvedValue(MOCK_BRIEF as any);

    const BriefPage = (await import("../app/brief/[ticker]/page")).default;
    render(<BriefPage />);

    await waitFor(() => {
      expect(screen.getByText("growing")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// F3-U4: /brief index page renders securities
// ---------------------------------------------------------------------------

describe("F3-U4: Briefs index page", () => {
  it("renders list of tracked securities", async () => {
    mockFetchSecurities.mockResolvedValue(MOCK_SECURITIES as any);

    const BriefsIndex = (await import("../app/brief/page")).default;
    render(<BriefsIndex />);

    await waitFor(() => {
      expect(screen.getByText("Intelligence Briefs")).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(screen.getByTestId("brief-link-TSM")).toBeInTheDocument();
      expect(screen.getByTestId("brief-link-NVDA")).toBeInTheDocument();
    });
  });
});
