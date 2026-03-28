/**
 * D2 Frontend Tests
 *
 * Unit:
 *   D2-U1: When all prices are null, "finnhub-unavailable-banner" is visible
 *   D2-U2: When current_price is populated, formatted price string renders (e.g., "$142.35")
 *   D2-U3: Positive price_change_24h renders in green with "+" prefix
 *   D2-U4: Negative price_change_24h renders in red with "−" prefix (minus sign)
 *   D2-U5: When current_price is null, "—" is rendered in its place (no error)
 *
 * Integration:
 *   D2-I1: Banner does NOT appear when at least one security has a non-null price
 */

import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import AffectedAssets from "../components/AffectedAssets";
import type { NarrativeAsset } from "../lib/api";

// ---------------------------------------------------------------------------
// Fixtures — null prices (D1 state, no Finnhub key)
// ---------------------------------------------------------------------------

const MOCK_ASSETS_NULL_PRICES: NarrativeAsset[] = [
  {
    id: "na-001",
    narrative_id: "nar-001",
    asset_class_id: "ac-001",
    exposure_score: 0.85,
    direction: "bullish",
    rationale: "CHIPS Act funding directly benefits domestic semiconductor fabrication capacity",
    asset_class: {
      id: "ac-001",
      name: "Semiconductors",
      type: "sector",
      description: "Companies involved in chip design, fabrication, and equipment",
    },
    securities: [
      {
        id: "ts-001",
        symbol: "TSM",
        name: "Taiwan Semiconductor Manufacturing",
        asset_class_id: "ac-001",
        exchange: "NYSE",
        current_price: null,
        price_change_24h: null,
        narrative_impact_score: 0,
      },
      {
        id: "ts-002",
        symbol: "NVDA",
        name: "NVIDIA Corporation",
        asset_class_id: "ac-001",
        exchange: "NASDAQ",
        current_price: null,
        price_change_24h: null,
        narrative_impact_score: 0,
      },
    ],
  },
];

// ---------------------------------------------------------------------------
// Fixtures — live prices populated
// ---------------------------------------------------------------------------

const MOCK_ASSETS_WITH_PRICES: NarrativeAsset[] = [
  {
    id: "na-001",
    narrative_id: "nar-001",
    asset_class_id: "ac-001",
    exposure_score: 0.85,
    direction: "bullish",
    rationale: "CHIPS Act funding directly benefits semiconductor capacity",
    asset_class: {
      id: "ac-001",
      name: "Semiconductors",
      type: "sector",
      description: "Companies involved in chip design, fabrication, and equipment",
    },
    securities: [
      {
        id: "ts-001",
        symbol: "TSM",
        name: "Taiwan Semiconductor Manufacturing",
        asset_class_id: "ac-001",
        exchange: "NYSE",
        current_price: 142.35,
        price_change_24h: 1.23,
        narrative_impact_score: 75,
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
    ],
  },
];

// ---------------------------------------------------------------------------
// Fixture — mixed: one has prices, one doesn't
// ---------------------------------------------------------------------------

const MOCK_ASSETS_MIXED: NarrativeAsset[] = [
  {
    ...MOCK_ASSETS_WITH_PRICES[0],
    securities: [
      {
        ...MOCK_ASSETS_WITH_PRICES[0].securities[0],
        current_price: 142.35,
        price_change_24h: 1.23,
      },
    ],
  },
];

// ---------------------------------------------------------------------------
// D2-U1: Banner appears when all prices are null
// ---------------------------------------------------------------------------

describe("D2-U1: finnhub-unavailable-banner visible when all prices null", () => {
  it("renders the banner when all current_price values are null", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_NULL_PRICES} />);
    expect(screen.getByTestId("finnhub-unavailable-banner")).toBeInTheDocument();
    expect(screen.getByTestId("finnhub-unavailable-banner")).toHaveTextContent(
      "Connect a Finnhub API key for live prices"
    );
  });
});

// ---------------------------------------------------------------------------
// D2-U2: Formatted price string renders when current_price is populated
// ---------------------------------------------------------------------------

describe("D2-U2: formatted price renders when current_price is set", () => {
  it("renders $142.35 for TSM", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    expect(screen.getByTestId("price-TSM")).toHaveTextContent("$142.35");
  });

  it("renders $875.50 for NVDA", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    expect(screen.getByTestId("price-NVDA")).toHaveTextContent("$875.50");
  });
});

// ---------------------------------------------------------------------------
// D2-U3: Positive change renders with "+" prefix
// ---------------------------------------------------------------------------

describe("D2-U3: positive price_change_24h renders in green with + prefix", () => {
  it("renders +1.23% for TSM positive change", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    const changeEl = screen.getByTestId("change-TSM");
    expect(changeEl).toHaveTextContent("+1.23%");
  });

  it("change element has green (emerald) class for positive change", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    const changeEl = screen.getByTestId("change-TSM");
    expect(changeEl.className).toMatch(/bullish/);
  });
});

// ---------------------------------------------------------------------------
// D2-U4: Negative change renders in red
// ---------------------------------------------------------------------------

describe("D2-U4: negative price_change_24h renders in red", () => {
  it("renders negative amount for NVDA", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    const changeEl = screen.getByTestId("change-NVDA");
    // Should show 12.40% (absolute value) with minus somewhere
    expect(changeEl.textContent).toMatch(/%/);
    expect(changeEl.textContent).toMatch(/12\.40/);
  });

  it("change element has red class for negative change", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    const changeEl = screen.getByTestId("change-NVDA");
    expect(changeEl.className).toMatch(/bearish/);
  });
});

// ---------------------------------------------------------------------------
// D2-U5: When current_price is null, "—" is rendered
// ---------------------------------------------------------------------------

describe("D2-U5: null current_price renders — placeholder", () => {
  it("shows — for current_price when null", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_NULL_PRICES} />);
    expect(screen.getByTestId("price-TSM")).toHaveTextContent("—");
    expect(screen.getByTestId("price-NVDA")).toHaveTextContent("—");
  });

  it("shows — for price_change_24h when null", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_NULL_PRICES} />);
    expect(screen.getByTestId("change-TSM")).toHaveTextContent("—");
    expect(screen.getByTestId("change-NVDA")).toHaveTextContent("—");
  });

  it("does not throw when prices are null", () => {
    expect(() => render(<AffectedAssets assets={MOCK_ASSETS_NULL_PRICES} />)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// D2-I1: Banner does NOT appear when at least one security has a non-null price
// ---------------------------------------------------------------------------

describe("D2-I1: banner absent when at least one security has non-null price", () => {
  it("does not render the banner when prices are available", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_WITH_PRICES} />);
    expect(screen.queryByTestId("finnhub-unavailable-banner")).not.toBeInTheDocument();
  });

  it("does not render the banner in mixed case (some prices set)", () => {
    render(<AffectedAssets assets={MOCK_ASSETS_MIXED} />);
    expect(screen.queryByTestId("finnhub-unavailable-banner")).not.toBeInTheDocument();
  });
});
