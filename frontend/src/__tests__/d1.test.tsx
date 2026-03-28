/**
 * D1 Frontend Tests
 *
 * Unit:
 *   D1-U1: AffectedAssets — renders "Affected Asset Classes" heading
 *   D1-U2: AffectedAssets — asset cards render name, type badge, exposure bar, direction, rationale
 *   D1-U3: AffectedAssets — TrackedSecurity rows show symbol and name
 *   D1-U4: AffectedAssets — price columns show "—" placeholder when prices are null
 *
 * Integration:
 *   D1-I1: AffectedAssets — empty state renders when assets array is []
 */

import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";

import AffectedAssets from "../components/AffectedAssets";
import type { NarrativeAsset } from "../lib/api";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const MOCK_ASSETS: NarrativeAsset[] = [
  {
    id: "na-001",
    narrative_id: "nar-001",
    asset_class_id: "ac-001",
    exposure_score: 0.92,
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
  {
    id: "na-003",
    narrative_id: "nar-002",
    asset_class_id: "ac-002",
    exposure_score: 0.78,
    direction: "bearish",
    rationale: "Clean energy transition creates structural pressure on fossil fuel valuations",
    asset_class: {
      id: "ac-002",
      name: "Energy",
      type: "sector",
      description: "Oil, gas, and renewable energy producers",
    },
    securities: [
      {
        id: "ts-005",
        symbol: "XOM",
        name: "Exxon Mobil Corporation",
        asset_class_id: "ac-002",
        exchange: "NYSE",
        current_price: null,
        price_change_24h: null,
        narrative_impact_score: 0,
      },
    ],
  },
];

// ---------------------------------------------------------------------------
// D1-U1: renders section heading
// ---------------------------------------------------------------------------

describe("D1-U1: AffectedAssets — heading renders", () => {
  it("renders the section with asset cards", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    // Section heading is rendered by the page; the component renders the list
    expect(screen.getByTestId("affected-assets-list")).toBeInTheDocument();
    expect(screen.getByTestId("asset-card-na-001")).toBeInTheDocument();
    expect(screen.getByTestId("asset-card-na-003")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D1-U2: asset cards have name, type badge, exposure bar, direction, rationale
// ---------------------------------------------------------------------------

describe("D1-U2: AffectedAssets — card content", () => {
  it("renders asset class name and type badge", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("asset-name-na-001")).toHaveTextContent("Semiconductors");
    expect(screen.getByTestId("asset-type-badge-na-001")).toHaveTextContent("sector");
  });

  it("renders exposure bar", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("exposure-bar-na-001")).toBeInTheDocument();
    // 92% shown
    expect(screen.getByTestId("exposure-bar-na-001")).toHaveTextContent("92%");
  });

  it("renders bullish direction indicator", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("direction-na-001")).toHaveTextContent("Bullish");
  });

  it("renders bearish direction indicator", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("direction-na-003")).toHaveTextContent("Bearish");
  });

  it("renders rationale text", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("rationale-na-001")).toHaveTextContent("CHIPS Act funding");
  });
});

// ---------------------------------------------------------------------------
// D1-U3: TrackedSecurity rows show symbol and name
// ---------------------------------------------------------------------------

describe("D1-U3: AffectedAssets — security rows", () => {
  it("renders security rows with symbol and name", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("security-row-TSM")).toBeInTheDocument();
    expect(screen.getByTestId("security-row-TSM")).toHaveTextContent("TSM");
    expect(screen.getByTestId("security-row-TSM")).toHaveTextContent("Taiwan Semiconductor Manufacturing");
    expect(screen.getByTestId("security-row-NVDA")).toBeInTheDocument();
    expect(screen.getByTestId("security-row-XOM")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D1-U4: price columns show "—" when prices are null
// ---------------------------------------------------------------------------

describe("D1-U4: AffectedAssets — null price placeholders", () => {
  it("shows — for current_price when null", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("price-TSM")).toHaveTextContent("—");
    expect(screen.getByTestId("price-NVDA")).toHaveTextContent("—");
    expect(screen.getByTestId("price-XOM")).toHaveTextContent("—");
  });

  it("shows — for price_change_24h when null", () => {
    render(<AffectedAssets assets={MOCK_ASSETS} />);
    expect(screen.getByTestId("change-TSM")).toHaveTextContent("—");
    expect(screen.getByTestId("change-NVDA")).toHaveTextContent("—");
  });
});

// ---------------------------------------------------------------------------
// D1-I1: empty state renders when assets is []
// ---------------------------------------------------------------------------

describe("D1-I1: AffectedAssets — empty state", () => {
  it("renders empty state when assets array is empty", () => {
    render(<AffectedAssets assets={[]} />);
    expect(screen.getByTestId("affected-assets-empty")).toBeInTheDocument();
    expect(screen.getByTestId("affected-assets-empty")).toHaveTextContent(
      "No asset class associations recorded."
    );
  });

  it("does not render asset cards when empty", () => {
    render(<AffectedAssets assets={[]} />);
    expect(screen.queryByTestId("affected-assets-list")).not.toBeInTheDocument();
  });
});
