/**
 * D3 Frontend Tests
 *
 * Unit:
 *   D3-U1: /stocks page renders heading "Narrative-Affected Securities"
 *   D3-U2: Securities table renders rows with symbol, name, price, impact score
 *   D3-U3: Impact score badge has green/amber/red class based on score value
 *   D3-U4: "data-testid=impact-score-{symbol}" is present for each security row
 *   D3-U5: Changing asset class dropdown updates the displayed list
 *   D3-U6: Setting min_impact filters out low-score securities
 *   D3-U7: Clicking a row opens the detail drawer
 *   D3-U8: Detail drawer shows "Affecting Narratives" list with links to /narrative/{id}
 *   D3-U9: Empty state renders when securities array is []
 *
 * Integration:
 *   D3-I1: Loading state renders skeleton rows before data arrives
 */

import React from "react";
import {
  render,
  screen,
  fireEvent,
  waitFor,
  act,
} from "@testing-library/react";
import "@testing-library/jest-dom";

// Mock the api module
jest.mock("@/lib/api", () => ({
  fetchStocks: jest.fn(),
  fetchAssetClasses: jest.fn(),
  fetchStockDetail: jest.fn(),
  fetchNarrativeAssets: jest.fn(),
  fetchAssets: jest.fn(),
  fetchPriceHistory: jest.fn().mockResolvedValue({ symbol: "TSM", data: [], available: false }),
}));

import StocksPage from "../app/stocks/page";
import StockDetailDrawer from "../components/StockDetailDrawer";
import type { TrackedSecurity, AssetClass, StockDetail } from "../lib/api";
import { fetchStocks, fetchAssetClasses, fetchStockDetail } from "../lib/api";

const mockFetchStocks = fetchStocks as jest.MockedFunction<typeof fetchStocks>;
const mockFetchAssetClasses = fetchAssetClasses as jest.MockedFunction<typeof fetchAssetClasses>;
const mockFetchStockDetail = fetchStockDetail as jest.MockedFunction<typeof fetchStockDetail>;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const MOCK_ASSET_CLASSES: AssetClass[] = [
  { id: "ac-001", name: "Semiconductors", type: "sector", description: "Chip companies" },
  { id: "ac-002", name: "Energy", type: "sector", description: "Oil and gas" },
  { id: "ac-005", name: "Currencies", type: "currency", description: "FX instruments" },
];

const MOCK_SECURITIES: TrackedSecurity[] = [
  {
    id: "ts-001", symbol: "TSM", name: "Taiwan Semiconductor Manufacturing",
    asset_class_id: "ac-001", exchange: "NYSE",
    current_price: 142.35, price_change_24h: 1.23, narrative_impact_score: 85,
  },
  {
    id: "ts-005", symbol: "XOM", name: "Exxon Mobil Corporation",
    asset_class_id: "ac-002", exchange: "NYSE",
    current_price: 118.50, price_change_24h: -0.85, narrative_impact_score: 53,
  },
  {
    id: "ts-012", symbol: "UUP", name: "Invesco DB US Dollar Index",
    asset_class_id: "ac-005", exchange: "NYSE",
    current_price: null, price_change_24h: null, narrative_impact_score: 1,
  },
];

const MOCK_STOCK_DETAIL: StockDetail = {
  id: "ts-001",
  symbol: "TSM",
  name: "Taiwan Semiconductor Manufacturing",
  asset_class_id: "ac-001",
  exchange: "NYSE",
  current_price: 142.35,
  price_change_24h: 1.23,
  narrative_impact_score: 85,
  narratives: [
    {
      narrative_id: "nar-001",
      narrative_name: "Semiconductor Reshoring Acceleration",
      exposure_score: 0.92,
      direction: "bullish",
    },
  ],
};

// ---------------------------------------------------------------------------
// beforeEach: reset mocks
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockFetchStocks.mockResolvedValue(MOCK_SECURITIES);
  mockFetchAssetClasses.mockResolvedValue(MOCK_ASSET_CLASSES);
  mockFetchStockDetail.mockResolvedValue(MOCK_STOCK_DETAIL);
});

afterEach(() => {
  jest.clearAllMocks();
});

// ---------------------------------------------------------------------------
// D3-U1: heading renders
// ---------------------------------------------------------------------------

describe("D3-U1: /stocks page renders heading", () => {
  it("renders 'Narrative-Affected Securities' heading", async () => {
    render(<StocksPage />);
    expect(
      await screen.findByText("Narrative-Affected Securities")
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D3-U2: securities table renders rows
// ---------------------------------------------------------------------------

describe("D3-U2: Securities table renders rows", () => {
  it("renders rows with symbol, name, price, impact score", async () => {
    render(<StocksPage />);
    // Wait for loading to complete
    const tsmRow = await screen.findByTestId("stock-row-TSM");
    expect(tsmRow).toBeInTheDocument();
    expect(tsmRow).toHaveTextContent("TSM");
    expect(tsmRow).toHaveTextContent("Taiwan Semiconductor Manufacturing");

    const xomRow = screen.getByTestId("stock-row-XOM");
    expect(xomRow).toBeInTheDocument();
    expect(xomRow).toHaveTextContent("XOM");

    // Price shows for TSM
    expect(tsmRow).toHaveTextContent("$142.35");
    // Impact score shows
    expect(screen.getByTestId("impact-score-TSM")).toHaveTextContent("85");
  });
});

// ---------------------------------------------------------------------------
// D3-U3: Impact score badge color coding
// ---------------------------------------------------------------------------

describe("D3-U3: Impact score badge has correct color class", () => {
  it("TSM (score 85) has red class", async () => {
    render(<StocksPage />);
    await screen.findByTestId("impact-score-TSM");
    const badge = screen.getByTestId("impact-score-TSM");
    expect(badge.className).toMatch(/bearish/);
  });

  it("XOM (score 53) has amber class", async () => {
    render(<StocksPage />);
    await screen.findByTestId("impact-score-XOM");
    const badge = screen.getByTestId("impact-score-XOM");
    expect(badge.className).toMatch(/alert/);
  });

  it("UUP (score 1) has green class", async () => {
    render(<StocksPage />);
    await screen.findByTestId("impact-score-UUP");
    const badge = screen.getByTestId("impact-score-UUP");
    expect(badge.className).toMatch(/bullish/);
  });
});

// ---------------------------------------------------------------------------
// D3-U4: data-testid="impact-score-{symbol}" present for each row
// ---------------------------------------------------------------------------

describe("D3-U4: data-testid=impact-score-{symbol} present", () => {
  it("each security row has impact-score testid", async () => {
    render(<StocksPage />);
    await screen.findByTestId("stocks-table");
    for (const sec of MOCK_SECURITIES) {
      expect(screen.getByTestId(`impact-score-${sec.symbol}`)).toBeInTheDocument();
    }
  });
});

// ---------------------------------------------------------------------------
// D3-U5: Changing asset class dropdown updates the displayed list
// ---------------------------------------------------------------------------

describe("D3-U5: Asset class filter updates displayed list", () => {
  it("filters to Semiconductors (ac-001) — shows only TSM, hides XOM and UUP", async () => {
    render(<StocksPage />);
    await screen.findByTestId("stocks-table");

    const dropdown = screen.getByTestId("filter-asset-class");
    fireEvent.change(dropdown, { target: { value: "ac-001" } });

    // TSM (ac-001) should be visible
    expect(screen.getByTestId("stock-row-TSM")).toBeInTheDocument();
    // XOM (ac-002) and UUP (ac-005) should not be visible
    expect(screen.queryByTestId("stock-row-XOM")).not.toBeInTheDocument();
    expect(screen.queryByTestId("stock-row-UUP")).not.toBeInTheDocument();
  });

  it("resetting to All Classes shows all securities", async () => {
    render(<StocksPage />);
    await screen.findByTestId("stocks-table");

    const dropdown = screen.getByTestId("filter-asset-class");
    fireEvent.change(dropdown, { target: { value: "ac-001" } });
    fireEvent.change(dropdown, { target: { value: "all" } });

    expect(screen.getByTestId("stock-row-TSM")).toBeInTheDocument();
    expect(screen.getByTestId("stock-row-XOM")).toBeInTheDocument();
    expect(screen.getByTestId("stock-row-UUP")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D3-U6: Setting min_impact filters out low-score securities
// ---------------------------------------------------------------------------

describe("D3-U6: min_impact filter removes low-score securities", () => {
  it("setting min_impact=50 hides UUP (score 1) but keeps TSM (85) and XOM (53)", async () => {
    render(<StocksPage />);
    await screen.findByTestId("stocks-table");

    const input = screen.getByTestId("filter-min-impact");
    fireEvent.change(input, { target: { value: "50" } });

    expect(screen.getByTestId("stock-row-TSM")).toBeInTheDocument();
    expect(screen.getByTestId("stock-row-XOM")).toBeInTheDocument();
    expect(screen.queryByTestId("stock-row-UUP")).not.toBeInTheDocument();
  });

  it("setting min_impact=90 shows empty state (no securities >= 90 in mock)", async () => {
    render(<StocksPage />);
    await screen.findByTestId("stocks-table");

    const input = screen.getByTestId("filter-min-impact");
    fireEvent.change(input, { target: { value: "90" } });

    expect(screen.getByTestId("stocks-empty")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D3-U7: Clicking a row opens the detail drawer
// ---------------------------------------------------------------------------

describe("D3-U7: Clicking a row opens the detail drawer", () => {
  it("clicking TSM row opens the stock detail drawer", async () => {
    render(<StocksPage />);
    const row = await screen.findByTestId("stock-row-TSM");
    fireEvent.click(row);

    await waitFor(() => {
      const drawer = screen.getByTestId("stock-detail-drawer");
      expect(drawer.className).not.toContain("translate-x-full");
    });
  });
});

// ---------------------------------------------------------------------------
// D3-U8: Detail drawer shows affecting narratives with links
// ---------------------------------------------------------------------------

describe("D3-U8: Detail drawer shows Affecting Narratives with /narrative/{id} links", () => {
  it("drawer displays affecting narratives section after click", async () => {
    render(<StocksPage />);
    const row = await screen.findByTestId("stock-row-TSM");
    fireEvent.click(row);

    // Wait for stock detail to load (fetchStockDetail resolves)
    await waitFor(() => {
      expect(screen.getByTestId("affecting-narratives")).toBeInTheDocument();
    });

    // Check narrative link
    const link = screen.getByTestId("narrative-link-nar-001");
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute("href", "/narrative/nar-001");
    expect(link).toHaveTextContent("Semiconductor Reshoring Acceleration");
  });

  it("StockDetailDrawer renders affecting narratives from stockDetail prop", () => {
    render(
      <StockDetailDrawer
        isOpen={true}
        stockDetail={MOCK_STOCK_DETAIL}
        loading={false}
        onClose={() => {}}
      />
    );
    expect(screen.getByTestId("affecting-narratives")).toBeInTheDocument();
    expect(screen.getByTestId("narrative-link-nar-001")).toHaveAttribute(
      "href",
      "/narrative/nar-001"
    );
    expect(screen.getByText("Semiconductor Reshoring Acceleration")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D3-U9: Empty state renders when securities array is []
// ---------------------------------------------------------------------------

describe("D3-U9: Empty state renders when securities array is empty", () => {
  it("shows stocks-empty testid and message when no securities", async () => {
    mockFetchStocks.mockResolvedValue([]);
    render(<StocksPage />);
    const empty = await screen.findByTestId("stocks-empty");
    expect(empty).toBeInTheDocument();
    expect(empty).toHaveTextContent(
      "No tracked securities. Securities are associated with narratives via asset classes."
    );
  });

  it("does not render stocks-table when empty", async () => {
    mockFetchStocks.mockResolvedValue([]);
    render(<StocksPage />);
    await screen.findByTestId("stocks-empty");
    expect(screen.queryByTestId("stocks-table")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// D3-I1: Loading state renders skeleton rows before data arrives
// ---------------------------------------------------------------------------

describe("D3-I1: Loading state shows skeleton rows before data arrives", () => {
  it("renders skeleton elements while data is loading", () => {
    // Make the promise never resolve to freeze in loading state
    mockFetchStocks.mockImplementation(() => new Promise(() => {}));
    mockFetchAssetClasses.mockImplementation(() => new Promise(() => {}));

    render(<StocksPage />);

    // Skeleton elements should be present immediately (before async resolves)
    const skeletons = screen.getAllByTestId("stocks-skeleton");
    expect(skeletons.length).toBeGreaterThan(0);
    expect(screen.getByTestId("stocks-loading")).toBeInTheDocument();
  });

  it("removes skeleton and shows table after data loads", async () => {
    render(<StocksPage />);
    // Initially loading
    expect(screen.getByTestId("stocks-loading")).toBeInTheDocument();
    // After data loads
    await screen.findByTestId("stocks-table");
    expect(screen.queryByTestId("stocks-loading")).not.toBeInTheDocument();
  });
});
