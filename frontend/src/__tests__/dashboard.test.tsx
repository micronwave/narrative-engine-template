/**
 * Dashboard & Phase 7 tests
 *
 * 11. Dashboard page renders with default widget layout
 * 12. WidgetCatalog shows available widget types
 * 13. Portfolio page renders allocation treemap section
 * 14. Alerts page renders notification list
 * 15. Alerts page renders rules management tab
 */

import React from "react";
import { render, screen, waitFor, act, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
}

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Global fetch mock — covers all endpoints used by the pages under test
global.fetch = jest.fn().mockImplementation((url: string) => {
  const u = String(url);
  let data: Record<string, unknown> | unknown[];

  if (u.includes("/api/dashboard/layout")) {
    data = {
      widgets: [
        { id: "narrative_radar", type: "narrative_radar", title: "Narrative Radar" },
        { id: "signal_leaderboard", type: "signal_leaderboard", title: "Signal Leaderboard" },
      ],
      grid: {
        lg: [
          { i: "narrative_radar", x: 0, y: 0, w: 8, h: 4 },
          { i: "signal_leaderboard", x: 8, y: 0, w: 4, h: 4 },
        ],
      },
    };
  } else if (u.includes("/api/portfolio/summary")) {
    data = { total_value: 5000, total_pnl: 100, day_change: 50, day_change_pct: 1.0, position_count: 2 };
  } else if (u.includes("/api/portfolio/allocation")) {
    data = [{ group: "Technology", value: 5000, pct: 1.0, pnl: 100, tickers: ["AAPL"] }];
  } else if (u.includes("/api/portfolio/correlation")) {
    data = { tickers: [], matrix: [], warnings: [] };
  } else if (u.includes("/api/portfolio/concentration")) {
    data = { top3_pct: 1.0, top3_warning: false, sector_hhi: 0, sector_concentrated: false, single_stock_warnings: [] };
  } else if (u.includes("/api/portfolio/performance")) {
    data = { portfolio: [], benchmark: [], total_return_pct: 0, benchmark_return_pct: 0 };
  } else if (u.includes("/api/portfolio/exposure")) {
    data = { exposures: [] };
  } else if (u.includes("/api/portfolio")) {
    data = { id: "p1", name: "My Portfolio", holdings: [{ id: "h1", ticker: "AAPL", shares: 10 }] };
  } else if (u.includes("/api/alerts/types")) {
    data = { ns_above: "Ns score above", mutation: "Mutation detected", price_above: "Price above" };
  } else if (u.includes("/api/alerts/rules")) {
    data = [];
  } else if (u.includes("/api/alerts")) {
    data = [];
  } else if (u.includes("/api/ticker")) {
    data = [];
  } else if (u.includes("/api/narratives")) {
    data = [];
  } else if (u.includes("/api/signals/leaderboard")) {
    data = { signals: [] };
  } else if (u.includes("/api/stocks")) {
    data = [
      { symbol: "NVDA", name: "NVIDIA Corp", current_price: 850.0, price_change_24h: 3.5 },
      { symbol: "TSLA", name: "Tesla Inc", current_price: 175.0, price_change_24h: -2.1 },
    ];
  } else if (u.includes("/api/sentiment/market")) {
    data = { market_score: 0.4, bullish_pct: 55, bearish_pct: 20, neutral_pct: 25 };
  } else if (u.includes("/api/watchlist")) {
    data = { watchlist_id: "wl1", items: [{ id: "i1", item_id: "AAPL", item_type: "ticker", current_price: 190.0 }] };
  } else if (u.includes("/api/analytics/sector-convergence")) {
    data = { sectors: [{ name: "Technology", narrative_count: 3, weighted_pressure: 4.2 }] };
  } else if (u.includes("/api/earnings/upcoming")) {
    data = [{ ticker: "MSFT", company: "Microsoft", date: "2026-04-15", days_until: 14 }];
  } else {
    data = {};
  }

  return Promise.resolve({
    ok: true,
    json: async () => data,
  } as unknown as Response);
});

// Mock ResizeObserver (not in jsdom)
global.ResizeObserver = class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

// ---------------------------------------------------------------------------
// Mock contexts used by AlertsPage and PortfolioPage
// ---------------------------------------------------------------------------

jest.mock("@/contexts/AlertContext", () => ({
  useAlerts: () => ({
    unreadCount: 0,
    markRead: jest.fn(),
    markAllRead: jest.fn(),
    refresh: jest.fn(),
  }),
  AlertProvider: ({ children }: { children: React.ReactNode }) => React.createElement(React.Fragment, null, children),
}));

jest.mock("@/contexts/AuthContext", () => ({
  useAuth: () => ({ isSignedIn: false, signIn: jest.fn(), signOut: jest.fn(), token: null }),
  AuthProvider: ({ children }: { children: React.ReactNode }) => React.createElement(React.Fragment, null, children),
}));

jest.mock("@/contexts/WatchlistContext", () => ({
  useWatchlist: () => ({ watchlist: [], isWatched: () => false, toggle: jest.fn() }),
  WatchlistProvider: ({ children }: { children: React.ReactNode }) => React.createElement(React.Fragment, null, children),
}));

// ---------------------------------------------------------------------------
// 11. Dashboard page renders with default widget layout
// ---------------------------------------------------------------------------

describe("DashboardPage", () => {
  it("renders the dashboard grid with default widget layout", async () => {
    const { default: DashboardPage } = await import("../app/dashboard/page");
    await act(async () => {
      render(<DashboardPage />, { wrapper: makeWrapper() });
    });
    await waitFor(() => {
      expect(screen.getByTestId("dashboard-grid")).toBeInTheDocument();
    });
  });

  it("renders Edit Dashboard button", async () => {
    const { default: DashboardPage } = await import("../app/dashboard/page");
    await act(async () => {
      render(<DashboardPage />, { wrapper: makeWrapper() });
    });
    expect(screen.getByTestId("edit-dashboard-btn")).toBeInTheDocument();
  });

  it("respects an empty saved widget layout", async () => {
    const fetchMock = global.fetch as jest.Mock;
    const previousImpl = fetchMock.getMockImplementation();
    fetchMock.mockImplementation((url: unknown, ...rest: unknown[]) => {
      if (String(url).includes("/api/dashboard/layout")) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            widgets: [],
            grid: { lg: [] },
          }),
        } as Response);
      }
      return previousImpl ? previousImpl(url, ...rest) : Promise.resolve({ ok: true, json: async () => ({}) } as Response);
    });

    try {
      const { default: DashboardPage } = await import("../app/dashboard/page");
      await act(async () => {
        render(<DashboardPage />, { wrapper: makeWrapper() });
      });

      await waitFor(() => {
        expect(screen.getByTestId("dashboard-grid")).toBeInTheDocument();
      });
      expect(screen.queryByTestId("widget-body-narrative_radar")).toBeNull();
      expect(screen.queryByTestId("widget-body-alert_feed")).toBeNull();
    } finally {
      fetchMock.mockImplementation(previousImpl);
    }
  });
});

// ---------------------------------------------------------------------------
// 12. WidgetCatalog shows available widget types
// ---------------------------------------------------------------------------

describe("WidgetCatalog", () => {
  it("shows available widget types when opened", async () => {
    const { default: WidgetCatalog } = await import("../components/dashboard/WidgetCatalog");
    await act(async () => {
      render(
        <WidgetCatalog
          activeTypes={[]}
          onAdd={jest.fn()}
          onClose={jest.fn()}
        />
      );
    });
    expect(screen.getByTestId("widget-catalog")).toBeInTheDocument();
    // Check that some known widget types are present
    expect(screen.getByTestId("add-widget-narrative_radar")).toBeInTheDocument();
    expect(screen.getByTestId("add-widget-signal_leaderboard")).toBeInTheDocument();
    expect(screen.getByTestId("add-widget-top_movers")).toBeInTheDocument();
  });

  it("shows Add Widget button when editing and opens catalog on click", async () => {
    const { default: DashboardPage } = await import("../app/dashboard/page");
    await act(async () => {
      render(<DashboardPage />, { wrapper: makeWrapper() });
    });
    // Click Edit Dashboard to enter edit mode
    await act(async () => {
      fireEvent.click(screen.getByTestId("edit-dashboard-btn"));
    });
    // Add Widget button should now be visible
    expect(screen.getByTestId("add-widget-btn")).toBeInTheDocument();
    // Click Add Widget to open catalog
    await act(async () => {
      fireEvent.click(screen.getByTestId("add-widget-btn"));
    });
    expect(screen.getByTestId("widget-catalog")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 13. Portfolio page renders allocation treemap section
// ---------------------------------------------------------------------------

describe("PortfolioPage", () => {
  it("renders the allocation treemap section", async () => {
    const { default: PortfolioPage } = await import("../app/portfolio/page");
    await act(async () => {
      render(<PortfolioPage />);
    });
    await waitFor(() => {
      expect(screen.getByTestId("allocation-treemap")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 14. Alerts page renders notification list
// ---------------------------------------------------------------------------

describe("AlertsPage — notification list", () => {
  it("renders the notification list (default tab)", async () => {
    const { default: AlertsPage } = await import("../app/alerts/page");
    await act(async () => {
      render(<AlertsPage />);
    });
    await waitFor(() => {
      expect(screen.getByTestId("notification-list")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 15. Alerts page renders rules management tab
// ---------------------------------------------------------------------------

describe("AlertsPage — rules tab", () => {
  it("renders the Rules tab button", async () => {
    const { default: AlertsPage } = await import("../app/alerts/page");
    await act(async () => {
      render(<AlertsPage />);
    });
    // Tab buttons are always rendered — check the tabs container exists
    await waitFor(() => {
      expect(screen.getByTestId("alert-tabs")).toBeInTheDocument();
    });
    expect(screen.getByText(/Rules/)).toBeInTheDocument();
  });

  it("renders the rules management section on click", async () => {
    const { default: AlertsPage } = await import("../app/alerts/page");
    await act(async () => {
      render(<AlertsPage />);
    });
    await waitFor(() => {
      expect(screen.getByTestId("alert-tabs")).toBeInTheDocument();
    });
    // Click the Rules tab
    await act(async () => {
      fireEvent.click(screen.getByText(/^Rules$/));
    });
    await waitFor(() => {
      expect(screen.getByTestId("rules-management")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 16. Widget body tests — each widget exits loading state and renders data
// ---------------------------------------------------------------------------

const WIDGET_TYPES = [
  "narrative_radar",
  "signal_leaderboard",
  "top_movers",
  "sentiment_meter",
  "alert_feed",
  "watchlist",
  "market_heatmap",
  "portfolio_summary",
  "convergence_radar",
  "economic_calendar",
] as const;

describe("WidgetRenderer bodies", () => {
  it.each(WIDGET_TYPES)("%s widget renders data section (not loading…)", async (type) => {
    const { default: WidgetRenderer } = await import(
      "../components/dashboard/WidgetRenderer"
    );
    await act(async () => {
      render(
        <WidgetRenderer
          id="test-widget"
          type={type}
          title={type}
          isEditing={false}
          onRemove={jest.fn()}
        />,
        { wrapper: makeWrapper() }
      );
    });
    await waitFor(() => {
      expect(screen.getByTestId(`widget-body-${type}`)).toBeInTheDocument();
    });
    // Confirm the loading placeholder is gone
    expect(screen.queryByText(new RegExp(`${type.replace(/_/g, " ")}.*loading`, "i"))).toBeNull();
  });
});
