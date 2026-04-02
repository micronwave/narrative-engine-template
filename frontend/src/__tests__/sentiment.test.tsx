/**
 * Sentiment tests — Part C (Social Sentiment System)
 *
 * 11. Sentiment page renders market gauge
 * 12. Social page renders trending tickers section
 * 13. SignalBadge renders correct color for bearish/bullish/neutral
 */

import React from "react";
import { render, screen, waitFor, act } from "@testing-library/react";
import "@testing-library/jest-dom";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Per-URL mock: return appropriate shapes for each endpoint
global.fetch = jest.fn().mockImplementation((url: string) => {
  const u = String(url);
  let data: Record<string, unknown>;

  if (u.includes("/history")) {
    // fetchSentimentHistory
    data = { ticker: "AAPL", hours: 720, data: [] };
  } else if (u.includes("/api/sentiment/market")) {
    // fetchMarketSentiment
    data = {
      market_score: 0.2,
      bullish_pct: 55,
      bearish_pct: 20,
      neutral_pct: 25,
      top_bullish: [],
      top_bearish: [],
      spikes: [],
    };
  } else if (u.includes("/api/sentiment/")) {
    // fetchTickerSentiment — extract ticker from URL to avoid duplicate key warnings
    const ticker = u.split("/api/sentiment/")[1]?.split("?")[0]?.split("/")[0] ?? "AAPL";
    data = {
      ticker,
      composite_score: 0.2,
      news_component: 0.1,
      social_component: 0.3,
      momentum_component: 0.1,
      message_volume_24h: 50,
      sources: { stocktwits: null, narrative_signals: null },
      spike_detected: false,
      computed_at: null,
    };
  } else if (u.includes("/api/social/trending")) {
    // fetchTrendingTickers
    data = { hours: 24, tickers: [] };
  } else if (u.includes("/api/social/")) {
    // fetchSocialDetail — extract ticker from URL to avoid duplicate key warnings
    const ticker = u.split("/api/social/")[1]?.split("?")[0] ?? "AAPL";
    data = { ticker, stocktwits: null, narrative_signals: null };
  } else if (u.includes("/price-history")) {
    // fetchPriceHistory
    data = { symbol: "AAPL", available: false, data: [] };
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

// lightweight-charts is mapped to a mock in jest.config.ts

// ---------------------------------------------------------------------------
// 11. Sentiment page renders market gauge
// ---------------------------------------------------------------------------
describe("SentimentPage", () => {
  it("renders the market gauge after data loads", async () => {
    const { default: SentimentPage } = await import("../app/sentiment/page");
    await act(async () => {
      render(<SentimentPage />);
    });
    await waitFor(() => {
      expect(screen.getByTestId("sentiment-gauge")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 12. Social page renders trending tickers section
// ---------------------------------------------------------------------------
describe("SocialPage", () => {
  it("renders the trending tickers section (empty state)", async () => {
    const { default: SocialPage } = await import("../app/social/page");
    await act(async () => {
      render(<SocialPage />);
    });
    await waitFor(() => {
      // The section exists in the DOM: either with data or as a hidden empty-state sentinel
      expect(screen.getByTestId("trending-tickers-section")).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// 13. SignalBadge renders correct testids for bullish/bearish/neutral
// ---------------------------------------------------------------------------
describe("SignalBadge", () => {
  it("renders bullish badge with correct testid", async () => {
    const { default: SignalBadge } = await import("../components/SignalBadge");
    render(<SignalBadge direction="bullish" confidence={0.82} />);
    expect(screen.getByTestId("signal-badge-bullish")).toBeInTheDocument();
  });

  it("renders bearish badge with correct testid", async () => {
    const { default: SignalBadge } = await import("../components/SignalBadge");
    render(<SignalBadge direction="bearish" confidence={0.65} />);
    expect(screen.getByTestId("signal-badge-bearish")).toBeInTheDocument();
  });

  it("renders neutral badge with correct testid", async () => {
    const { default: SignalBadge } = await import("../components/SignalBadge");
    render(<SignalBadge direction="neutral" confidence={0.3} />);
    expect(screen.getByTestId("signal-badge-neutral")).toBeInTheDocument();
  });

  it("shows confidence percentage", async () => {
    const { default: SignalBadge } = await import("../components/SignalBadge");
    render(<SignalBadge direction="bullish" confidence={0.75} />);
    expect(screen.getByTestId("signal-badge-bullish").textContent).toContain("75%");
  });
});
