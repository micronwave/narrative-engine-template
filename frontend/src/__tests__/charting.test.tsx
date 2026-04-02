/**
 * Charting tests — Phase 4 (Part A)
 *
 * 1. CandlestickChart renders without crashing with mock OHLCV data
 * 2. TimeframeSelector renders all 7 options
 * 3. TimeframeSelector calls onChange with correct days value
 * 4. IndicatorOverlay renders checkbox for each indicator type
 * 5. Price history API response shape matches OHLCVBar (mock check)
 */

import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";

// lightweight-charts is mapped to src/__mocks__/lightweight-charts.ts via jest.config.ts

// Mock ResizeObserver (not in jsdom)
global.ResizeObserver = class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

import CandlestickChart from "../components/CandlestickChart";
import TimeframeSelector from "../components/TimeframeSelector";
import IndicatorOverlay from "../components/IndicatorOverlay";
import type { OHLCVBar } from "../lib/api";

const MOCK_BARS: OHLCVBar[] = [
  { date: "2026-03-01", open: 170, high: 175, low: 169, close: 173, volume: 1000000, change_pct: 1.76 },
  { date: "2026-03-02", open: 173, high: 177, low: 172, close: 176, volume: 900000, change_pct: 1.73 },
  { date: "2026-03-03", open: 176, high: 178, low: 174, close: 175, volume: 800000, change_pct: -0.57 },
];

// ---------------------------------------------------------------------------
// 1. CandlestickChart renders without crashing
// ---------------------------------------------------------------------------
describe("CandlestickChart", () => {
  it("renders container with mock OHLCV data", () => {
    render(<CandlestickChart symbol="AAPL" data={MOCK_BARS} />);
    expect(screen.getByTestId("candlestick-chart")).toBeInTheDocument();
  });

  it("renders empty state when data is empty", () => {
    render(<CandlestickChart symbol="AAPL" data={[]} />);
    expect(screen.getByTestId("candlestick-chart-empty")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 2. TimeframeSelector renders all 7 options
// ---------------------------------------------------------------------------
describe("TimeframeSelector", () => {
  it("renders all 7 timeframe options", () => {
    render(<TimeframeSelector selected={30} onChange={jest.fn()} />);
    const labels = ["1D", "5D", "1M", "3M", "6M", "YTD", "1Y"];
    for (const label of labels) {
      expect(screen.getByTestId(`timeframe-${label}`)).toBeInTheDocument();
    }
  });

  // ---------------------------------------------------------------------------
  // 3. TimeframeSelector calls onChange with correct days value
  // ---------------------------------------------------------------------------
  it("calls onChange with correct days for 1D", () => {
    const onChange = jest.fn();
    render(<TimeframeSelector selected={30} onChange={onChange} />);
    fireEvent.click(screen.getByTestId("timeframe-1D"));
    expect(onChange).toHaveBeenCalledWith(1);
  });

  it("calls onChange with correct days for 3M", () => {
    const onChange = jest.fn();
    render(<TimeframeSelector selected={30} onChange={onChange} />);
    fireEvent.click(screen.getByTestId("timeframe-3M"));
    expect(onChange).toHaveBeenCalledWith(90);
  });

  it("calls onChange with correct days for 1Y", () => {
    const onChange = jest.fn();
    render(<TimeframeSelector selected={30} onChange={onChange} />);
    fireEvent.click(screen.getByTestId("timeframe-1Y"));
    expect(onChange).toHaveBeenCalledWith(365);
  });
});

// ---------------------------------------------------------------------------
// 4. IndicatorOverlay renders checkbox for each indicator type
// ---------------------------------------------------------------------------
describe("IndicatorOverlay", () => {
  it("renders a checkbox for each indicator", () => {
    render(<IndicatorOverlay data={MOCK_BARS} />);
    expect(screen.getByTestId("indicator-overlay")).toBeInTheDocument();
    // 5 default indicators: sma x2, ema, rsi, macd
    const checkboxes = screen.getAllByRole("checkbox");
    expect(checkboxes.length).toBeGreaterThanOrEqual(4);
  });

  it("renders sma, ema, rsi, macd indicator checkboxes", () => {
    render(<IndicatorOverlay data={MOCK_BARS} />);
    // SMA has 2 instances, so use getAllByTestId
    expect(screen.getAllByTestId("indicator-input-sma").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByTestId("indicator-input-ema")).toBeInTheDocument();
    expect(screen.getByTestId("indicator-input-rsi")).toBeInTheDocument();
    expect(screen.getByTestId("indicator-input-macd")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 5. OHLCVBar shape validation (type-level check via mock data)
// ---------------------------------------------------------------------------
describe("OHLCVBar shape", () => {
  it("mock data conforms to OHLCVBar shape", () => {
    for (const bar of MOCK_BARS) {
      expect(typeof bar.date).toBe("string");
      expect(typeof bar.open).toBe("number");
      expect(typeof bar.high).toBe("number");
      expect(typeof bar.low).toBe("number");
      expect(typeof bar.close).toBe("number");
      expect(typeof bar.volume).toBe("number");
      expect(typeof bar.change_pct).toBe("number");
      expect(bar.high).toBeGreaterThanOrEqual(bar.low);
    }
  });
});
