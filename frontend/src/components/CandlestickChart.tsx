"use client";

import { useEffect, useRef } from "react";
import type { OHLCVBar } from "@/lib/api";
import type { IndicatorConfig } from "./IndicatorOverlay";

type Props = {
  symbol: string;
  data: OHLCVBar[];
  height?: number;
  indicators?: IndicatorConfig[];
};

// Hardcoded sub-pane margins so stacked bands never overlap
const VOLUME_MARGIN: Record<string, { top: number; bottom: number }> = {
  none: { top: 0.82, bottom: 0 },
  one:  { top: 0.88, bottom: 0 },
  two:  { top: 0.92, bottom: 0 },
};
const RSI_MARGIN: Record<string, { top: number; bottom: number }> = {
  noMACD:   { top: 0.75, bottom: 0.12 },
  withMACD: { top: 0.80, bottom: 0.08 },
};
const MACD_MARGIN: Record<string, { top: number; bottom: number }> = {
  noRSI:   { top: 0.75, bottom: 0.12 },
  withRSI: { top: 0.65, bottom: 0.20 },
};

export default function CandlestickChart({
  symbol: _symbol,
  data,
  height = 320,
  indicators = [],
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<import("lightweight-charts").IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    let cancelled = false;
    let syncCleanup: (() => void) | undefined;

    (async () => {
      const { createChart, CandlestickSeries, HistogramSeries, LineSeries } =
        await import("lightweight-charts");
      if (cancelled) return;

      const container = containerRef.current;
      if (!container) return;

      const style = getComputedStyle(document.documentElement);
      const bgSurface = style.getPropertyValue("--bg-surface").trim() || "#1c2127";
      const textSecondary = style.getPropertyValue("--text-secondary").trim() || "#abb3bf";
      const border = style.getPropertyValue("--bg-border").trim() || "#383e47";

      const chart = createChart(container, {
        width: container.clientWidth,
        height: height - 80,
        layout: { background: { color: bgSurface }, textColor: textSecondary },
        grid: { vertLines: { color: border }, horzLines: { color: border } },
        crosshair: { mode: 0 },
        timeScale: { borderColor: border },
        rightPriceScale: { borderColor: border },
      });

      chartRef.current = chart;

      const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: "#32a467",
        downColor: "#e05454",
        borderVisible: false,
        wickUpColor: "#32a467",
        wickDownColor: "#e05454",
      });

      candleSeries.setData(
        data.map((bar) => ({
          time: bar.date as import("lightweight-charts").Time,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
        }))
      );

      const hasRSI  = indicators.some((i) => i.type === "rsi"  && i.enabled);
      const hasMACD = indicators.some((i) => i.type === "macd" && i.enabled);
      const subCount = (hasRSI ? 1 : 0) + (hasMACD ? 1 : 0);

      // Volume pane — shrinks to leave room for sub-indicator panes
      const volMarginKey = subCount === 2 ? "two" : subCount === 1 ? "one" : "none";
      const volSeries = chart.addSeries(HistogramSeries, {
        color: "#3a4452",
        priceFormat: { type: "volume" },
        priceScaleId: "volume",
      });
      chart.priceScale("volume").applyOptions({
        scaleMargins: VOLUME_MARGIN[volMarginKey],
      });
      volSeries.setData(
        data.map((bar) => ({
          time: bar.date as import("lightweight-charts").Time,
          value: bar.volume,
          color: bar.close >= bar.open ? "#32a46744" : "#e0545444",
        }))
      );

      // Only import technicalindicators if any indicator is enabled
      const anyEnabled = indicators.some((i) => i.enabled);
      if (anyEnabled) {
        const TI = await import("technicalindicators");
        if (cancelled) return;

        const closes = data.map((b) => b.close);

        // --- Overlay indicators (SMA, EMA, Bollinger) ---
        for (const ind of indicators) {
          if (!ind.enabled) continue;

          if (ind.type === "sma") {
            const vals = TI.SMA.calculate({ period: ind.params.period, values: closes });
            const startIdx = closes.length - vals.length;
            const color = ind.params.period >= 50 ? "#1a6fc4" : "#4a9eff";
            const s = chart.addSeries(LineSeries, { color, lineWidth: 1, priceLineVisible: false });
            s.setData(
              vals.map((v, i) => ({
                time: data[startIdx + i].date as import("lightweight-charts").Time,
                value: v,
              }))
            );
          }

          if (ind.type === "ema") {
            const vals = TI.EMA.calculate({ period: ind.params.period, values: closes });
            const startIdx = closes.length - vals.length;
            const s = chart.addSeries(LineSeries, { color: "#f0a030", lineWidth: 1, priceLineVisible: false });
            s.setData(
              vals.map((v, i) => ({
                time: data[startIdx + i].date as import("lightweight-charts").Time,
                value: v,
              }))
            );
          }

          if (ind.type === "bollinger") {
            const period = ind.params.period ?? 20;
            const stdDev = ind.params.stdDev ?? 2;
            const vals = TI.BollingerBands.calculate({ period, stdDev, values: closes });
            const startIdx = closes.length - vals.length;
            const bbLines: { key: "upper" | "middle" | "lower"; color: string }[] = [
              { key: "upper",  color: "#4a9eff66" },
              { key: "middle", color: "#4a9eff" },
              { key: "lower",  color: "#4a9eff66" },
            ];
            for (const { key, color } of bbLines) {
              const s = chart.addSeries(LineSeries, { color, lineWidth: 1, priceLineVisible: false });
              s.setData(
                vals.map((v, i) => ({
                  time: data[startIdx + i].date as import("lightweight-charts").Time,
                  value: v[key],
                }))
              );
            }
          }
        }

        // --- RSI sub-pane ---
        if (hasRSI) {
          const rsiInd = indicators.find((i) => i.type === "rsi" && i.enabled)!;
          const vals = TI.RSI.calculate({ period: rsiInd.params.period, values: closes });
          const startIdx = closes.length - vals.length;
          const margins = hasMACD ? RSI_MARGIN.withMACD : RSI_MARGIN.noMACD;
          const rsiSeries = chart.addSeries(LineSeries, {
            color: "#a855f7",
            lineWidth: 1,
            priceScaleId: "rsi",
            priceLineVisible: false,
          });
          chart.priceScale("rsi").applyOptions({ scaleMargins: margins });
          rsiSeries.setData(
            vals.map((v, i) => ({
              time: data[startIdx + i].date as import("lightweight-charts").Time,
              value: v,
            }))
          );
        }

        // --- MACD sub-pane ---
        if (hasMACD) {
          const macdInd = indicators.find((i) => i.type === "macd" && i.enabled)!;
          const vals = TI.MACD.calculate({
            fastPeriod: macdInd.params.fast,
            slowPeriod: macdInd.params.slow,
            signalPeriod: macdInd.params.signal,
            values: closes,
            SimpleMAOscillator: false,
            SimpleMASignal: false,
          });
          const startIdx = closes.length - vals.length;
          const margins = hasRSI ? MACD_MARGIN.withRSI : MACD_MARGIN.noRSI;
          const macdLine = chart.addSeries(LineSeries, {
            color: "#3b82f6",
            lineWidth: 1,
            priceScaleId: "macd",
            priceLineVisible: false,
          });
          const signalLine = chart.addSeries(LineSeries, {
            color: "#f97316",
            lineWidth: 1,
            priceScaleId: "macd",
            priceLineVisible: false,
          });
          chart.priceScale("macd").applyOptions({ scaleMargins: margins });
          macdLine.setData(
            vals.map((v, i) => ({
              time: data[startIdx + i].date as import("lightweight-charts").Time,
              value: v.MACD ?? 0,
            }))
          );
          signalLine.setData(
            vals.map((v, i) => ({
              time: data[startIdx + i].date as import("lightweight-charts").Time,
              value: v.signal ?? 0,
            }))
          );
        }
      }

      chart.timeScale().fitContent();

      const ro = new ResizeObserver(() => {
        if (chart && container) {
          chart.applyOptions({ width: container.clientWidth });
        }
      });
      ro.observe(container);

      syncCleanup = () => {
        ro.disconnect();
        chart.remove();
        chartRef.current = null;
      };

      if (cancelled) {
        syncCleanup();
        syncCleanup = undefined;
      }
    })();

    return () => {
      cancelled = true;
      syncCleanup?.();
      syncCleanup = undefined;
    };
  }, [data, height, indicators]);

  if (data.length === 0) {
    return (
      <div
        data-testid="candlestick-chart-empty"
        className="flex items-center justify-center text-text-tertiary text-xs bg-inset rounded-sm"
        style={{ height }}
      >
        No price data available
      </div>
    );
  }

  return (
    <div data-testid="candlestick-chart" style={{ height }}>
      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
    </div>
  );
}
