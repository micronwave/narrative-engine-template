"use client";

import { useState } from "react";
import type { OHLCVBar } from "@/lib/api";

export type IndicatorConfig = {
  type: "sma" | "ema" | "rsi" | "macd" | "bollinger";
  enabled: boolean;
  params: Record<string, number>;
};

export const DEFAULT_INDICATORS: IndicatorConfig[] = [
  { type: "sma", enabled: false, params: { period: 20 } },
  { type: "sma", enabled: false, params: { period: 50 } },
  { type: "ema", enabled: false, params: { period: 12 } },
  { type: "rsi", enabled: true, params: { period: 14 } },
  { type: "macd", enabled: false, params: { fast: 12, slow: 26, signal: 9 } },
];

const INDICATOR_LABELS: Record<string, string> = {
  sma: "SMA",
  ema: "EMA",
  rsi: "RSI",
  macd: "MACD",
  bollinger: "BB",
};

type Props = {
  data: OHLCVBar[];
  onIndicatorsChange?: (indicators: IndicatorConfig[]) => void;
};

export default function IndicatorOverlay({ data: _data, onIndicatorsChange }: Props) {
  const [indicators, setIndicators] = useState<IndicatorConfig[]>(DEFAULT_INDICATORS);

  function toggle(index: number) {
    const updated = indicators.map((ind, i) =>
      i === index ? { ...ind, enabled: !ind.enabled } : ind
    );
    setIndicators(updated);
    onIndicatorsChange?.(updated);
  }

  return (
    <div data-testid="indicator-overlay" className="flex flex-wrap items-center gap-3 py-2">
      <span className="font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted">
        Indicators
      </span>
      {indicators.map((ind, i) => {
        const label =
          INDICATOR_LABELS[ind.type] +
          (ind.type === "sma" || ind.type === "ema"
            ? ` ${ind.params.period}`
            : ind.type === "rsi"
            ? `(${ind.params.period})`
            : "");
        return (
          <label
            key={i}
            data-testid={`indicator-checkbox-${ind.type}-${i}`}
            className="flex items-center gap-1.5 cursor-pointer group"
          >
            <input
              type="checkbox"
              checked={ind.enabled}
              onChange={() => toggle(i)}
              className="w-3 h-3 accent-[var(--accent-primary)]"
              data-testid={`indicator-input-${ind.type}`}
            />
            <span className="font-mono text-[10px] text-text-secondary group-hover:text-text-primary transition-colors">
              {label}
            </span>
          </label>
        );
      })}
    </div>
  );
}
