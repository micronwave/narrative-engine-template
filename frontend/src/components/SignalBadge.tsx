"use client";

import { ArrowUp, ArrowDown, Minus } from "lucide-react";

type Props = {
  direction?: string | null;
  confidence?: number | null;
};

/**
 * Inline badge showing signal direction arrow + confidence percentage.
 * Bearish = red down arrow, Bullish = green up arrow, Neutral = gray dash.
 * Example: "BEARISH 82%"
 */
export default function SignalBadge({ direction, confidence }: Props) {
  if (!direction || direction === "neutral") {
    return (
      <span
        data-testid="signal-badge-neutral"
        className="flex items-center gap-0.5"
        style={{
          fontSize: 10,
          fontFamily: "var(--font-mono)",
          color: "var(--text-disabled)",
          letterSpacing: "0.3px",
        }}
      >
        <Minus size={10} />
        NEUTRAL
      </span>
    );
  }

  const isBullish = direction === "bullish";
  const Icon = isBullish ? ArrowUp : ArrowDown;
  const color = isBullish ? "var(--bullish)" : "var(--bearish)";
  const pct = confidence != null ? ` ${Math.round(confidence * 100)}%` : "";

  return (
    <span
      data-testid={isBullish ? "signal-badge-bullish" : "signal-badge-bearish"}
      className="flex items-center gap-0.5"
      style={{
        fontSize: 10,
        fontFamily: "var(--font-mono)",
        color,
        letterSpacing: "0.3px",
      }}
    >
      <Icon size={10} />
      {direction.toUpperCase()}{pct}
    </span>
  );
}
