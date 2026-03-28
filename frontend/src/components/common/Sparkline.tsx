"use client";

import { COLORS } from "@/lib/colors";

type Props = {
  data: number[];
  width?: number;
  height?: number;
  className?: string;
};

export default function Sparkline({ data, width = 64, height = 20, className = "" }: Props) {
  const clean = data?.filter(Number.isFinite) ?? [];
  if (clean.length < 2) return null;

  const min = Math.min(...clean);
  const max = Math.max(...clean);
  const range = max - min || 1;
  const pad = 2;

  const points = clean.map((v, i) => {
    const x = pad + (i / (clean.length - 1)) * (width - 2 * pad);
    const y = pad + (height - 2 * pad) - ((v - min) / range) * (height - 2 * pad);
    return `${x},${y}`;
  }).join(" ");

  const isUp = clean[clean.length - 1] >= clean[0];
  const stroke = isUp ? COLORS.bullish : COLORS.bearish;

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={className}
    >
      <polyline
        points={points}
        fill="none"
        stroke={stroke}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
