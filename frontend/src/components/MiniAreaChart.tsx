"use client";

import { useState, useRef, useCallback } from "react";

type DataPoint = { date: string; value: number };

type Props = {
  data: DataPoint[];
  color: string;
  width?: number;
  height?: number;
};

/** Format date string to short day label */
function dayLabel(dateStr: string): string {
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString("en-US", { weekday: "short" }).toUpperCase();
  } catch {
    return dateStr.slice(5);
  }
}

export default function MiniAreaChart({ data, color, width = 220, height = 90 }: Props) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  const padX = 4;
  const padTop = 8;
  const padBottom = 16; // space for x-axis labels
  const chartW = width - padX * 2;
  const chartH = height - padTop - padBottom;

  const hasEnoughData = Array.isArray(data) && data.length >= 2;

  const values = hasEnoughData ? data.map((d) => d.value) : [0, 0];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const points = (hasEnoughData ? data : []).map((d, i) => ({
    x: padX + (i / (data.length - 1)) * chartW,
    y: padTop + chartH - ((d.value - min) / range) * chartH,
    ...d,
  }));

  if (!hasEnoughData || points.length < 2) return null;

  // SVG path for the line
  const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x},${p.y}`).join(" ");
  // Area path (line + close to bottom)
  const areaPath = `${linePath} L${points[points.length - 1].x},${padTop + chartH} L${points[0].x},${padTop + chartH} Z`;

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (!svgRef.current) return;
      const rect = svgRef.current.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      // Find nearest point
      let nearest = 0;
      let minDist = Infinity;
      for (let i = 0; i < points.length; i++) {
        const dist = Math.abs(points[i].x - mouseX);
        if (dist < minDist) {
          minDist = dist;
          nearest = i;
        }
      }
      setHoverIdx(nearest);
    },
    [points]
  );

  const hoverPoint = hoverIdx !== null ? points[hoverIdx] : null;

  return (
    <div style={{ position: "relative", width, height }}>
      <svg
        ref={svgRef}
        width={width}
        height={height}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setHoverIdx(null)}
        style={{ cursor: "crosshair" }}
      >
        {/* Area fill */}
        <path d={areaPath} fill={color} opacity={0.12} />
        {/* Line stroke */}
        <path d={linePath} fill="none" stroke={color} strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />

        {/* Data points (small dots) */}
        {points.map((p, i) => (
          <circle
            key={`${p.date}-${i}`}
            cx={p.x}
            cy={p.y}
            r={hoverIdx === i ? 3.5 : 1.5}
            fill={color}
            opacity={hoverIdx === i ? 1 : 0.6}
          />
        ))}

        {/* Hover crosshair line */}
        {hoverPoint && (
          <line
            x1={hoverPoint.x}
            y1={padTop}
            x2={hoverPoint.x}
            y2={padTop + chartH}
            stroke={color}
            strokeWidth={0.5}
            opacity={0.3}
            strokeDasharray="2,2"
          />
        )}

        {/* X-axis day labels */}
        {points.map((p, i) => (
          <text
            key={`label-${p.date}-${i}`}
            x={p.x}
            y={height - 2}
            textAnchor="middle"
            fill="var(--text-disabled)"
            style={{ fontSize: 8, fontFamily: "var(--font-mono)", letterSpacing: "0.5px" }}
          >
            {dayLabel(p.date)}
          </text>
        ))}
      </svg>

      {/* Hover tooltip */}
      {hoverPoint && (
        <div
          style={{
            position: "absolute",
            left: Math.min(hoverPoint.x - 40, width - 85),
            top: Math.max(hoverPoint.y - 32, 0),
            background: "var(--bg-surface-hover)",
            border: "1px solid var(--bg-border)",
            padding: "3px 6px",
            fontSize: 10,
            fontFamily: "var(--font-mono)",
            color: "var(--text-primary)",
            pointerEvents: "none",
            whiteSpace: "nowrap",
            zIndex: 10,
          }}
        >
          {hoverPoint.date} · {(hoverPoint.value * 100).toFixed(1)}%
        </div>
      )}
    </div>
  );
}
