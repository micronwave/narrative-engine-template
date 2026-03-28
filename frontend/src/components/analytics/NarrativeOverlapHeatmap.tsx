"use client";

import { useEffect, useState } from "react";
import { fetchNarrativeOverlap, type AnalyticsOverlapResponse } from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";

type Props = {
  timeRange: string;
};

function cellBackground(value: number, isDiagonal: boolean, nsScore?: number): string {
  if (isDiagonal) {
    const alpha = 0.15 + (nsScore ?? value) * 0.4;
    return `rgba(45,114,210,${alpha.toFixed(2)})`;
  }
  if (value > 0.35) {
    return `rgba(236,154,60,${(0.2 + value * 0.5).toFixed(2)})`;
  }
  if (value > 0.15) {
    return `rgba(45,114,210,${(0.1 + value * 0.4).toFixed(2)})`;
  }
  return `rgba(115,128,145,${(0.05 + value * 0.15).toFixed(2)})`;
}

function abbreviate(name: string): string {
  return name.split(" ").slice(0, 2).join(" ");
}

export default function NarrativeOverlapHeatmap({ timeRange }: Props) {
  const [data, setData] = useState<AnalyticsOverlapResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hoveredCell, setHoveredCell] = useState<{ row: number; col: number } | null>(null);

  const days = parseDays(timeRange);

  useEffect(() => {
    setError(null);
    fetchNarrativeOverlap(days)
      .then(setData)
      .catch(() => setError("Failed to load overlap data"));
  }, [days]);

  if (error) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  if (!data || data.narratives.length === 0) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-text-tertiary">No overlap data available</p>
      </div>
    );
  }

  const { narratives, matrix } = data;
  const names = narratives.map((n) => abbreviate(n.name));
  const n = narratives.length;

  return (
    <div>
      <SectionHeader />

      <div
        className="grid gap-1"
        style={{ gridTemplateColumns: `64px 1fr` }}
      >
        {/* X-axis header labels */}
        <div /> {/* spacer for y-axis column */}
        <div
          className="grid gap-[3px]"
          style={{ gridTemplateColumns: `repeat(${n}, 1fr)` }}
        >
          {names.map((name, i) => (
            <div
              key={`x-${i}`}
              className="font-mono text-[8px] text-text-tertiary text-center overflow-hidden text-ellipsis whitespace-nowrap"
            >
              {name}
            </div>
          ))}
        </div>

        {/* Rows: y-label + cells */}
        {matrix.map((row, ri) => (
          <div key={`row-${ri}`} className="contents">
            {/* Y-axis label */}
            <div className="font-mono text-[9px] text-text-tertiary flex items-center justify-end pr-1.5 overflow-hidden text-ellipsis whitespace-nowrap">
              {names[ri]}
            </div>

            {/* Cell row */}
            <div
              className="grid gap-[3px]"
              style={{ gridTemplateColumns: `repeat(${n}, 1fr)` }}
            >
              {row.map((val, ci) => {
                const isDiagonal = ri === ci;
                const isHovered = hoveredCell?.row === ri && hoveredCell?.col === ci;

                return (
                  <div
                    key={`cell-${ri}-${ci}`}
                    className="relative cursor-pointer"
                    style={{
                      aspectRatio: "1",
                      background: cellBackground(val, isDiagonal, isDiagonal ? narratives[ri].ns_score : undefined),
                      borderRadius: 1,
                      outline: isHovered ? "1px solid var(--intent-primary)" : "1px solid transparent",
                      transition: "all 0.15s ease",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                    }}
                    onMouseEnter={() => setHoveredCell({ row: ri, col: ci })}
                    onMouseLeave={() => setHoveredCell(null)}
                  >
                    {/* Value text */}
                    {!isDiagonal && (
                      <span
                        className="font-mono text-[10px] text-text-secondary"
                        style={{ opacity: val > 0.05 ? 1 : 0.4 }}
                      >
                        {val > 0.05 ? val.toFixed(2) : ""}
                      </span>
                    )}

                    {/* Tooltip */}
                    {isHovered && !isDiagonal && (
                      <div
                        className="absolute left-1/2 -translate-x-1/2 whitespace-nowrap pointer-events-none"
                        style={{
                          bottom: "calc(100% + 6px)",
                          background: "var(--bg-surface)",
                          border: "1px solid var(--bg-border)",
                          padding: "6px 10px",
                          borderRadius: 3,
                          zIndex: 100,
                          fontSize: 11,
                          fontFamily: "var(--font-mono)",
                          color: "var(--text-primary)",
                          boxShadow: "0 4px 12px rgba(0,0,0,0.5)",
                        }}
                      >
                        {narratives[ri].name} &times; {narratives[ci].name}: {val.toFixed(2)} overlap
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function SectionHeader() {
  return (
    <div className="flex items-baseline gap-3 mb-4">
      <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
        Narrative Overlap
      </h2>
      <span className="font-mono text-[11px] text-text-tertiary">document + asset + topic</span>
    </div>
  );
}
