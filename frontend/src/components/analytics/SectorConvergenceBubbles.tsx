"use client";

import { useEffect, useState, useMemo } from "react";
import {
  fetchSectorConvergence,
  type AnalyticsSectorResponse,
  type AnalyticsSectorEntry,
} from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";
import StageBadge from "@/components/common/StageBadge";
import { COLORS } from "@/lib/colors";

type Props = { timeRange: string };

const MARGIN = { top: 24, right: 24, bottom: 36, left: 48 };
const WIDTH = 400;
const HEIGHT = 340;
const INNER_W = WIDTH - MARGIN.left - MARGIN.right;
const INNER_H = HEIGHT - MARGIN.top - MARGIN.bottom;

function SectionHeader() {
  return (
    <div className="flex items-baseline gap-3 mb-4">
      <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
        Sector Convergence
      </h2>
      <span className="font-mono text-[11px] text-text-tertiary">
        narrative pressure by sector
      </span>
    </div>
  );
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * Math.max(0, Math.min(1, t));
}

function pressureColor(t: number): string {
  const r = Math.round(lerp(0x2f, 0x2d, t));
  const g = Math.round(lerp(0x34, 0x72, t));
  const b = Math.round(lerp(0x3c, 0xd2, t));
  return `rgb(${r},${g},${b})`;
}

export default function SectorConvergenceBubbles({ timeRange }: Props) {
  const [data, setData] = useState<AnalyticsSectorResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hoveredSector, setHoveredSector] = useState<string | null>(null);
  const [expandedSector, setExpandedSector] = useState<string | null>(null);

  const days = parseDays(timeRange);

  useEffect(() => {
    setError(null);
    fetchSectorConvergence(days)
      .then(setData)
      .catch(() => setError("Failed to load sector convergence data"));
  }, [days]);

  const sectors = data?.sectors ?? [];

  const layout = useMemo(() => {
    if (sectors.length === 0) return null;

    const pressures = sectors.map((s) => s.weighted_pressure);
    const counts = sectors.map((s) => s.narrative_count);

    const pMin = Math.min(...pressures);
    const pMax = Math.max(...pressures);
    const cMin = Math.min(...counts);
    const cMax = Math.max(...counts);

    const pRange = pMax - pMin || 1;
    const cRange = cMax - cMin || 1;

    const medP = median(pressures);
    const medC = median(counts);

    return { pMin, pMax, pRange, cMin, cMax, cRange, medP, medC };
  }, [sectors]);

  if (error) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  if (!data || sectors.length === 0) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-text-tertiary">
          No sector data available
        </p>
      </div>
    );
  }

  const scaleX = (v: number) =>
    MARGIN.left + ((v - layout!.pMin) / layout!.pRange) * INNER_W;
  const scaleY = (v: number) =>
    MARGIN.top + INNER_H - ((v - layout!.cMin) / layout!.cRange) * INNER_H;
  const scaleR = (count: number) => {
    const t = layout!.cRange > 0 ? (count - layout!.cMin) / layout!.cRange : 0.5;
    return 16 + t * 44;
  };

  const medXpx = scaleX(layout!.medP);
  const medYpx = scaleY(layout!.medC);

  const quadrantLabels = [
    { x: MARGIN.left + 4, y: MARGIN.top + INNER_H - 4, label: "Quiet" },
    { x: MARGIN.left + INNER_W - 4, y: MARGIN.top + INNER_H - 4, label: "Pressure Building", anchor: "end" as const },
    { x: MARGIN.left + 4, y: MARGIN.top + 12, label: "Active" },
    { x: MARGIN.left + INNER_W - 4, y: MARGIN.top + 12, label: "Hot Zone", anchor: "end" as const },
  ];

  const expandedData = expandedSector
    ? sectors.find((s) => s.name === expandedSector)
    : null;

  return (
    <div>
      <SectionHeader />
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        className="w-full"
        style={{ maxHeight: 340 }}
      >
        {/* Axes */}
        <line
          x1={MARGIN.left}
          y1={MARGIN.top + INNER_H}
          x2={MARGIN.left + INNER_W}
          y2={MARGIN.top + INNER_H}
          stroke="var(--bg-border)"
          strokeOpacity={0.5}
        />
        <line
          x1={MARGIN.left}
          y1={MARGIN.top}
          x2={MARGIN.left}
          y2={MARGIN.top + INNER_H}
          stroke="var(--bg-border)"
          strokeOpacity={0.5}
        />

        {/* Axis labels */}
        <text
          x={MARGIN.left + INNER_W / 2}
          y={HEIGHT - 4}
          textAnchor="middle"
          className="font-mono"
          style={{ fontSize: 9, fill: "var(--text-tertiary)" }}
        >
          Narrative Pressure
        </text>
        <text
          x={12}
          y={MARGIN.top + INNER_H / 2}
          textAnchor="middle"
          className="font-mono"
          style={{ fontSize: 9, fill: "var(--text-tertiary)" }}
          transform={`rotate(-90, 12, ${MARGIN.top + INNER_H / 2})`}
        >
          Active Narratives
        </text>

        {/* Quadrant lines */}
        {sectors.length >= 3 && (
          <>
            <line
              x1={medXpx}
              y1={MARGIN.top}
              x2={medXpx}
              y2={MARGIN.top + INNER_H}
              stroke="var(--bg-border)"
              strokeOpacity={0.35}
              strokeDasharray="4 4"
            />
            <line
              x1={MARGIN.left}
              y1={medYpx}
              x2={MARGIN.left + INNER_W}
              y2={medYpx}
              stroke="var(--bg-border)"
              strokeOpacity={0.35}
              strokeDasharray="4 4"
            />
            {quadrantLabels.map((q) => (
              <text
                key={q.label}
                x={q.x}
                y={q.y}
                textAnchor={q.anchor ?? "start"}
                className="font-mono"
                style={{ fontSize: 8, fill: "var(--text-disabled)", pointerEvents: "none" }}
              >
                {q.label}
              </text>
            ))}
          </>
        )}

        {/* Bubbles */}
        {sectors.map((s) => {
          const cx = scaleX(s.weighted_pressure);
          const cy = scaleY(s.narrative_count);
          const r = scaleR(s.narrative_count);
          const t = layout!.pRange > 0
            ? (s.weighted_pressure - layout!.pMin) / layout!.pRange
            : 0.5;
          const isHovered = hoveredSector === s.name;
          const isExpanded = expandedSector === s.name;

          return (
            <g key={s.name}>
              <circle
                cx={cx}
                cy={cy}
                r={r}
                fill={pressureColor(t)}
                opacity={isHovered || isExpanded ? 1 : 0.75}
                stroke={isHovered || isExpanded ? COLORS.accent : "transparent"}
                strokeWidth={isHovered || isExpanded ? 2 : 0}
                style={{ cursor: "pointer", transition: "all 0.15s ease" }}
                onMouseEnter={() => setHoveredSector(s.name)}
                onMouseLeave={() => setHoveredSector(null)}
                onClick={() =>
                  setExpandedSector(expandedSector === s.name ? null : s.name)
                }
              />
              <text
                x={cx}
                y={cy - r - 4}
                textAnchor="middle"
                className="font-mono"
                style={{
                  fontSize: 10,
                  fill: "var(--text-secondary)",
                  pointerEvents: "none",
                }}
              >
                {s.name}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Hover tooltip */}
      {hoveredSector && !expandedSector && (() => {
        const s = sectors.find((sec) => sec.name === hoveredSector);
        if (!s) return null;
        return (
          <div
            className="font-mono text-[11px] mt-2 px-3 py-2"
            style={{
              background: "var(--bg-surface)",
              border: "1px solid var(--bg-border)",
              borderRadius: 3,
            }}
          >
            <div className="font-medium text-text-primary mb-1">
              {s.name} &mdash; {s.narrative_count} narratives, pressure{" "}
              {s.weighted_pressure.toFixed(1)}
            </div>
            <div className="text-text-tertiary">
              Top tickers:{" "}
              {s.top_assets.slice(0, 5).map((a) => a.ticker).join(", ") || "none"}
            </div>
          </div>
        );
      })()}

      {/* Expanded detail list */}
      {expandedData && (
        <div
          className="mt-3 px-3 py-2"
          style={{
            background: "var(--bg-surface)",
            border: "1px solid var(--bg-border)",
            borderRadius: 3,
          }}
        >
          <div className="flex items-center justify-between mb-2">
            <span className="font-mono text-[12px] font-medium text-text-primary">
              {expandedData.name} &mdash; Contributing Narratives
            </span>
            <button
              className="font-mono text-[10px] text-text-tertiary hover:text-text-primary"
              onClick={() => setExpandedSector(null)}
            >
              close
            </button>
          </div>
          {expandedData.contributing_narratives.map((n) => (
            <div
              key={n.narrative_id}
              className="flex items-center gap-2 py-1"
              style={{ borderTop: "1px solid rgba(56,62,71,0.2)" }}
            >
              <span className="font-mono text-[11px] text-text-primary flex-1 truncate">
                {n.name}
              </span>
              <StageBadge stage={n.stage} />
              <span className="font-mono text-[10px] text-text-tertiary">
                ns {n.ns_score.toFixed(2)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
