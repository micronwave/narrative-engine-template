"use client";

import { useEffect, useState, useMemo } from "react";
import {
  fetchLeadTimeDistribution,
  type AnalyticsLeadTimeResponse,
  type AnalyticsLeadTimeDataPoint,
} from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";
import SegmentedControl from "@/components/common/SegmentedControl";
import { COLORS } from "@/lib/colors";

type Props = { timeRange: string };

const THRESHOLD_OPTIONS = ["1%", "2%", "5%"];
const THRESHOLD_MAP: Record<string, number> = { "1%": 1.0, "2%": 2.0, "5%": 5.0 };

const SVG_W = 400;
const SVG_H = 200;
const MARGIN = { top: 12, right: 16, bottom: 32, left: 36 };
const INNER_W = SVG_W - MARGIN.left - MARGIN.right;
const INNER_H = SVG_H - MARGIN.top - MARGIN.bottom;

const BAR_COLORS = [
  COLORS.bullish,  // 0-1 days
  COLORS.bullish,  // 2-3 days
  COLORS.accent,   // 4-7 days
  COLORS.accent,   // 8-14 days
  COLORS.muted,    // 15-30 days
  "#4B5060",       // No move
];

function SectionHeader({
  threshold,
  onThresholdChange,
}: {
  threshold: string;
  onThresholdChange: (v: string) => void;
}) {
  return (
    <div className="flex items-baseline justify-between mb-4">
      <div className="flex items-baseline gap-3">
        <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
          Lead Time Distribution
        </h2>
        <span className="font-mono text-[11px] text-text-tertiary">
          narrative → price lag
        </span>
      </div>
      <SegmentedControl
        options={THRESHOLD_OPTIONS}
        activeOption={threshold}
        onChange={onThresholdChange}
      />
    </div>
  );
}

export default function LeadTimeHistogram({ timeRange }: Props) {
  const [data, setData] = useState<AnalyticsLeadTimeResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [threshold, setThreshold] = useState("2%");
  const [hoveredBar, setHoveredBar] = useState<number | null>(null);
  const [expandedBucket, setExpandedBucket] = useState<number | null>(null);

  const days = parseDays(timeRange);
  const thresholdNum = THRESHOLD_MAP[threshold] ?? 2.0;

  useEffect(() => {
    setError(null);
    setExpandedBucket(null);
    fetchLeadTimeDistribution(days, thresholdNum)
      .then(setData)
      .catch(() => setError("Failed to load lead time data"));
  }, [days, thresholdNum]);

  const buckets = data?.histogram_buckets ?? [];
  const maxCount = useMemo(
    () => Math.max(...buckets.map((b) => b.count), 1),
    [buckets]
  );
  const totalCount = useMemo(
    () => buckets.reduce((a, b) => a + b.count, 0),
    [buckets]
  );

  // CDF points
  const cdfPoints = useMemo(() => {
    if (buckets.length === 0 || totalCount === 0) return [];
    let cumulative = 0;
    return buckets.map((b, i) => {
      cumulative += b.count;
      return { x: i, y: cumulative / totalCount };
    });
  }, [buckets, totalCount]);

  // Filtered data points for expanded bucket
  const expandedPoints = useMemo((): AnalyticsLeadTimeDataPoint[] => {
    if (expandedBucket === null || !data?.data_points || !buckets[expandedBucket])
      return [];
    const range = buckets[expandedBucket].range;
    if (range === "No move") {
      return data.data_points.filter((dp) => dp.lead_days === null);
    }
    const match = range.match(/(\d+)-?(\d+)?/);
    if (!match) return [];
    const lo = parseInt(match[1], 10);
    const hi = match[2] ? parseInt(match[2], 10) : lo;
    return data.data_points.filter(
      (dp) => dp.lead_days !== null && dp.lead_days >= lo && dp.lead_days <= hi
    );
  }, [expandedBucket, data, buckets]);

  if (error) {
    return (
      <div>
        <SectionHeader threshold={threshold} onThresholdChange={setThreshold} />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  if (!data || buckets.length === 0 || totalCount === 0) {
    return (
      <div>
        <SectionHeader threshold={threshold} onThresholdChange={setThreshold} />
        <p className="font-mono text-[12px] text-text-tertiary">
          No lead time data available
        </p>
      </div>
    );
  }

  const barWidth = INNER_W / buckets.length;
  const barPad = 4;
  const noMoveGap = 8;

  return (
    <div>
      <SectionHeader threshold={threshold} onThresholdChange={setThreshold} />

      {/* Hero stats */}
      <div className="grid grid-cols-3 gap-4 mb-4">
        <div className="text-center">
          <div className="font-mono text-[22px] font-semibold text-text-primary">
            {data.median_lead_days}
          </div>
          <div className="font-mono text-[10px] text-text-tertiary">
            Median (days)
          </div>
        </div>
        <div className="text-center">
          <div className="font-mono text-[22px] font-semibold text-text-primary">
            {data.mean_lead_days}
          </div>
          <div className="font-mono text-[10px] text-text-tertiary">
            Mean (days)
          </div>
        </div>
        <div className="text-center">
          <div className="font-mono text-[22px] font-semibold text-text-primary">
            {(data.hit_rate * 100).toFixed(0)}%
          </div>
          <div className="font-mono text-[10px] text-text-tertiary">
            Hit Rate
          </div>
        </div>
      </div>

      {/* Chart */}
      <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} className="w-full" style={{ maxHeight: 200 }}>
        {/* Y-axis */}
        <line
          x1={MARGIN.left}
          y1={MARGIN.top}
          x2={MARGIN.left}
          y2={MARGIN.top + INNER_H}
          stroke="var(--bg-border)"
          strokeOpacity={0.5}
        />
        {/* X-axis */}
        <line
          x1={MARGIN.left}
          y1={MARGIN.top + INNER_H}
          x2={MARGIN.left + INNER_W}
          y2={MARGIN.top + INNER_H}
          stroke="var(--bg-border)"
          strokeOpacity={0.5}
        />

        {/* Y-axis ticks */}
        {[0, 0.25, 0.5, 0.75, 1].map((t) => {
          const y = MARGIN.top + INNER_H - t * INNER_H;
          const val = Math.round(t * maxCount);
          return (
            <g key={t}>
              <line
                x1={MARGIN.left - 3}
                y1={y}
                x2={MARGIN.left}
                y2={y}
                stroke="var(--bg-border)"
                strokeOpacity={0.5}
              />
              <text
                x={MARGIN.left - 6}
                y={y}
                textAnchor="end"
                dominantBaseline="central"
                className="font-mono"
                style={{ fontSize: 8, fill: "var(--text-tertiary)" }}
              >
                {val}
              </text>
            </g>
          );
        })}

        {/* Bars */}
        {buckets.map((b, i) => {
          const isNoMove = b.range === "No move";
          const xOffset = isNoMove ? noMoveGap : 0;
          const x = MARGIN.left + i * barWidth + barPad / 2 + xOffset;
          const w = barWidth - barPad - (isNoMove ? noMoveGap : 0);
          const h = (b.count / maxCount) * INNER_H;
          const y = MARGIN.top + INNER_H - h;
          const isHovered = hoveredBar === i;
          const isExpanded = expandedBucket === i;

          return (
            <g key={i}>
              <rect
                x={x}
                y={y}
                width={Math.max(w, 0)}
                height={h}
                fill={BAR_COLORS[i] ?? COLORS.muted}
                opacity={isHovered || isExpanded ? 1 : 0.75}
                rx={1}
                style={{ cursor: "pointer", transition: "opacity 0.15s ease" }}
                onMouseEnter={() => setHoveredBar(i)}
                onMouseLeave={() => setHoveredBar(null)}
                onClick={() =>
                  setExpandedBucket(expandedBucket === i ? null : i)
                }
              />
              {/* X-axis label */}
              <text
                x={x + Math.max(w, 0) / 2}
                y={MARGIN.top + INNER_H + 12}
                textAnchor="middle"
                className="font-mono"
                style={{ fontSize: 8, fill: "var(--text-tertiary)" }}
              >
                {b.range}
              </text>
              {/* Count label on hover */}
              {isHovered && b.count > 0 && (
                <text
                  x={x + Math.max(w, 0) / 2}
                  y={y - 4}
                  textAnchor="middle"
                  className="font-mono"
                  style={{ fontSize: 9, fill: "var(--text-primary)" }}
                >
                  {b.count} ({totalCount > 0 ? ((b.count / totalCount) * 100).toFixed(0) : 0}%)
                </text>
              )}
            </g>
          );
        })}

        {/* CDF overlay */}
        {cdfPoints.length > 1 && (
          <path
            d={cdfPoints
              .map((p, i) => {
                const x =
                  MARGIN.left + p.x * barWidth + barWidth / 2;
                const y = MARGIN.top + INNER_H - p.y * INNER_H;
                return `${i === 0 ? "M" : "L"} ${x} ${y}`;
              })
              .join(" ")}
            fill="none"
            stroke={COLORS.purple}
            strokeWidth={1.5}
            strokeOpacity={0.6}
          />
        )}

        {/* CDF right-axis label */}
        <text
          x={SVG_W - 4}
          y={MARGIN.top + 8}
          textAnchor="end"
          className="font-mono"
          style={{ fontSize: 8, fill: COLORS.purple, opacity: 0.6 }}
        >
          cumulative %
        </text>
      </svg>

      {/* Expanded bucket detail */}
      {expandedBucket !== null && expandedPoints.length > 0 && (
        <div
          className="mt-3 px-3 py-2"
          style={{
            background: "var(--bg-surface)",
            border: "1px solid var(--bg-border)",
            borderRadius: 3,
          }}
        >
          <div className="flex items-center justify-between mb-2">
            <span className="font-mono text-[11px] font-medium text-text-primary">
              {buckets[expandedBucket]?.range} &mdash; {expandedPoints.length}{" "}
              pairs
            </span>
            <button
              className="font-mono text-[10px] text-text-tertiary hover:text-text-primary"
              onClick={() => setExpandedBucket(null)}
            >
              close
            </button>
          </div>
          <div
            className="grid gap-[2px]"
            style={{ gridTemplateColumns: "1fr 60px 60px", maxHeight: 150, overflowY: "auto" }}
          >
            <span className="font-mono text-[9px] text-text-disabled">
              Ticker
            </span>
            <span className="font-mono text-[9px] text-text-disabled text-right">
              Lead
            </span>
            <span className="font-mono text-[9px] text-text-disabled text-right">
              Move
            </span>
            {expandedPoints.slice(0, 20).map((dp, i) => (
              <div key={i} className="contents">
                <span className="font-mono text-[10px] text-text-secondary">
                  {dp.ticker}
                </span>
                <span className="font-mono text-[10px] text-text-primary text-right">
                  {dp.lead_days !== null ? `${dp.lead_days}d` : "—"}
                </span>
                <span
                  className="font-mono text-[10px] text-right"
                  style={{
                    color:
                      dp.price_change_pct === null
                        ? "var(--text-muted)"
                        : dp.price_change_pct >= 0
                        ? COLORS.bullish
                        : COLORS.bearish,
                  }}
                >
                  {dp.price_change_pct !== null
                    ? `${dp.price_change_pct >= 0 ? "+" : ""}${dp.price_change_pct.toFixed(1)}%`
                    : "—"}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
