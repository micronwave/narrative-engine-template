"use client";

import { useEffect, useState, useMemo } from "react";
import {
  fetchMomentumLeaderboard,
  fetchNarrativeHistories,
  fetchNarrativeOverlap,
  type AnalyticsMomentumEntry,
  type AnalyticsHistoriesResponse,
  type AnalyticsOverlapResponse,
} from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";
import StageBadge from "@/components/common/StageBadge";
import Sparkline from "@/components/common/Sparkline";
import { COLORS } from "@/lib/colors";

type Props = {
  timeRange: string;
};

function MomentumArrowInline({ score, direction }: { score: number; direction: string }) {
  const isUp = direction === "accelerating";
  const isDown = direction === "decelerating";
  const color = isUp ? COLORS.bullish : isDown ? COLORS.bearish : COLORS.muted;
  const rotation = isUp ? -45 : isDown ? 45 : 0;
  const size = Math.min(Math.abs(score) * 60 + 12, 24);

  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke={color}
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      style={{ display: "block", transform: `rotate(${rotation}deg)`, transition: "transform 0.12s ease" }}
    >
      <path d="M5 12h14M13 6l6 6-6 6" />
    </svg>
  );
}

export default function MomentumLeaderboard({ timeRange }: Props) {
  const [leaderboard, setLeaderboard] = useState<AnalyticsMomentumEntry[]>([]);
  const [histories, setHistories] = useState<AnalyticsHistoriesResponse | null>(null);
  const [overlap, setOverlap] = useState<AnalyticsOverlapResponse | null>(null);
  const [hoveredRow, setHoveredRow] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const days = parseDays(timeRange);

  useEffect(() => {
    setError(null);
    fetchMomentumLeaderboard(days)
      .then((r) => setLeaderboard(r.leaderboard))
      .catch(() => setError("Failed to load momentum data"));
  }, [days]);

  useEffect(() => {
    fetchNarrativeHistories(days)
      .then(setHistories)
      .catch(() => {});
    fetchNarrativeOverlap(days)
      .then(setOverlap)
      .catch(() => {});
  }, [days]);

  // Build ns_score lookup from overlap data
  const nsScoreMap = useMemo(() => {
    const map: Record<string, number> = {};
    if (overlap?.narratives) {
      for (const n of overlap.narratives) {
        map[n.id] = n.ns_score;
      }
    }
    return map;
  }, [overlap]);

  // Build sparkline data from histories (last 7 velocity values)
  const sparklineMap = useMemo(() => {
    const map: Record<string, number[]> = {};
    if (histories?.narratives) {
      for (const [nid, nh] of Object.entries(histories.narratives)) {
        const velocities = nh.history
          .slice(-7)
          .map((s) => s.velocity)
          .filter((v): v is number => v !== null);
        if (velocities.length >= 2) {
          map[nid] = velocities;
        }
      }
    }
    return map;
  }, [histories]);

  if (error) {
    return (
      <div>
        <SectionHeader />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  return (
    <div>
      <SectionHeader />

      {/* Header row */}
      <div
        className="grid gap-2 px-2 pb-2"
        style={{ gridTemplateColumns: "32px 1fr 100px 64px 100px 180px" }}
      >
        {["#", "Narrative", "Velocity", "NS", "Momentum", "Linked Assets"].map((h) => (
          <span
            key={h}
            className="font-mono text-[10px] text-text-tertiary uppercase tracking-[0.05em]"
          >
            {h}
          </span>
        ))}
      </div>

      {/* Data rows */}
      {leaderboard.map((entry, idx) => {
        const nsScore = nsScoreMap[entry.narrative_id];
        const sparkData = sparklineMap[entry.narrative_id];
        const isSurge = entry.burst_active;
        const isHovered = hoveredRow === entry.narrative_id;

        return (
          <div
            key={entry.narrative_id}
            className="grid gap-2 px-2 items-center cursor-pointer"
            style={{
              gridTemplateColumns: "32px 1fr 100px 64px 100px 180px",
              padding: "10px 8px",
              borderBottom: "1px solid rgba(56,62,71,0.13)",
              borderLeft: isSurge ? "2px solid var(--alert)" : "2px solid transparent",
              background: isHovered
                ? "var(--accent-primary-hover)"
                : isSurge
                  ? "rgba(236,154,60,0.04)"
                  : "transparent",
              transition: "background 0.12s ease",
            }}
            onMouseEnter={() => setHoveredRow(entry.narrative_id)}
            onMouseLeave={() => setHoveredRow(null)}
          >
            {/* Rank */}
            <span className="font-mono text-[13px] font-semibold text-text-primary">
              {idx + 1}
            </span>

            {/* Name + Stage + SURGE */}
            <div className="flex items-center gap-2 min-w-0">
              <span className="text-[13px] font-medium text-text-primary truncate">
                {entry.name}
              </span>
              <StageBadge stage={entry.stage} />
              {isSurge && (
                <span className="font-mono text-[10px] font-bold text-alert bg-alert-bg px-1.5 py-[1px] rounded-sm shrink-0">
                  SURGE
                </span>
              )}
            </div>

            {/* Velocity + Sparkline */}
            <div className="flex items-center gap-1.5">
              <span className="font-mono text-[12px] text-text-primary">
                {entry.current_velocity.toFixed(2)}
              </span>
              {sparkData && (
                <Sparkline data={sparkData} width={48} height={16} />
              )}
            </div>

            {/* NS Score */}
            <span
              className="font-mono text-[12px]"
              style={{
                color: nsScore != null && nsScore > 0.8 ? "var(--text-primary)" : "var(--text-secondary)",
                fontWeight: nsScore != null && nsScore > 0.8 ? 600 : 400,
              }}
            >
              {nsScore != null ? nsScore.toFixed(2) : "—"}
            </span>

            {/* Momentum arrow + value */}
            <div className="flex items-center gap-1.5">
              <MomentumArrowInline score={entry.momentum_score} direction={entry.slope_direction} />
              <span
                className="font-mono text-[12px]"
                style={{
                  color:
                    entry.slope_direction === "accelerating"
                      ? "var(--bullish)"
                      : entry.slope_direction === "decelerating"
                        ? "var(--bearish)"
                        : "var(--text-muted)",
                }}
              >
                {entry.momentum_score > 0 ? "+" : ""}
                {entry.momentum_score.toFixed(2)}
              </span>
            </div>

            {/* Linked Assets */}
            <div className="flex flex-wrap gap-[3px]">
              {entry.linked_assets.slice(0, 3).map((ticker) => (
                <span
                  key={ticker}
                  className="inline-block font-mono text-[11px] text-text-secondary bg-surface-hover px-1.5 py-[2px] rounded-sm"
                >
                  {ticker}
                </span>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function SectionHeader() {
  return (
    <div className="flex items-baseline gap-3 mb-4">
      <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
        Momentum Leaderboard
      </h2>
      <span className="font-mono text-[11px] text-text-tertiary">ranked by acceleration</span>
    </div>
  );
}
