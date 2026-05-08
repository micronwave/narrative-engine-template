"use client";

import { useEffect, useState, useMemo } from "react";
import {
  fetchContrarianSignals,
  type AnalyticsContrarianSignal,
} from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";
import StageBadge from "@/components/common/StageBadge";
import SegmentedControl from "@/components/common/SegmentedControl";
import { COLORS } from "@/lib/colors";

type Props = { timeRange: string };

function SectionHeader({
  sortBy,
  onSortChange,
  hasSignals,
}: {
  sortBy: string;
  onSortChange: (v: string) => void;
  hasSignals: boolean;
}) {
  return (
    <div className="flex items-baseline justify-between mb-4">
      <div className="flex items-baseline gap-3">
        <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
          Contrarian Signals
        </h2>
        <span className="font-mono text-[11px] text-text-tertiary">
          coordination flags as intelligence
        </span>
      </div>
      {hasSignals && (
        <SegmentedControl
          options={["Confidence", "Recency", "Velocity"]}
          activeOption={sortBy}
          onChange={onSortChange}
        />
      )}
    </div>
  );
}

function maxConfidence(signal: AnalyticsContrarianSignal): number {
  if (!signal.coordination_events || signal.coordination_events.length === 0)
    return 0;
  return Math.max(...signal.coordination_events.map((e) => e.similarity_score));
}

function mostRecentDetection(signal: AnalyticsContrarianSignal): string {
  if (!signal.coordination_events || signal.coordination_events.length === 0)
    return "";
  const dates = signal.coordination_events
    .map((e) => e.detected_at)
    .filter(Boolean) as string[];
  if (dates.length === 0) return "";
  return dates.sort().reverse()[0];
}

export default function ContrarianSignalCards({ timeRange }: Props) {
  const [data, setData] = useState<AnalyticsContrarianSignal[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState("Confidence");

  const days = parseDays(timeRange);

  useEffect(() => {
    setError(null);
    fetchContrarianSignals(days)
      .then((r) => setData(r.signals ?? []))
      .catch(() => setError("Failed to load contrarian signals"));
  }, [days]);

  const sorted = useMemo(() => {
    const copy = [...data];
    switch (sortBy) {
      case "Confidence":
        return copy.sort((a, b) => maxConfidence(b) - maxConfidence(a));
      case "Recency":
        return copy.sort(
          (a, b) =>
            mostRecentDetection(b).localeCompare(mostRecentDetection(a))
        );
      case "Velocity":
        return copy.sort((a, b) => b.velocity_now - a.velocity_now);
      default:
        return copy;
    }
  }, [data, sortBy]);

  if (error) {
    return (
      <div>
        <SectionHeader sortBy={sortBy} onSortChange={setSortBy} hasSignals={false} />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  if (sorted.length === 0) {
    return (
      <div>
        <SectionHeader sortBy={sortBy} onSortChange={setSortBy} hasSignals={false} />
        <p className="font-mono text-[12px] text-text-tertiary">
          No active coordination flags
        </p>
      </div>
    );
  }

  return (
    <div>
      <SectionHeader sortBy={sortBy} onSortChange={setSortBy} hasSignals />

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {sorted.map((signal) => {
          const conf = maxConfidence(signal);
          const confColor = conf >= 0.7 ? "var(--intent-danger)" : "var(--intent-warning)";
          const recentDate = mostRecentDetection(signal);

          return (
            <div
              key={signal.narrative_id}
              style={{
                background: "var(--bg-surface)",
                border: "1px solid var(--bg-border)",
                borderRadius: 0,
                padding: "12px 14px",
              }}
            >
              {/* Header */}
              <div className="flex items-center gap-2 mb-3">
                <span className="font-mono text-[13px] font-medium text-text-primary flex-1 truncate">
                  {signal.name}
                </span>
                <StageBadge stage={signal.stage} />
                <span
                  className="font-mono text-[10px] px-1.5 py-[1px]"
                  style={{
                    background: "var(--accent-primary-muted)",
                    color: "var(--accent-primary-text)",
                    borderRadius: 2,
                  }}
                >
                  {signal.ns_score.toFixed(2)}
                </span>
              </div>

              {/* Confidence bar */}
              <div className="mb-3">
                <div className="flex items-center justify-between mb-1">
                  <span className="font-mono text-[10px] text-text-tertiary">
                    Coordination confidence
                  </span>
                  <span
                    className="font-mono text-[10px]"
                    style={{ color: confColor }}
                  >
                    {(conf * 100).toFixed(0)}%
                  </span>
                </div>
                <div
                  style={{
                    height: 4,
                    background: "var(--bg-surface-hover)",
                    borderRadius: 2,
                    overflow: "hidden",
                  }}
                >
                  <div
                    style={{
                      width: `${conf * 100}%`,
                      height: "100%",
                      background: confColor,
                      borderRadius: 2,
                      transition: "width 0.2s ease",
                    }}
                  />
                </div>
              </div>

              {/* Velocity comparison */}
              <div className="flex items-center gap-4 mb-3">
                <div>
                  <div className="font-mono text-[9px] text-text-tertiary mb-0.5">
                    At detection
                  </div>
                  <div className="font-mono text-[12px] text-text-primary">
                    {signal.velocity_at_detection.toFixed(3)}
                  </div>
                </div>
                <div className="font-mono text-[11px] text-text-disabled">&rarr;</div>
                <div>
                  <div className="font-mono text-[9px] text-text-tertiary mb-0.5">
                    Now
                  </div>
                  <div className="font-mono text-[12px] text-text-primary">
                    {signal.velocity_now.toFixed(3)}
                  </div>
                </div>
                <span
                  className="font-mono text-[10px] px-1.5 py-[1px] ml-auto"
                  style={{
                    background: signal.velocity_sustained
                      ? "var(--bullish-bg)"
                      : "var(--bearish-bg)",
                    color: signal.velocity_sustained
                      ? COLORS.bullish
                      : COLORS.bearish,
                    borderRadius: 2,
                  }}
                >
                  {signal.velocity_sustained ? "Sustained" : "Faded"}
                </span>
              </div>

              {/* Coordination events count + date */}
              {signal.coordination_events.length > 0 && (
                <div className="font-mono text-[10px] text-text-tertiary mb-3">
                  {signal.coordination_events.length} coordination event
                  {signal.coordination_events.length !== 1 ? "s" : ""}
                  {recentDate && (
                    <>
                      {" "}&middot; latest{" "}
                      {recentDate.slice(0, 10)}
                    </>
                  )}
                </div>
              )}

              {/* Linked assets */}
              {signal.linked_assets.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {signal.linked_assets.slice(0, 6).map((a) => {
                    const pctColor =
                      a.price_change_pct === null
                        ? "var(--text-muted)"
                        : a.price_change_pct >= 0
                        ? COLORS.bullish
                        : COLORS.bearish;
                    return (
                      <span
                        key={a.ticker}
                        className="font-mono text-[10px] px-1.5 py-[1px]"
                        style={{
                          background: "var(--bg-surface-hover)",
                          borderRadius: 2,
                          color: pctColor,
                        }}
                      >
                        {a.ticker}
                        {a.price_change_pct !== null && (
                          <> {a.price_change_pct >= 0 ? "+" : ""}
                            {a.price_change_pct.toFixed(1)}%</>
                        )}
                      </span>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
