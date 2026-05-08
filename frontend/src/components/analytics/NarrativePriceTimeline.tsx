"use client";

import { useEffect, useState, useMemo } from "react";
import {
  fetchNarrativeHistories,
  type AnalyticsHistoriesResponse,
} from "@/lib/api";
import { parseDays } from "@/components/analytics/GlobalTimeRange";
import { COLORS } from "@/lib/colors";

type Props = {
  timeRange: string;
};

type TimelineEvent = {
  date: string;
  dayIndex: number;
  narrativeId: string;
  narrativeName: string;
  eventType: "detected" | "stage" | "surge" | "correlation";
  label: string;
};

const EVENT_STYLES: Record<string, { icon: string; color: string; label: string }> = {
  detected: { icon: "\u25C7", color: COLORS.accent, label: "Detected" },
  stage: { icon: "\u25CF", color: COLORS.bullish, label: "Stage Change" },
  surge: { icon: "\u2605", color: COLORS.alert, label: "SURGE" },
  correlation: { icon: "\u25C6", color: COLORS.purple, label: "Price Impact" },
};

function deriveEvents(data: AnalyticsHistoriesResponse): TimelineEvent[] {
  const events: TimelineEvent[] = [];
  for (const [nid, nh] of Object.entries(data.narratives)) {
    const history = nh.history;
    let detectedIdx = -1;

    // Find detection: first non-null, non-gap-filled snapshot
    for (let i = 0; i < history.length; i++) {
      if (history[i].velocity !== null && !history[i].gap_filled) {
        detectedIdx = i;
        break;
      }
    }
    // Fallback: first non-null snapshot
    if (detectedIdx === -1) {
      for (let i = 0; i < history.length; i++) {
        if (history[i].velocity !== null) {
          detectedIdx = i;
          break;
        }
      }
    }

    if (detectedIdx >= 0) {
      events.push({
        date: history[detectedIdx].date,
        dayIndex: detectedIdx,
        narrativeId: nid,
        narrativeName: nh.name,
        eventType: "detected",
        label: `${nh.name} detected`,
      });
    }

    // Stage changes: compare adjacent non-null snapshots
    for (let i = 0; i < history.length; i++) {
      const snap = history[i];
      if (snap.velocity === null) continue;

      // Infer stage from snapshot context — use parent narrative stage for last entry,
      // detect transitions via velocity/burst patterns
      // Since snapshots don't have stage directly, we detect surges via burst_ratio
      if (snap.burst_ratio !== null && snap.burst_ratio >= 3.0) {
        events.push({
          date: snap.date,
          dayIndex: i,
          narrativeId: nid,
          narrativeName: nh.name,
          eventType: "surge",
          label: `${nh.name} SURGE (burst: ${snap.burst_ratio.toFixed(1)}x)`,
        });
      }
    }
  }

  return events;
}

export default function NarrativePriceTimeline({ timeRange }: Props) {
  const [data, setData] = useState<AnalyticsHistoriesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hoveredEvent, setHoveredEvent] = useState<TimelineEvent | null>(null);

  const days = parseDays(timeRange);

  useEffect(() => {
    setError(null);
    fetchNarrativeHistories(days)
      .then(setData)
      .catch(() => setError("Failed to load narrative histories"));
  }, [days]);

  const events = useMemo(() => (data ? deriveEvents(data) : []), [data]);

  // Generate day markers
  const dayMarkers = useMemo(() => {
    const markers: { day: number; label: string }[] = [];
    const step = days <= 14 ? 2 : days <= 30 ? 5 : days <= 60 ? 10 : 15;
    for (let d = 0; d <= days; d += step) {
      markers.push({
        day: d,
        label: d === days ? "Today" : `-${days - d}d`,
      });
    }
    return markers;
  }, [days]);

  if (error) {
    return (
      <div>
        <SectionHeader timeRange={timeRange} />
        <p className="font-mono text-[12px] text-bearish">{error}</p>
      </div>
    );
  }

  return (
    <div>
      <SectionHeader timeRange={timeRange} />

      {/* Legend */}
      <div className="flex gap-4 mb-3">
        {Object.entries(EVENT_STYLES).map(([key, style]) => (
          <div key={key} className="flex items-center gap-1">
            <span style={{ color: style.color, fontSize: 12 }}>{style.icon}</span>
            <span className="font-mono text-[10px] text-text-tertiary">{style.label}</span>
          </div>
        ))}
      </div>

      {/* Timeline */}
      <div
        className="relative"
        style={{
          height: 80,
          borderLeft: "1px solid rgba(56,62,71,0.27)",
          borderBottom: "1px solid rgba(56,62,71,0.27)",
        }}
      >
        {/* Vertical grid lines */}
        {dayMarkers.map((m) => (
          <div key={m.day}>
            {m.day > 0 && m.day < days && (
              <div
                className="absolute top-0 bottom-0"
                style={{
                  left: `${(m.day / days) * 100}%`,
                  width: 1,
                  background: "rgba(56,62,71,0.13)",
                }}
              />
            )}
          </div>
        ))}

        {/* Event markers */}
        {events.map((ev, i) => {
          const leftPct = (ev.dayIndex / days) * 100;
          const style = EVENT_STYLES[ev.eventType];
          const isHovered = hoveredEvent === ev;

          return (
            <div
              key={`${ev.narrativeId}-${ev.eventType}-${i}`}
              className="absolute cursor-pointer"
              style={{
                left: `${leftPct}%`,
                top: "50%",
                transform: "translate(-50%, -50%)",
                zIndex: isHovered ? 10 : 1,
              }}
              onMouseEnter={() => setHoveredEvent(ev)}
              onMouseLeave={() => setHoveredEvent(null)}
            >
              <span
                style={{
                  fontSize: ev.eventType === "surge" ? 16 : 12,
                  color: style.color,
                  filter: isHovered ? `drop-shadow(0 0 4px ${style.color})` : "none",
                  transition: "filter 0.15s ease",
                }}
              >
                {style.icon}
              </span>

              {/* Tooltip */}
              {isHovered && (
                <div
                  className="absolute left-1/2 -translate-x-1/2 whitespace-nowrap pointer-events-none"
                  style={{
                    bottom: "calc(100% + 8px)",
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
                  {ev.date} &middot; {ev.label}
                </div>
              )}
            </div>
          );
        })}

        {/* Day marker labels */}
        {dayMarkers.map((m) => (
          <div
            key={`label-${m.day}`}
            className="absolute font-mono text-[9px] text-text-tertiary"
            style={{
              left: `${(m.day / days) * 100}%`,
              bottom: -20,
              transform: "translateX(-50%)",
            }}
          >
            {m.label}
          </div>
        ))}
      </div>

      {/* Spacer for day labels */}
      <div className="h-7" />
    </div>
  );
}

function SectionHeader({ timeRange }: { timeRange: string }) {
  return (
    <div className="flex items-baseline gap-3 mb-4">
      <h2 className="text-[13px] font-semibold text-text-secondary uppercase tracking-[0.06em] m-0">
        Narrative &rarr; Price Timeline
      </h2>
      <span className="font-mono text-[11px] text-text-tertiary">
        {timeRange} detection and impact events
      </span>
    </div>
  );
}
