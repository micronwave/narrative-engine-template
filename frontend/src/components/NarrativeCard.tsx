"use client";

import { useState } from "react";
import { Search, ArrowUp, ArrowDown, Minus, Telescope, Radar, Star } from "lucide-react";
import type { Narrative, VisibleNarrative } from "@/lib/api";
import { useWatchlist } from "@/contexts/WatchlistContext";
import VelocitySparkline from "./VelocitySparkline";
import MomentumBar from "./MomentumBar";
import MiniAreaChart from "./MiniAreaChart";
import SignalBadge from "./SignalBadge";

type Props = {
  narrative: Narrative;
  variant?: "hero" | "secondary" | "compact";
  /** For secondary variant: show one-line summary */
  showSummary?: boolean;
  onInvestigateClick?: (id: string) => void;
};

/** Parse numeric velocity from summary string like "+13.6% signal velocity over 7d" */
function parseVelocity(summary: string): number {
  const match = summary.match(/([+-]?\d+\.?\d*)/);
  return match ? parseFloat(match[1]) : 0;
}

/** Semantic velocity color: warm (accelerating), neutral (stable), cool (decelerating) */
function velSemanticColor(summary: string): string {
  const val = parseVelocity(summary);
  if (val >= 5) return "var(--vel-accelerating)";
  if (val < -0.5) return "var(--vel-decelerating)";
  return "var(--vel-stable)";
}

/** Is this an accelerating signal? (for pulse animation) */
function isAccelerating(summary: string): boolean {
  return parseVelocity(summary) >= 5;
}

/** Original velocity color (green/red) — used ONLY in default variant for test safety */
function velocityColor(summary: string): string {
  if (summary.startsWith("+") && !summary.startsWith("+0.0")) return "var(--intent-success)";
  if (summary.startsWith("-")) return "var(--intent-danger)";
  return "var(--text-disabled)";
}

/** Returns stage badge classes for DEFAULT variant (test-critical — do not rename) */
function stageBadgeClass(stage: string): string {
  switch (stage) {
    case "Growing": return "bg-bullish-bg text-bullish";
    case "Mature": return "bg-alert-bg text-alert";
    case "Declining": return "bg-bearish-bg text-bearish";
    case "Dormant": return "bg-inset text-text-disabled";
    default: return "bg-accent-muted text-accent-text"; // Emerging
  }
}

/** Border-only stage badge color for hero/secondary/compact variants */
function stageBorderColor(stage: string): string {
  switch (stage) {
    case "Emerging":
    case "Growing":
      return "var(--vel-accelerating)";
    case "Mature":
      return "var(--vel-stable)";
    default:
      return "var(--vel-decelerating)";
  }
}

/** CTA text varies by signal state */
function ctaText(narrative: VisibleNarrative): { text: string; icon: typeof Search } {
  if (narrative.burst_velocity?.is_burst) return { text: "Monitor", icon: Radar };
  return { text: "Deep Dive", icon: Telescope };
}

/** Direction arrow based on velocity */
function DirectionArrow({ summary }: { summary: string }) {
  const color = velSemanticColor(summary);
  if (summary.startsWith("+") && !summary.startsWith("+0.0"))
    return <ArrowUp size={14} style={{ color }} />;
  if (summary.startsWith("-"))
    return <ArrowDown size={14} style={{ color }} />;
  return <Minus size={12} style={{ color: "var(--text-disabled)" }} />;
}

/** Catalyst type badge (earnings, regulatory, etc.) */
function CatalystTypeBadge({ type }: { type?: string | null }) {
  if (!type || type === "unknown") return null;
  return (
    <span
      data-testid="catalyst-type"
      style={{
        fontSize: 9,
        fontFamily: "var(--font-mono)",
        padding: "1px 5px",
        borderRadius: "var(--radius-badge)",
        border: "1px solid var(--bg-border)",
        color: "var(--text-muted)",
        letterSpacing: "0.3px",
        textTransform: "uppercase",
      }}
    >
      {type}
    </span>
  );
}

/** Relative time from ISO string */
function relativeTime(iso: string): string {
  if (!iso) return "—";
  try {
    const diff = Date.now() - new Date(iso).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "now";
    if (mins < 60) return `${mins}m`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h`;
    const days = Math.floor(hours / 24);
    return `${days}d`;
  } catch {
    return "—";
  }
}

/** Mini 7-bar trend visualization for compact rows */
function MiniTrendBars({ timeseries, color }: { timeseries: { date: string; value: number }[]; color: string }) {
  if (!timeseries || timeseries.length < 2) return null;
  const values = timeseries.slice(-7).map((t) => t.value);
  const max = Math.max(...values) || 1;
  return (
    <div className="flex items-end gap-px" style={{ height: 16, width: 28 }}>
      {values.map((v, i) => (
        <div
          key={i}
          style={{
            width: 3,
            height: Math.max(2, (v / max) * 16),
            background: color,
            opacity: 0.7,
          }}
        />
      ))}
    </div>
  );
}

/* ============================================================
   HERO VARIANT — Two-column featured panel
   ============================================================ */
function HeroCard({
  narrative,
  onInvestigateClick,
}: {
  narrative: VisibleNarrative;
  onInvestigateClick?: (id: string) => void;
}) {
  const { isWatched, toggleWatch } = useWatchlist();
  const watched = isWatched(narrative.id);
  const [expanded, setExpanded] = useState(false);
  const velColor = velSemanticColor(narrative.velocity_summary);
  const { text: ctaLabel, icon: CtaIcon } = ctaText(narrative);
  const accel = isAccelerating(narrative.velocity_summary);

  function handleInvestigate() {
    onInvestigateClick?.(narrative.id);
  }

  // Truncate descriptor to ~2-3 sentences
  const sentences = (narrative.descriptor || "").split(/(?<=[.!?])\s+/).filter(Boolean);
  const shortDesc = sentences.slice(0, 3).join(" ");
  const hasMore = sentences.length > 3;

  const stats = narrative.source_stats;
  const entities = narrative.entity_tags || [];

  return (
    <article
      className="overflow-hidden cursor-pointer transition-all hover:bg-[var(--accent-primary-hover)]"
      style={{ borderLeft: `2px solid ${velColor}`, borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
      onClick={handleInvestigate}
      aria-label={`Narrative: ${narrative.name}`}
      tabIndex={0}
      role="button"
    >
      <div className="grid grid-cols-1 lg:grid-cols-[55%_45%]">
        {/* LEFT COLUMN: context */}
        <div className="p-5 flex flex-col">
          {/* Stage + topics row */}
          <div className="flex items-center gap-3 mb-3">
            <button
              onClick={(e) => { e.stopPropagation(); toggleWatch("narrative", narrative.id); }}
              className="shrink-0"
              style={{ background: "none", border: "none", cursor: "pointer", padding: 0 }}
              aria-label={watched ? "Remove from watchlist" : "Add to watchlist"}
            >
              <Star
                size={16}
                fill={watched ? "var(--intent-warning)" : "none"}
                stroke={watched ? "var(--intent-warning)" : "var(--text-disabled)"}
              />
            </button>
            {narrative.stage && (
              <span
                data-testid="stage-badge"
                className={`font-medium px-1.5 py-0.5 ${stageBadgeClass(narrative.stage)}`}
                style={{
                  fontSize: 10,
                  borderRadius: "var(--radius-badge)",
                  background: "transparent",
                  border: `1px solid ${stageBorderColor(narrative.stage)}`,
                  color: stageBorderColor(narrative.stage),
                }}
              >
                {narrative.stage.toLowerCase()}
              </span>
            )}
            <SignalBadge direction={narrative.signal_direction} confidence={narrative.signal_confidence} />
            <CatalystTypeBadge type={narrative.signal_catalyst_type} />
            {narrative.topic_tags && narrative.topic_tags.length > 0 && (
              <div className="flex gap-2" data-testid="topic-tags">
                {narrative.topic_tags.map((tag) => (
                  <span key={tag} className="label-topic">{tag}</span>
                ))}
              </div>
            )}
          </div>

          {/* Title */}
          <h3
            className="text-text-primary font-semibold leading-tight mb-2"
            style={{ fontSize: 22, fontFamily: "var(--font-display)", letterSpacing: "-0.02em" }}
          >
            {narrative.name}
          </h3>

          {/* Summary — 2-3 sentences */}
          <p className="text-text-muted leading-relaxed mb-3 flex-1" style={{ fontSize: 13 }}>
            {expanded ? narrative.descriptor : shortDesc}
            {hasMore && !expanded && (
              <button
                onClick={(e) => { e.stopPropagation(); setExpanded(true); }}
                className="ml-1 font-medium"
                style={{ color: "var(--vel-accelerating)", fontSize: 12, background: "none", border: "none", cursor: "pointer" }}
              >
                read more
              </button>
            )}
          </p>

          {/* CTA */}
          <button
            onClick={(e) => { e.stopPropagation(); handleInvestigate(); }}
            className="flex items-center gap-2 text-xs font-medium transition-all hover:opacity-80 self-start"
            style={{
              color: "var(--text-muted)",
              background: "none",
              border: `1px solid var(--bg-border)`,
              padding: "6px 14px",
              cursor: "pointer",
              borderRadius: "var(--radius-badge)",
            }}
            aria-label={`Investigate: ${narrative.name}`}
          >
            <CtaIcon size={13} />
            {ctaLabel}
          </button>
        </div>

        {/* RIGHT COLUMN: data */}
        <div className="p-5 flex flex-col" style={{ borderLeft: "1px solid rgba(56, 62, 71, 0.13)" }}>
          {/* Velocity number — the hero */}
          <div className="flex items-center gap-3 mb-3">
            <DirectionArrow summary={narrative.velocity_summary} />
            <span
              className={`font-data-hero ${accel ? "animate-pulse-accent" : ""}`}
              style={{ color: velColor }}
            >
              {narrative.velocity_summary.match(/[+-]?\d+\.?\d*%?/)?.[0] || "0%"}
            </span>
            {narrative.burst_velocity?.is_burst && (
              <span
                data-testid="burst-indicator"
                className="bg-critical-bg text-critical font-medium animate-pulse-accent"
                style={{ fontSize: 10, padding: "2px 6px", borderRadius: "var(--radius-badge)" }}
              >
                SURGE
              </span>
            )}
          </div>
          <div className="label-micro mb-3">7D SIGNAL VELOCITY</div>

          {/* Interactive chart */}
          <div className="mb-3">
            <MiniAreaChart
              data={narrative.velocity_timeseries}
              color={velColor}
              width={220}
              height={90}
            />
          </div>

          {/* Metrics row */}
          <div className="flex items-center gap-5 mb-3">
            <div>
              <div className="font-data-medium" style={{ color: "var(--text-primary)" }}>
                {narrative.signals?.length ?? "—"}
              </div>
              <div className="label-micro">signals</div>
            </div>
            <div>
              <div className="font-data-medium" style={{ color: "var(--text-primary)" }}>
                {narrative.entropy !== null && narrative.entropy !== undefined
                  ? narrative.entropy.toFixed(1)
                  : "—"}
              </div>
              <div className="label-micro">diversity</div>
            </div>
            {narrative.signal_certainty && narrative.signal_certainty !== "speculative" && (
              <div>
                <div className="font-data-medium" style={{ color: "var(--text-primary)" }}>
                  {narrative.signal_certainty}
                </div>
                <div className="label-micro">certainty</div>
              </div>
            )}
          </div>

          {/* Source breakdown */}
          {stats && stats.total > 0 && (
            <div className="mb-3" style={{ fontSize: 10, fontFamily: "var(--font-mono)", color: "var(--text-muted)", letterSpacing: "0.3px" }}>
              {stats.total} DOCS
              {stats.news > 0 && <> · {stats.news} NEWS</>}
              {stats.research > 0 && <> · {stats.research} RESEARCH</>}
              {stats.filings > 0 && <> · {stats.filings} FILINGS</>}
            </div>
          )}

          {/* Entity tags */}
          {entities.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {entities.map((tag) => (
                <span
                  key={tag}
                  style={{
                    border: "1px solid var(--bg-border)",
                    fontSize: 9,
                    fontFamily: "var(--font-mono)",
                    padding: "2px 6px",
                    borderRadius: "var(--radius-badge)",
                    color: "var(--text-muted)",
                    letterSpacing: "0.3px",
                  }}
                >
                  {tag}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>
    </article>
  );
}

/* ============================================================
   SECONDARY VARIANT — Compact sidebar card
   ============================================================ */
function SecondaryCard({
  narrative,
  showSummary = false,
  onInvestigateClick,
}: {
  narrative: VisibleNarrative;
  showSummary?: boolean;
  onInvestigateClick?: (id: string) => void;
}) {
  const { isWatched, toggleWatch } = useWatchlist();
  const watched = isWatched(narrative.id);
  const vel = parseVelocity(narrative.velocity_summary);
  const velColor = velSemanticColor(narrative.velocity_summary);
  const [hoverCta, setHoverCta] = useState(false);

  function handleInvestigate() {
    onInvestigateClick?.(narrative.id);
  }

  return (
    <article
      className="p-4 cursor-pointer transition-all hover:bg-[var(--accent-primary-hover)]"
      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
      onClick={handleInvestigate}
      onMouseEnter={() => setHoverCta(true)}
      onMouseLeave={() => setHoverCta(false)}
      aria-label={`Narrative: ${narrative.name}`}
      tabIndex={0}
      role="button"
    >
      <div className="flex items-start justify-between gap-2 mb-1">
        <div className="flex items-center gap-2 min-w-0">
          <button
            onClick={(e) => { e.stopPropagation(); toggleWatch("narrative", narrative.id); }}
            className="shrink-0"
            style={{ background: "none", border: "none", cursor: "pointer", padding: 0 }}
            aria-label={watched ? "Remove from watchlist" : "Add to watchlist"}
          >
            <Star
              size={14}
              fill={watched ? "var(--intent-warning)" : "none"}
              stroke={watched ? "var(--intent-warning)" : "var(--text-disabled)"}
            />
          </button>
          <h3
            className="text-text-primary font-medium leading-snug line-clamp-2"
            style={{ fontSize: 15, fontFamily: "var(--font-display)", letterSpacing: "-0.01em" }}
          >
            {narrative.name}
          </h3>
        </div>
        {narrative.stage && (
          <span
            data-testid="stage-badge"
            className={`shrink-0 font-medium px-1.5 py-0.5 ${stageBadgeClass(narrative.stage)}`}
            style={{
              fontSize: 9,
              borderRadius: "var(--radius-badge)",
              background: "transparent",
              border: `1px solid ${stageBorderColor(narrative.stage)}`,
              color: stageBorderColor(narrative.stage),
            }}
          >
            {narrative.stage.toLowerCase()}
          </span>
        )}
      </div>

      {/* Signal + topic labels */}
      <div className="flex items-center gap-2 mb-2 flex-wrap">
        <SignalBadge direction={narrative.signal_direction} confidence={narrative.signal_confidence} />
        <CatalystTypeBadge type={narrative.signal_catalyst_type} />
        {narrative.topic_tags && narrative.topic_tags.length > 0 &&
          narrative.topic_tags.map((tag) => (
            <span key={tag} className="label-topic">{tag}</span>
          ))
        }
      </div>

      {/* One-line summary for top secondary card only */}
      {showSummary && (
        <p className="text-text-muted line-clamp-1 mb-2" style={{ fontSize: 12 }}>
          {narrative.descriptor}
        </p>
      )}

      {/* Velocity + momentum bar */}
      <div className="flex items-center gap-3 mb-2">
        <DirectionArrow summary={narrative.velocity_summary} />
        <span className="font-data-large" style={{ color: velColor }}>
          {narrative.velocity_summary.match(/[+-]?\d+\.?\d*%?/)?.[0] || "0%"}
        </span>
        {narrative.burst_velocity?.is_burst && (
          <span
            data-testid="burst-indicator"
            className="bg-critical-bg text-critical font-medium animate-pulse-accent"
            style={{ fontSize: 9, padding: "1px 5px", borderRadius: "var(--radius-badge)" }}
          >
            SURGE
          </span>
        )}
      </div>
      <MomentumBar velocity={vel} size="md" />

      {/* CTA revealed on hover */}
      <div
        className="mt-2 text-xs font-medium transition-all"
        style={{
          color: "var(--text-muted)",
          opacity: hoverCta ? 1 : 0,
          height: hoverCta ? 20 : 0,
          overflow: "hidden",
          transition: "opacity 150ms ease, height 150ms ease",
        }}
      >
        {narrative.burst_velocity?.is_burst ? "Monitor →" : "Deep Dive →"}
      </div>

      {/* Hidden CTA for accessibility */}
      <button
        onClick={(e) => { e.stopPropagation(); handleInvestigate(); }}
        className="sr-only"
        aria-label={`Investigate: ${narrative.name}`}
      >
        Investigate
      </button>
    </article>
  );
}

/* ============================================================
   COMPACT VARIANT — Dense signal row with proper columns
   ============================================================ */
function CompactRow({
  narrative,
  onInvestigateClick,
}: {
  narrative: VisibleNarrative;
  onInvestigateClick?: (id: string) => void;
}) {
  const [hovered, setHovered] = useState(false);
  const velColor = velSemanticColor(narrative.velocity_summary);

  function handleInvestigate() {
    onInvestigateClick?.(narrative.id);
  }

  return (
    <article
      className="flex items-center gap-3 px-4 cursor-pointer transition-all"
      style={{
        height: hovered ? "auto" : 44,
        minHeight: 44,
        borderBottom: "1px solid var(--bg-border)",
        borderLeft: `2px solid ${velColor}`,
        background: hovered ? "var(--bg-surface-hover)" : "transparent",
      }}
      onClick={handleInvestigate}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      aria-label={`Narrative: ${narrative.name}`}
      tabIndex={0}
      role="button"
    >
      {/* Direction arrow */}
      <DirectionArrow summary={narrative.velocity_summary} />

      {/* Velocity number */}
      <span
        className={`font-data-medium shrink-0 ${isAccelerating(narrative.velocity_summary) ? "animate-pulse-accent" : ""}`}
        style={{ color: velColor, width: 72, textAlign: "right" }}
      >
        {narrative.velocity_summary.match(/[+-]?\d+\.?\d*%?/)?.[0] || "0%"}
      </span>

      {/* Mini trend bars */}
      <MiniTrendBars timeseries={narrative.velocity_timeseries} color={velColor} />

      {/* Name — truncated */}
      <div className="flex-1 min-w-0">
        <span
          className="text-text-primary truncate block font-medium"
          style={{ fontSize: 13, fontFamily: "var(--font-display)" }}
        >
          {narrative.name}
        </span>
        {/* Hover reveals one-line summary */}
        {hovered && narrative.descriptor && (
          <span className="text-text-muted block truncate" style={{ fontSize: 11, marginTop: 2 }}>
            {narrative.descriptor}
          </span>
        )}
      </div>

      {/* Stage badge (border-only) */}
      {narrative.stage && (
        <span
          data-testid="stage-badge"
          className={`shrink-0 font-medium px-1.5 py-0.5 ${stageBadgeClass(narrative.stage)}`}
          style={{
            fontSize: 9,
            borderRadius: "var(--radius-badge)",
            background: "transparent",
            border: `1px solid ${stageBorderColor(narrative.stage)}`,
            color: stageBorderColor(narrative.stage),
          }}
        >
          {narrative.stage.toLowerCase()}
        </span>
      )}

      <SignalBadge direction={narrative.signal_direction} confidence={narrative.signal_confidence} />

      {/* Topics */}
      {narrative.topic_tags && narrative.topic_tags.length > 0 && (
        <span className="label-topic shrink-0 hidden xl:inline">
          {narrative.topic_tags.join(" · ")}
        </span>
      )}

      {/* Doc count */}
      <span className="font-mono-data shrink-0" style={{ color: "var(--text-muted)", width: 36, textAlign: "right", fontSize: 11 }}>
        {narrative.signals?.length ?? "—"}
      </span>

      {/* Last updated */}
      <span className="font-mono-data shrink-0" style={{ color: "var(--text-disabled)", width: 32, textAlign: "right", fontSize: 10 }}>
        {relativeTime(narrative.last_evidence_at || "")}
      </span>

      {/* SURGE */}
      {narrative.burst_velocity?.is_burst && (
        <span
          data-testid="burst-indicator"
          className="bg-critical-bg text-critical font-medium animate-pulse-accent shrink-0"
          style={{ fontSize: 9, padding: "1px 5px", borderRadius: "var(--radius-badge)" }}
        >
          SURGE
        </span>
      )}

      {/* Hidden CTA for accessibility */}
      <button
        onClick={(e) => { e.stopPropagation(); handleInvestigate(); }}
        className="sr-only"
        aria-label={`Investigate: ${narrative.name}`}
      >
        Investigate
      </button>
    </article>
  );
}

/* ============================================================
   DEFAULT VARIANT — Original card (UNCHANGED for test safety)
   ============================================================ */
function DefaultCard({
  narrative,
  onInvestigateClick,
}: {
  narrative: VisibleNarrative;
  onInvestigateClick?: (id: string) => void;
}) {
  function handleInvestigate() {
    onInvestigateClick?.(narrative.id);
  }

  return (
    <article
      className="rounded-sm p-5 flex flex-col gap-3 transition-all cursor-pointer relative overflow-hidden hover:bg-[var(--accent-primary-hover)]"
      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
      onClick={handleInvestigate}
      aria-label={`Narrative: ${narrative.name}`}
    >
      <div className="flex items-start justify-between gap-2">
        <h3
          className="text-text-primary font-semibold leading-snug line-clamp-2 font-display"
          style={{ fontSize: 20 }}
        >
          {narrative.name}
        </h3>
        {narrative.stage && (
          <span
            data-testid="stage-badge"
            className={`shrink-0 font-medium px-1.5 py-0.5 ${stageBadgeClass(narrative.stage)}`}
            style={{
              fontSize: "var(--text-micro)",
              borderRadius: "var(--radius-badge)",
              ...(narrative.stage === "Emerging"
                ? { background: "rgba(200, 118, 25, 0.15)", color: "var(--intent-warning)" }
                : {}),
            }}
          >
            {narrative.stage.toLowerCase()}
          </span>
        )}
      </div>

      <p
        className="text-text-muted leading-relaxed flex-1"
        style={{
          fontSize: "var(--text-body)",
          display: "-webkit-box",
          WebkitLineClamp: 3,
          WebkitBoxOrient: "vertical",
          overflow: "hidden",
        }}
      >
        {narrative.descriptor}
      </p>

      {narrative.topic_tags && narrative.topic_tags.length > 0 && (
        <div className="flex flex-wrap gap-1" data-testid="topic-tags">
          {narrative.topic_tags.map((tag) => (
            <span
              key={tag}
              className="font-medium"
              style={{
                fontSize: "var(--text-micro)",
                padding: "var(--space-1) var(--space-2)",
                borderRadius: "var(--radius-badge)",
                background: "var(--bg-border)",
                color: "var(--text-muted)",
              }}
            >
              {tag}
            </span>
          ))}
        </div>
      )}

      <div style={{ minHeight: 70 }}>
        <VelocitySparkline timeseries={narrative.velocity_timeseries} height={70} showEndValue />
      </div>

      <div className="flex items-center justify-between pt-1" style={{ borderTop: "1px solid var(--bg-border)" }}>
        <span className="flex items-center gap-1.5">
          <span
            className="font-mono-data cursor-help"
            style={{
              color: velocityColor(narrative.velocity_summary),
              fontSize: "var(--text-mono-size)",
              fontWeight: 500,
            }}
            aria-label={`Momentum: ${narrative.velocity_summary}`}
            title="Narrative momentum — how fast this story is evolving"
          >
            {narrative.velocity_summary}
          </span>
          {narrative.burst_velocity?.is_burst && (
            <span
              data-testid="burst-indicator"
              className="bg-critical-bg text-critical font-medium cursor-help"
              style={{
                fontSize: "var(--text-micro)",
                padding: "var(--space-1) var(--space-2)",
                borderRadius: "var(--radius-badge)",
              }}
              title={`Document ingestion rate is ${narrative.burst_velocity.ratio}x above normal`}
            >
              SURGE
            </span>
          )}
        </span>
      </div>

      <button
        onClick={(e) => { e.stopPropagation(); handleInvestigate(); }}
        className="mt-1 w-full flex items-center justify-center gap-2 text-xs font-medium py-2 px-3 transition-all hover:border-[var(--accent-primary)] hover:text-[var(--accent-primary)] hover:bg-[var(--accent-primary-hover)]"
        style={{
          border: "1px solid var(--bg-border)",
          background: "transparent",
          color: "var(--text-muted)",
          borderRadius: "var(--radius-badge)",
          height: 32,
        }}
        aria-label={`Investigate: ${narrative.name}`}
      >
        <Search size={13} />
        Investigate
      </button>

    </article>
  );
}

/* ============================================================
   BLURRED CARD (neutral placeholder)
   ============================================================ */
function BlurredCard() {
  return (
    <article
      data-testid="blurred-card"
      className="relative overflow-hidden h-44"
      aria-label="Narrative unavailable"
    >
      <div
        className="absolute inset-0 p-5"
        style={{ background: "var(--bg-surface)", border: "1px solid var(--bg-border)" }}
      >
        <div className="h-4 rounded w-3/4 mb-2" style={{ background: "var(--bg-surface-hover)" }} />
        <div className="h-3 rounded w-full mb-1" style={{ background: "var(--bg-border)" }} />
        <div className="h-3 rounded w-5/6" style={{ background: "var(--bg-border)" }} />
      </div>
      <div
        className="absolute inset-0 flex flex-col items-center justify-center gap-2"
        style={{ background: "var(--bg-surface)", opacity: 0.85 }}
      >
        <span style={{ color: "var(--text-muted)", fontSize: "var(--text-small)", fontWeight: 500 }}>
          Narrative unavailable
        </span>
      </div>
    </article>
  );
}

/* ============================================================
   EXPORTS — variant dispatch
   ============================================================ */
export default function NarrativeCard({
  narrative,
  variant,
  showSummary,
  onInvestigateClick,
}: Props) {
  if (narrative.blurred) {
    return <BlurredCard />;
  }

  const vis = narrative as VisibleNarrative;

  switch (variant) {
    case "hero":
      return <HeroCard narrative={vis} onInvestigateClick={onInvestigateClick} />;
    case "secondary":
      return <SecondaryCard narrative={vis} showSummary={showSummary} onInvestigateClick={onInvestigateClick} />;
    case "compact":
      return <CompactRow narrative={vis} onInvestigateClick={onInvestigateClick} />;
    default:
      return <DefaultCard narrative={vis} onInvestigateClick={onInvestigateClick} />;
  }
}
