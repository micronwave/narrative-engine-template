"use client";

import type { NarrativeSignal } from "@/lib/api";
import MetricTooltip from "@/components/common/MetricTooltip";

type SignalIntelligenceSectionProps = {
  signal?: NarrativeSignal | null;
};

export default function SignalIntelligenceSection({ signal }: SignalIntelligenceSectionProps) {
  if (!signal) return null;

  return (
    <section className="mb-10" data-testid="signal-panel">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
          Signal Intelligence
        </h2>
        <span className="font-mono text-[11px] text-text-muted">
          {signal.certainty} · {signal.timeframe.replace(/_/g, " ")}
        </span>
      </div>

      <div className="flex items-center gap-4 mb-4">
        <div
          className="font-mono text-[22px] font-bold uppercase"
          style={{
            color:
              signal.direction === "bullish"
                ? "var(--bullish)"
                : signal.direction === "bearish"
                  ? "var(--bearish)"
                  : "var(--text-muted)",
          }}
        >
          {signal.direction}
        </div>
        {signal.catalyst_type && signal.catalyst_type !== "unknown" && (
          <span
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
            {signal.catalyst_type}
          </span>
        )}
      </div>

      <div className="mb-4">
        <div className="flex items-center justify-between mb-1">
          <span className="font-mono text-[10px] uppercase text-text-muted">Confidence</span>
          <span className="font-mono text-[11px] text-text-primary">
            {((signal.confidence ?? 0) * 100).toFixed(0)}%
          </span>
        </div>
        <div className="h-1.5 bg-[var(--bg-surface-hover)]">
          <div
            className="h-full"
            style={{
              width: `${(signal.confidence ?? 0) * 100}%`,
              background:
                signal.direction === "bullish"
                  ? "var(--bullish)"
                  : signal.direction === "bearish"
                    ? "var(--bearish)"
                    : "var(--text-muted)",
            }}
          />
        </div>
      </div>

      <div className="flex gap-6 mb-4">
        <div>
          <div className="font-mono text-[10px] uppercase text-text-muted mb-1">Certainty</div>
          <div className="font-mono text-[12px] text-text-primary">{signal.certainty}</div>
        </div>
        <div>
          <div className="font-mono text-[10px] uppercase text-text-muted mb-1">Timeframe</div>
          <div className="font-mono text-[12px] text-text-primary">
            {signal.timeframe.replace(/_/g, " ")}
          </div>
        </div>
        <div>
          <div className="font-mono text-[10px] uppercase text-text-muted mb-1">Magnitude</div>
          <div className="font-mono text-[12px] text-text-primary">{signal.magnitude}</div>
        </div>
      </div>

      {(signal.key_actors ?? []).length > 0 && (
        <div className="mb-3">
          <div className="font-mono text-[10px] uppercase text-text-muted mb-1.5">Key Actors</div>
          <div className="flex flex-wrap gap-1.5">
            {(signal.key_actors ?? []).map((actor) => (
              <span key={actor} className="label-topic">
                {actor}
              </span>
            ))}
          </div>
        </div>
      )}

      {(signal.affected_sectors ?? []).length > 0 && (
        <div className="mb-3">
          <div className="font-mono text-[10px] uppercase text-text-muted mb-1.5">
            Affected Sectors
          </div>
          <div className="flex flex-wrap gap-1.5">
            {(signal.affected_sectors ?? []).map((sector) => (
              <span key={sector} className="label-topic">
                {sector}
              </span>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
