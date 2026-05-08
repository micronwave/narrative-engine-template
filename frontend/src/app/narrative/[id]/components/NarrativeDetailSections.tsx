"use client";

import type { Catalyst, CorrelationResult, CoordinationData, Signal, SourceBreakdown } from "@/lib/api";
import MetricTooltip from "@/components/common/MetricTooltip";

export function MarketImpactSection({ correlations }: { correlations: CorrelationResult[] }) {
  if (correlations.length === 0) return null;
  return (
    <section className="mb-10">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
          Market Impact
        </h2>
        <span className="font-mono text-[11px] text-text-muted">velocity-price correlation</span>
      </div>
      <div className="flex flex-col">
        {correlations.slice(0, 5).map((c) => (
          <div
            key={c.ticker}
            className="flex items-center justify-between py-2.5 px-0 transition-colors hover:bg-[var(--accent-primary-hover)]"
            style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
          >
            <span className="font-mono text-[13px] font-semibold text-text-primary">{c.ticker}</span>
            <MetricTooltip metricKey="correlation">
              <span
                className="font-mono text-[18px] font-bold"
                style={{
                  color: Math.abs(c.correlation) < 0.1 ? "var(--text-muted)" : c.correlation > 0 ? "var(--vel-accelerating)" : "var(--vel-decelerating)",
                }}
              >
                r={c.correlation > 0 ? "+" : ""}{c.correlation.toFixed(3)}
              </span>
            </MetricTooltip>
            <span className="font-mono text-[10px] text-text-muted">{c.interpretation}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

export function SourceCoverageSection({ sources }: { sources: SourceBreakdown[] }) {
  if (sources.length === 0) return null;
  return (
    <section className="mb-10">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
          Source Coverage
        </h2>
        <span className="font-mono text-[11px] text-text-muted">
          {sources.length} source{sources.length !== 1 ? "s" : ""} · {sources.reduce((s, d) => s + d.count, 0)} articles
        </span>
      </div>
      <div className="flex flex-col gap-1">
        {sources.slice(0, 10).map((s) => (
          <div key={s.domain} className="flex items-center gap-2">
            <span className="font-mono text-[11px] text-text-secondary w-[160px] truncate shrink-0">{s.domain}</span>
            <div className="flex-1 h-1.5 bg-[var(--bg-surface-hover)] overflow-hidden">
              <div className="h-full bg-[var(--vel-stable)]" style={{ width: `${Math.min(s.percentage, 100)}%` }} />
            </div>
            <span className="font-mono text-[10px] text-text-muted w-[50px] text-right shrink-0">
              {s.count} ({s.percentage}%)
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

export function CoordinationRiskSection({ coordination }: { coordination: CoordinationData | null | undefined }) {
  if (!coordination || (!coordination.is_coordinated && coordination.events.length === 0)) return null;
  return (
    <section className="mb-10">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
          Coordination Risk
        </h2>
        <span className="font-mono text-[11px] text-text-muted">adversarial detection</span>
      </div>
      <div className="font-mono text-[11px]" style={{ color: coordination.is_coordinated ? "var(--intent-danger)" : "var(--text-muted)" }}>
        {coordination.is_coordinated
          ? `Coordinated activity detected — ${coordination.flags} flag(s)`
          : "No coordination signals detected"}
      </div>
      {coordination.events.length > 0 && (
        <div className="mt-3 font-mono text-[11px] text-text-muted">
          {coordination.events.map((e) => {
            const key = `${e.id ?? "event"}-${e.event_type ?? "type"}-${e.detected_at ?? "time"}`;
            return (
              <div key={key} className="py-1" style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}>
                {e.event_type} — {e.detected_at}
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

export function EvidenceSection({ signals }: { signals: Signal[] }) {
  return (
    <section className="mb-10">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">Evidence</h2>
        <span className="font-mono text-[11px] text-text-muted">{signals.length} signals</span>
      </div>
      {signals.length === 0 ? (
        <p className="font-mono text-[12px] text-text-muted">No evidence signals available.</p>
      ) : (
        <div className="flex flex-col">
          {signals.slice(0, 10).map((sig) => (
            <div
              key={sig.id}
              className="py-2.5 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
              style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
            >
              <p className="text-text-primary text-[13px] line-clamp-2">{sig.headline}</p>
              <div className="flex items-center gap-2 mt-1 text-text-muted text-[11px] font-mono">
                <span>{sig.source.name}</span>
                <span>·</span>
                <span>{sig.timestamp ? new Date(sig.timestamp).toLocaleDateString() : ""}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export function CatalystsSection({ catalysts }: { catalysts: Catalyst[] }) {
  return (
    <section className="mb-10">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">Catalysts</h2>
        <span className="font-mono text-[11px] text-text-muted">{catalysts.length} detected</span>
      </div>
      {catalysts.length === 0 ? (
        <p className="font-mono text-[12px] text-text-muted">No catalysts detected.</p>
      ) : (
        <div className="flex flex-col">
          {catalysts.map((cat) => (
            <div
              key={cat.id}
              className="py-2.5 border-l-2 border-l-alert pl-4 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
              style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
            >
              <p className="text-alert text-[13px]">{cat.description}</p>
              <div className="flex items-center gap-2 mt-1 text-text-muted text-[11px] font-mono">
                <span>impact {cat.impact_score.toFixed(2)}</span>
                <span>·</span>
                <span>{cat.timestamp ? new Date(cat.timestamp).toLocaleDateString() : ""}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
