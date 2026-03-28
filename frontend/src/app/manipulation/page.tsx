"use client";

import { useEffect, useState, useMemo } from "react";
import { ShieldAlert, AlertTriangle } from "lucide-react";
import Link from "next/link";
import { fetchManipulation } from "@/lib/api";
import type { ManipulationIndicator, ManipulationNarrative } from "@/lib/api";
import Skeleton from "@/components/common/Skeleton";

const TYPE_LABELS: Record<ManipulationIndicator["indicator_type"], string> = {
  coordinated_amplification: "Coordinated Amplification",
  astroturfing: "Astroturfing",
  bot_network: "Bot Network",
  sockpuppet_cluster: "Sockpuppet Cluster",
  temporal_spike: "Temporal Spike",
  source_concentration: "Source Concentration",
};

const TYPE_CLASSES: Record<ManipulationIndicator["indicator_type"], string> = {
  coordinated_amplification: "bg-bearish-bg text-bearish border border-critical/40",
  astroturfing: "bg-alert-bg text-alert border border-alert/40",
  bot_network: "bg-purple-muted text-purple border border-purple/40",
  sockpuppet_cluster: "bg-bearish-bg text-bearish border border-critical/40",
  temporal_spike: "bg-alert-bg text-alert border border-alert/40",
  source_concentration: "bg-accent-muted text-accent-text border border-blue-800/40",
};

const STATUS_CLASSES: Record<ManipulationIndicator["status"], string> = {
  active: "border border-critical text-bearish",
  confirmed: "bg-critical text-text-primary",
  under_review: "border border-alert/60 text-alert",
  dismissed: "text-text-disabled line-through",
};

function formatRelativeTime(isoString: string): string {
  try {
    const diff = Date.now() - new Date(isoString).getTime();
    const days = Math.floor(diff / 86400000);
    if (days > 0) return `${days}d ago`;
    const hours = Math.floor(diff / 3600000);
    if (hours > 0) return `${hours}h ago`;
    const mins = Math.floor(diff / 60000);
    return `${mins}m ago`;
  } catch {
    return isoString;
  }
}

export default function ManipulationPage() {
  const [data, setData] = useState<ManipulationNarrative[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterType, setFilterType] = useState("");
  const [filterStatus, setFilterStatus] = useState("");
  const [minConfidence, setMinConfidence] = useState(0);

  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchManipulation()
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    return data
      .map((nar) => ({
        ...nar,
        manipulation_indicators: nar.manipulation_indicators.filter((mi) => {
          if (filterType && mi.indicator_type !== filterType) return false;
          if (filterStatus && mi.status !== filterStatus) return false;
          if (mi.confidence * 100 < minConfidence) return false;
          return true;
        }),
      }))
      .filter((nar) => nar.manipulation_indicators.length > 0);
  }, [data, filterType, filterStatus, minConfidence]);

  const allIndicators = data.flatMap((n) => n.manipulation_indicators);
  const activeCount = allIndicators.filter((mi) => mi.status === "active").length;
  const highestConf = allIndicators.reduce(
    (best, mi) => (mi.confidence > (best?.confidence ?? 0) ? mi : best),
    null as ManipulationIndicator | null
  );
  const highestNarName = highestConf
    ? data.find((n) => n.id === highestConf.narrative_id)?.name ?? highestConf.narrative_id
    : "—";
  const typeCounts: Record<string, number> = {};
  allIndicators.forEach((mi) => {
    typeCounts[mi.indicator_type] = (typeCounts[mi.indicator_type] ?? 0) + 1;
  });
  const mostCommonType = Object.entries(typeCounts).sort((a, b) => b[1] - a[1])[0]?.[0] ?? "—";

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Header */}
        <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
          Manipulation &amp; Coordination Detection
        </h1>
        <p className="font-mono text-[11px] text-text-muted mt-1">
          Narratives flagged for patterns consistent with artificial amplification or coordinated campaigns
        </p>

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {/* Summary stats */}
        <div
          data-testid="manipulation-stats"
          className="flex items-center gap-5 mb-8 flex-wrap"
        >
          <div>
            <div className="font-data-large text-text-primary">{data.length}</div>
            <div className="label-micro">Flagged</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <div className="font-data-large text-text-primary">{activeCount}</div>
            <div className="label-micro">Active</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <div className="font-mono text-[13px] font-semibold text-bearish truncate max-w-[200px]">
              {highestNarName}{" "}
              {highestConf && (
                <span className="font-mono-data">
                  {Math.round(highestConf.confidence * 100)}%
                </span>
              )}
            </div>
            <div className="label-micro">Highest Confidence</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <div className="font-mono text-[13px] font-semibold text-accent-text capitalize truncate max-w-[200px]">
              {mostCommonType.replace(/_/g, " ")}
            </div>
            <div className="label-micro">Most Common</div>
          </div>
        </div>

        {/* Filter bar */}
        <div className="flex flex-wrap items-center gap-3 mb-6">
          <select
            data-testid="filter-indicator-type"
            value={filterType}
            onChange={(e) => setFilterType(e.target.value)}
            className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
          >
            <option value="">All Types</option>
            <option value="coordinated_amplification">Coordinated Amplification</option>
            <option value="astroturfing">Astroturfing</option>
            <option value="bot_network">Bot Network</option>
            <option value="sockpuppet_cluster">Sockpuppet Cluster</option>
            <option value="temporal_spike">Temporal Spike</option>
            <option value="source_concentration">Source Concentration</option>
          </select>

          <div className="flex items-center gap-2">
            <label className="font-mono text-[10px] uppercase text-text-muted">Min confidence</label>
            <input
              data-testid="filter-min-confidence"
              type="number"
              min={0}
              max={100}
              value={minConfidence}
              onChange={(e) => setMinConfidence(Number(e.target.value))}
              className="w-14 font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
            />
            <span className="font-mono text-[10px] text-text-muted">%</span>
          </div>

          <select
            data-testid="filter-status"
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value)}
            className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
          >
            <option value="">All Statuses</option>
            <option value="active">Active</option>
            <option value="confirmed">Confirmed</option>
            <option value="under_review">Under Review</option>
            <option value="dismissed">Dismissed</option>
          </select>
        </div>

        {loading && (
          <div className="flex flex-col gap-3">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} height={80} />
            ))}
          </div>
        )}

        {!loading && error && (
          <div className="font-mono text-[12px] text-bearish py-8 text-center">
            Failed to load: {error}
          </div>
        )}

        {!loading && !error && filtered.length === 0 && (
          <p
            data-testid="manipulation-empty"
            className="font-mono text-[12px] text-text-muted py-8 text-center"
          >
            No manipulation patterns detected. The system continuously monitors for coordinated
            amplification and artificial narrative campaigns.
          </p>
        )}

        {!loading && filtered.length > 0 && (
          <div className="flex flex-col">
            {filtered.map((nar) => (
              <article
                key={nar.id}
                data-testid={`manipulation-card-${nar.id}`}
                className="py-4"
                style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
              >
                {/* Narrative header */}
                <div className="mb-3">
                  <Link
                    href={`/narrative/${nar.id}`}
                    className="text-[13px] text-text-primary font-semibold hover:text-accent-text transition-colors duration-[120ms]"
                    data-testid={`narrative-link-${nar.id}`}
                  >
                    {nar.name}
                  </Link>
                  {nar.descriptor && (
                    <p className="text-text-tertiary text-[12px] mt-0.5 line-clamp-2">{nar.descriptor}</p>
                  )}
                  <div className="flex items-center gap-3 mt-1 font-mono text-[10px] text-text-muted">
                    {nar.entropy !== null && nar.entropy !== undefined && (
                      <span className="cursor-help" title="Source diversity — how complex and multi-sourced this narrative is">diversity {nar.entropy.toFixed(3)}</span>
                    )}
                    {nar.velocity_summary && <span className="cursor-help" title="Narrative momentum — how fast this story is evolving">{nar.velocity_summary}</span>}
                  </div>
                </div>

                {/* Indicators */}
                <div className="pt-2">
                  <p className="font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted mb-2">
                    Manipulation Indicators
                  </p>
                  <div className="flex flex-col gap-3">
                    {nar.manipulation_indicators.map((mi) => (
                      <div
                        key={mi.id}
                        data-testid={`indicator-${mi.id}`}
                        className="pl-4 py-3"
                        style={{ borderLeft: "2px solid var(--bg-border)" }}
                      >
                        <div className="flex items-center gap-2 mb-2 flex-wrap">
                          {/* Type badge */}
                          <span
                            className={`font-mono text-[11px] font-medium px-2 py-0.5 rounded-sm ${TYPE_CLASSES[mi.indicator_type]}`}
                            data-testid={`type-badge-${mi.id}`}
                          >
                            {TYPE_LABELS[mi.indicator_type]}
                          </span>

                          {/* Status badge */}
                          <span
                            className={`font-mono text-[11px] px-2 py-0.5 rounded-sm ${STATUS_CLASSES[mi.status]}`}
                            data-testid={`status-badge-${mi.id}`}
                          >
                            {mi.status.replace(/_/g, " ")}
                          </span>

                          {/* Flagged signals count */}
                          <span
                            data-testid={`flagged-signals-${mi.id}`}
                            className="font-mono text-[11px] text-text-muted px-2 py-0.5 rounded-sm border border-[var(--bg-border)]"
                          >
                            Flagged Signals: {mi.flagged_signals.length}
                          </span>

                          {/* Detected at */}
                          <span
                            className="font-mono text-[10px] text-text-muted ml-auto"
                            title={new Date(mi.detected_at).toLocaleString()}
                          >
                            {formatRelativeTime(mi.detected_at)}
                          </span>
                        </div>

                        {/* Confidence bar */}
                        <div className="mb-2">
                          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted mb-1">
                            <span>Confidence</span>
                            <span>
                              {Math.round(mi.confidence * 100)}%
                            </span>
                          </div>
                          <div
                            className="h-1.5 w-full rounded-sm overflow-hidden"
                            style={{ background: 'var(--bg-border)' }}
                            data-testid={`confidence-bar-${mi.id}`}
                          >
                            <div
                              className="h-full rounded-sm"
                              style={{ width: `${mi.confidence * 100}%`, backgroundColor: mi.confidence * 100 >= 85 ? 'var(--intent-danger)' : mi.confidence * 100 >= 65 ? 'var(--intent-warning)' : mi.confidence * 100 >= 40 ? 'var(--intent-primary)' : 'var(--text-disabled)' }}
                            />
                          </div>
                        </div>

                        {/* Evidence summary */}
                        <p className="text-text-secondary text-[12px] leading-relaxed">
                          {mi.evidence_summary}
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              </article>
            ))}
          </div>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            <span>{filtered.length} flagged narratives</span>
          </div>
        </div>
      </div>
    </main>
  );
}
