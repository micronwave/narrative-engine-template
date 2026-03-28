"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { ArrowLeft, Download, Sparkles, Loader2 } from "lucide-react";
import Link from "next/link";
import { fetchNarrativeDetail, exportNarrative, fetchNarrativeAssets, fetchNarrativeManipulation, fetchNarrativeHistory, fetchNarrativeSources, fetchNarrativeCorrelations, analyzeNarrative } from "@/lib/api";
import type { NarrativeDetail, NarrativeAsset, ManipulationIndicator, NarrativeSnapshot, SourceBreakdown, CorrelationResult, DeepAnalysis } from "@/lib/api";
import { useAuth } from "@/contexts/AuthContext";
import VelocitySparkline from "@/components/VelocitySparkline";
import HistoryChart from "@/components/HistoryChart";
import NarrativeChangelog from "@/components/NarrativeChangelog";
import AffectedAssets from "@/components/AffectedAssets";
import StageBadge from "@/components/common/StageBadge";
import MetricTooltip from "@/components/common/MetricTooltip";

export default function NarrativeDetailPage() {
  const params = useParams();
  const id = params?.id as string;
  const { token } = useAuth();

  const [narrative, setNarrative] = useState<NarrativeDetail | null>(null);
  const [assets, setAssets] = useState<NarrativeAsset[]>([]);
  const [manipulationIndicators, setManipulationIndicators] = useState<ManipulationIndicator[]>([]);
  const [velocityHistory, setVelocityHistory] = useState<NarrativeSnapshot[]>([]);
  const [sources, setSources] = useState<SourceBreakdown[]>([]);
  const [correlations, setCorrelations] = useState<CorrelationResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [notFound, setNotFound] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [analysis, setAnalysis] = useState<DeepAnalysis | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [analyzeError, setAnalyzeError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    fetchNarrativeDetail(id)
      .then(setNarrative)
      .catch((err: Error) => {
        if (err.message.includes("404")) setNotFound(true);
      })
      .finally(() => setLoading(false));
    fetchNarrativeAssets(id)
      .then(setAssets)
      .catch(() => setAssets([]));
    fetchNarrativeManipulation(id)
      .then(setManipulationIndicators)
      .catch(() => setManipulationIndicators([]));
    fetchNarrativeHistory(id, 30)
      .then(setVelocityHistory)
      .catch(() => setVelocityHistory([]));
    fetchNarrativeSources(id)
      .then(setSources)
      .catch(() => setSources([]));
    fetchNarrativeCorrelations(id)
      .then(setCorrelations)
      .catch(() => setCorrelations([]));
  }, [id]);

  async function handleExport() {
    if (!token || !id) return;
    setExporting(true);
    try {
      const blob = await exportNarrative(id, token);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `narrative-${id}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Export failed:", err);
    } finally {
      setExporting(false);
    }
  }

  async function handleAnalyze(force = false) {
    if (!id) return;
    setAnalyzing(true);
    setAnalyzeError(null);
    try {
      const result = await analyzeNarrative(id, force);
      setAnalysis(result);
    } catch (err) {
      setAnalyzeError("Analysis failed. Try again later.");
      console.error("Analyze failed:", err);
    } finally {
      setAnalyzing(false);
    }
  }

  if (loading) {
    return (
      <main className="min-h-screen bg-base text-text-primary flex items-center justify-center">
        <p className="text-text-tertiary text-sm">Loading narrative…</p>
      </main>
    );
  }

  if (notFound || !narrative) {
    return (
      <main className="min-h-screen bg-base text-text-primary p-8">
        <Link
          href="/"
          className="flex items-center gap-2 text-text-secondary hover:text-text-primary text-sm mb-8"
        >
          <ArrowLeft size={14} /> Back
        </Link>
        <p className="text-bearish">Narrative not found.</p>
      </main>
    );
  }

  const entropyDisplay =
    narrative.entropy !== null && narrative.entropy !== undefined
      ? narrative.entropy.toFixed(3)
      : "N/A";
  const saturationDisplay =
    narrative.saturation !== null && narrative.saturation !== undefined
      ? narrative.saturation.toFixed(3)
      : "N/A";

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Back link */}
        <Link
          href="/"
          className="inline-flex items-center gap-2 font-mono text-[12px] text-text-muted hover:text-text-primary transition-all mb-6"
        >
          <ArrowLeft size={14} /> Back to signals
        </Link>

        {/* Manipulation warning banner */}
        {manipulationIndicators.length > 0 && (
          <div
            role="alert"
            data-testid="manipulation-warning-banner"
            className="mb-6 bg-alert-bg border-l-[3px] border-l-alert border border-alert/20 rounded-sm px-4 py-3 text-sm text-alert"
          >
            This narrative has {manipulationIndicators.length} manipulation indicator{manipulationIndicators.length !== 1 ? "s" : ""}.{" "}
            <a href="/manipulation" className="underline">
              View details →
            </a>
          </div>
        )}

        {/* Page title row */}
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-[22px] font-semibold text-text-primary leading-snug font-display" style={{ letterSpacing: "-0.01em" }}>
                {narrative.name}
              </h1>
              {narrative.stage && <StageBadge stage={narrative.stage} />}
            </div>
            <p className="text-text-secondary text-[13px]">{narrative.descriptor}</p>
          </div>

          <div className="shrink-0 flex items-center gap-2">
            <button
              onClick={() => handleAnalyze(false)}
              disabled={analyzing}
              className="flex items-center gap-1.5 bg-[var(--bg-surface)] hover:bg-[var(--bg-surface-hover)] border border-[var(--bg-border)] disabled:opacity-40 disabled:cursor-not-allowed text-text-primary text-[13px] font-medium px-4 py-2 rounded-sm transition-all"
              data-testid="analyze-btn"
            >
              {analyzing ? <Loader2 size={13} className="animate-spin" /> : <Sparkles size={13} />}
              {analyzing ? "Analyzing..." : "AI Analysis"}
            </button>
            {token && (
              <button
                onClick={handleExport}
                disabled={exporting}
                className="flex items-center gap-1.5 bg-accent-primary hover:brightness-110 disabled:opacity-40 disabled:cursor-not-allowed text-text-primary text-[13px] font-medium px-4 py-2 rounded-sm transition-all"
                aria-label="Export narrative report as CSV"
                data-testid="export-btn"
              >
                <Download size={13} />
                {exporting ? "Exporting..." : "Export Report"}
              </button>
            )}
          </div>
        </div>

        {/* Title rule */}
        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-8" />

        {/* Key metrics row */}
        <div className="flex flex-wrap gap-5 mb-10">
          <MetricTooltip metricKey="velocity">
            <span className="font-mono text-[12px] text-bullish">
              {narrative.velocity_summary}
            </span>
          </MetricTooltip>
          <MetricTooltip metricKey="entropy">
            <span className="font-mono text-[12px] text-text-secondary">
              diversity {entropyDisplay}
            </span>
          </MetricTooltip>
          <MetricTooltip metricKey="ns_score">
            <span className="font-mono text-[12px] text-text-secondary">
              saturation {saturationDisplay}
            </span>
          </MetricTooltip>
        </div>

        {/* Velocity sparkline */}
        <section className="mb-10">
          <div className="flex items-baseline gap-3 mb-4">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Momentum Trend
            </h2>
            <span className="font-mono text-[11px] text-text-muted">7-day window</span>
          </div>
          <VelocitySparkline
            timeseries={narrative.velocity_timeseries}
            width={600}
            height={60}
          />
        </section>

        {/* Velocity History (F5) */}
        {velocityHistory.length > 1 && (
          <section className="mb-10">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                Velocity History
              </h2>
              <span className="font-mono text-[11px] text-text-muted">30-day window</span>
            </div>
            <HistoryChart
              data={velocityHistory
                .slice()
                .reverse()
                .map((s) => ({ date: s.date || "", value: s.velocity || 0 }))}
              color="#32A467"
            />
          </section>
        )}

        {/* Entropy detail */}
        {narrative.entropy_detail && (
          <section className="mb-10">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                Diversity Breakdown
              </h2>
              <span className="font-mono text-[11px] text-text-muted">entropy components</span>
            </div>
            <div className="grid grid-cols-3 gap-5">
              <MetricTooltip metricKey="entropy">
                <div className="cursor-help">
                  <div className="font-mono text-[22px] font-semibold text-text-primary">
                    {narrative.entropy_detail.components.source_diversity.toFixed(2)}
                  </div>
                  <div className="font-mono text-[10px] uppercase text-text-muted mt-1">Source Diversity</div>
                </div>
              </MetricTooltip>
              <div>
                <div className="font-mono text-[22px] font-semibold text-text-primary">
                  {narrative.entropy_detail.components.temporal_spread.toFixed(2)}
                </div>
                <div className="font-mono text-[10px] uppercase text-text-muted mt-1">Temporal Spread</div>
              </div>
              <MetricTooltip metricKey="polarization">
                <div className="cursor-help">
                  <div className="font-mono text-[22px] font-semibold text-text-primary">
                    {narrative.entropy_detail.components.sentiment_variance.toFixed(2)}
                  </div>
                  <div className="font-mono text-[10px] uppercase text-text-muted mt-1">Sentiment Variance</div>
                </div>
              </MetricTooltip>
            </div>
          </section>
        )}

        {/* V3 1.1: Deep Analysis (Sonnet) */}
        {narrative.sonnet_analysis && (
          <section className="mb-10">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                Deep Analysis
              </h2>
              <span className="font-mono text-[11px] text-text-muted">AI · Sonnet</span>
            </div>
            <div
              className="font-display text-[13px] text-text-secondary leading-[1.7] border-l-[3px] pl-5"
              style={{ borderColor: "var(--vel-accelerating)" }}
            >
              {narrative.sonnet_analysis}
            </div>
          </section>
        )}

        {/* Phase 3 Batch 3: AI Deep Analysis (Haiku, structured) */}
        {analysis && (
          <section className="mb-10" data-testid="deep-analysis-section">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                AI Analysis
              </h2>
              <span className="font-mono text-[11px] text-text-muted">
                Haiku {analysis.cached ? "· cached" : "· fresh"} · {new Date(analysis.analyzed_at).toLocaleDateString()}
              </span>
              {analysis.cached && (
                <button
                  onClick={() => handleAnalyze(true)}
                  className="font-mono text-[10px] text-accent-text hover:text-text-primary transition-colors"
                  data-testid="refresh-analysis-btn"
                >
                  Refresh
                </button>
              )}
            </div>

            {/* Thesis */}
            <div
              className="font-display text-[13px] text-text-secondary leading-[1.7] border-l-[3px] pl-5 mb-6"
              style={{ borderColor: "var(--accent-primary)" }}
              data-testid="analysis-thesis"
            >
              {analysis.thesis}
            </div>

            {/* Key Drivers */}
            {analysis.key_drivers.length > 0 && (
              <div className="mb-4">
                <div className="font-mono text-[10px] uppercase text-text-muted mb-2 tracking-[0.06em]">
                  Key Drivers
                </div>
                <ul className="flex flex-col gap-1">
                  {analysis.key_drivers.map((d, i) => (
                    <li
                      key={i}
                      className="font-mono text-[12px] text-text-secondary pl-3 border-l"
                      style={{ borderColor: "var(--bg-border)" }}
                    >
                      {d}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Asset Impact */}
            {analysis.asset_impact.length > 0 && (
              <div className="mb-4">
                <div className="font-mono text-[10px] uppercase text-text-muted mb-2 tracking-[0.06em]">
                  Asset Impact
                </div>
                {analysis.asset_impact.map((a, i) => (
                  <div
                    key={i}
                    className="flex gap-2 py-1"
                    style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                  >
                    <span className="font-mono text-[12px] text-text-primary font-semibold shrink-0">
                      {a.asset}
                    </span>
                    <span className="font-mono text-[11px] text-text-secondary">
                      {a.impact}
                    </span>
                  </div>
                ))}
              </div>
            )}

            {/* Risk Factors */}
            {analysis.risk_factors.length > 0 && (
              <div className="mb-4">
                <div className="font-mono text-[10px] uppercase text-text-muted mb-2 tracking-[0.06em]">
                  Risk Factors
                </div>
                <ul className="flex flex-col gap-1">
                  {analysis.risk_factors.map((r, i) => (
                    <li
                      key={i}
                      className="font-mono text-[12px] text-text-secondary pl-3 border-l"
                      style={{ borderColor: "var(--intent-danger)" }}
                    >
                      {r}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Historical Comparison */}
            {analysis.historical_comparison && (
              <div className="mt-4 font-mono text-[11px] text-text-muted italic">
                {analysis.historical_comparison}
              </div>
            )}
          </section>
        )}

        {/* Analysis error */}
        {analyzeError && !analysis && (
          <div
            className="mb-6 font-mono text-[12px]"
            style={{ color: "var(--intent-danger)" }}
            data-testid="analyze-error"
          >
            {analyzeError}
          </div>
        )}

        {/* V3 1.3: Market Impact — correlations */}
        {correlations.length > 0 && (
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
                  style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                >
                  <span className="font-mono text-[13px] font-semibold text-text-primary">
                    {c.ticker}
                  </span>
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
                  <span className="font-mono text-[10px] text-text-muted">
                    {c.interpretation}
                  </span>
                </div>
              ))}
            </div>
          </section>
        )}

        {/* V3 1.4: Source Coverage */}
        {sources.length > 0 && (
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
                  <span className="font-mono text-[11px] text-text-secondary w-[160px] truncate shrink-0">
                    {s.domain}
                  </span>
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
        )}

        {/* Phase 1: Structured Signal Panel */}
        {narrative.signal && (
          <section className="mb-10" data-testid="signal-panel">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                Signal Intelligence
              </h2>
              <span className="font-mono text-[11px] text-text-muted">
                {narrative.signal.certainty} · {narrative.signal.timeframe.replace(/_/g, " ")}
              </span>
            </div>

            {/* Direction — primary indicator */}
            <div className="flex items-center gap-4 mb-4">
              <div
                className="font-mono text-[22px] font-bold uppercase"
                style={{
                  color: narrative.signal.direction === "bullish" ? "var(--bullish)"
                       : narrative.signal.direction === "bearish" ? "var(--bearish)"
                       : "var(--text-muted)",
                }}
              >
                {narrative.signal.direction}
              </div>
              {narrative.signal.catalyst_type && narrative.signal.catalyst_type !== "unknown" && (
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
                  {narrative.signal.catalyst_type}
                </span>
              )}
            </div>

            {/* Confidence bar */}
            <div className="mb-4">
              <div className="flex items-center justify-between mb-1">
                <span className="font-mono text-[10px] uppercase text-text-muted">Confidence</span>
                <span className="font-mono text-[11px] text-text-primary">
                  {((narrative.signal.confidence ?? 0) * 100).toFixed(0)}%
                </span>
              </div>
              <div className="h-1.5 bg-[var(--bg-surface-hover)]">
                <div
                  className="h-full"
                  style={{
                    width: `${(narrative.signal.confidence ?? 0) * 100}%`,
                    background: narrative.signal.direction === "bullish" ? "var(--bullish)"
                               : narrative.signal.direction === "bearish" ? "var(--bearish)"
                               : "var(--text-muted)",
                  }}
                />
              </div>
            </div>

            {/* Metadata row */}
            <div className="flex gap-6 mb-4">
              <div>
                <div className="font-mono text-[10px] uppercase text-text-muted mb-1">Certainty</div>
                <div className="font-mono text-[12px] text-text-primary">{narrative.signal.certainty}</div>
              </div>
              <div>
                <div className="font-mono text-[10px] uppercase text-text-muted mb-1">Timeframe</div>
                <div className="font-mono text-[12px] text-text-primary">{narrative.signal.timeframe.replace(/_/g, " ")}</div>
              </div>
              <div>
                <div className="font-mono text-[10px] uppercase text-text-muted mb-1">Magnitude</div>
                <div className="font-mono text-[12px] text-text-primary">{narrative.signal.magnitude}</div>
              </div>
            </div>

            {/* Key actors */}
            {(narrative.signal.key_actors ?? []).length > 0 && (
              <div className="mb-3">
                <div className="font-mono text-[10px] uppercase text-text-muted mb-1.5">Key Actors</div>
                <div className="flex flex-wrap gap-1.5">
                  {(narrative.signal.key_actors ?? []).map((actor) => (
                    <span key={actor} className="label-topic">{actor}</span>
                  ))}
                </div>
              </div>
            )}

            {/* Affected sectors */}
            {(narrative.signal.affected_sectors ?? []).length > 0 && (
              <div className="mb-3">
                <div className="font-mono text-[10px] uppercase text-text-muted mb-1.5">Affected Sectors</div>
                <div className="flex flex-wrap gap-1.5">
                  {(narrative.signal.affected_sectors ?? []).map((sector) => (
                    <span key={sector} className="label-topic">{sector}</span>
                  ))}
                </div>
              </div>
            )}
          </section>
        )}

        {/* Legacy sentiment fallback — shown only when no signal data */}
        {!narrative.signal && narrative.sentiment && narrative.sentiment.count > 0 && (
          <section className="mb-10">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                Sentiment
              </h2>
              <span className="font-mono text-[11px] text-text-muted">
                {narrative.sentiment.polarization_label} · {narrative.sentiment.count} documents
              </span>
            </div>
            <div className="flex items-center gap-4 mb-4">
              <div
                className="font-mono text-[22px] font-bold"
                style={{ color: narrative.sentiment.mean > 0.02 ? "var(--vel-accelerating)" : narrative.sentiment.mean < -0.02 ? "var(--vel-decelerating)" : "var(--vel-stable)" }}
              >
                {narrative.sentiment.mean > 0 ? "+" : ""}{narrative.sentiment.mean.toFixed(3)}
              </div>
              <div className="font-mono text-[10px] uppercase text-text-muted">
                Avg Sentiment
              </div>
            </div>
            <div className="relative h-2 bg-[var(--bg-surface-hover)]">
              <div
                className="absolute -top-0.5 w-1 h-3 bg-text-primary"
                style={{
                  left: `${Math.max(0, Math.min(100, (narrative.sentiment.mean + 1) / 2 * 100))}%`,
                  transform: "translateX(-50%)",
                }}
              />
              <div className="absolute left-0 top-3 font-mono text-[9px] text-text-muted">-1</div>
              <div className="absolute right-0 top-3 font-mono text-[9px] text-text-muted">+1</div>
            </div>
          </section>
        )}

        {/* V3 1.2: Coordination Risk */}
        {narrative.coordination && (narrative.coordination.is_coordinated || narrative.coordination.events.length > 0) && (
          <section className="mb-10">
            <div className="flex items-baseline gap-3 mb-4">
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                Coordination Risk
              </h2>
              <span className="font-mono text-[11px] text-text-muted">adversarial detection</span>
            </div>
            <div className="font-mono text-[11px]" style={{ color: narrative.coordination.is_coordinated ? "var(--intent-danger)" : "var(--text-muted)" }}>
              {narrative.coordination.is_coordinated
                ? `Coordinated activity detected — ${narrative.coordination.flags} flag(s)`
                : "No coordination signals detected"}
            </div>
            {narrative.coordination.events.length > 0 && (
              <div className="mt-3 font-mono text-[11px] text-text-muted">
                {narrative.coordination.events.map((e, i) => (
                  <div key={i} className="py-1" style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}>
                    {e.event_type} — {e.detected_at}
                  </div>
                ))}
              </div>
            )}
          </section>
        )}

        {/* V3: Key Metrics Row */}
        <section className="mb-10">
          <div className="flex items-baseline gap-3 mb-4">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Key Metrics
            </h2>
          </div>
          <div className="flex flex-wrap gap-5">
            {[
              { label: "NS Score", value: narrative.ns_score?.toFixed(2), key: "ns_score" },
              { label: "Documents", value: String(narrative.document_count ?? 0), key: "" },
              { label: "Cross-Source", value: narrative.cross_source_score?.toFixed(2), key: "" },
              { label: "Stage", value: narrative.stage, key: "" },
            ].map((m) => m.value ? (
              <div key={m.label}>
                {m.key ? (
                  <MetricTooltip metricKey={m.key}>
                    <div>
                      <div className="font-mono text-[22px] font-bold text-text-primary">{m.value}</div>
                      <div className="font-mono text-[10px] uppercase tracking-[0.06em] text-text-muted mt-0.5">{m.label}</div>
                    </div>
                  </MetricTooltip>
                ) : (
                  <div>
                    <div className="font-mono text-[22px] font-bold text-text-primary">{m.value}</div>
                    <div className="font-mono text-[10px] uppercase tracking-[0.06em] text-text-muted mt-0.5">{m.label}</div>
                  </div>
                )}
              </div>
            ) : null)}
          </div>
        </section>

        {/* Signals */}
        <section className="mb-10">
          <div className="flex items-baseline gap-3 mb-4">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Evidence
            </h2>
            <span className="font-mono text-[11px] text-text-muted">{narrative.signals.length} signals</span>
          </div>
          {narrative.signals.length === 0 ? (
            <p className="font-mono text-[12px] text-text-muted">
              No evidence signals available.
            </p>
          ) : (
            <div className="flex flex-col">
              {narrative.signals.slice(0, 10).map((sig) => (
                <div
                  key={sig.id}
                  className="py-2.5 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
                  style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                >
                  <p className="text-text-primary text-[13px] line-clamp-2">
                    {sig.headline}
                  </p>
                  <div className="flex items-center gap-2 mt-1 text-text-muted text-[11px] font-mono">
                    <span>{sig.source.name}</span>
                    <span>·</span>
                    <span>
                      {sig.timestamp
                        ? new Date(sig.timestamp).toLocaleDateString()
                        : ""}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* Catalysts */}
        <section className="mb-10">
          <div className="flex items-baseline gap-3 mb-4">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Catalysts
            </h2>
            <span className="font-mono text-[11px] text-text-muted">{narrative.catalysts.length} detected</span>
          </div>
          {narrative.catalysts.length === 0 ? (
            <p className="font-mono text-[12px] text-text-muted">No catalysts detected.</p>
          ) : (
            <div className="flex flex-col">
              {narrative.catalysts.map((cat) => (
                <div
                  key={cat.id}
                  className="py-2.5 border-l-2 border-l-alert pl-4 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
                  style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                >
                  <p className="text-alert text-[13px]">{cat.description}</p>
                  <div className="flex items-center gap-2 mt-1 text-text-muted text-[11px] font-mono">
                    <span>impact {cat.impact_score.toFixed(2)}</span>
                    <span>·</span>
                    <span>
                      {cat.timestamp
                        ? new Date(cat.timestamp).toLocaleDateString()
                        : ""}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* Changelog — Phase 3 Batch 3 */}
        <section className="mb-10">
          <div className="flex items-baseline gap-3 mb-4">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Changelog
            </h2>
            <span className="font-mono text-[11px] text-text-muted">{narrative.mutations.length} events</span>
          </div>
          <NarrativeChangelog mutations={narrative.mutations} signals={narrative.signals} />
        </section>

        {/* Affected Asset Classes — D1 */}
        <section className="mb-10" data-testid="affected-assets-section">
          <div className="flex items-baseline gap-3 mb-4">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Affected Asset Classes
            </h2>
          </div>
          <AffectedAssets assets={assets} />
        </section>

        {/* Footer */}
        <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
        <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
          <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
          <span>narrative {id}</span>
        </div>
      </div>
    </main>
  );
}
