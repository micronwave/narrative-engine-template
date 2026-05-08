"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { ArrowLeft, Download, Sparkles, Loader2 } from "lucide-react";
import Link from "next/link";
import { fetchNarrativeDetail, exportNarrative, fetchNarrativeAssets, fetchNarrativeManipulation, fetchNarrativeHistory, fetchNarrativeSources, fetchNarrativeCorrelations, analyzeNarrative } from "@/lib/api";
import type { NarrativeDetail, NarrativeAsset, ManipulationIndicator, NarrativeSnapshot, SourceBreakdown, CorrelationResult, DeepAnalysis } from "@/lib/api";
import VelocitySparkline from "@/components/VelocitySparkline";
import HistoryChart from "@/components/HistoryChart";
import NarrativeChangelog from "@/components/NarrativeChangelog";
import AffectedAssets from "@/components/AffectedAssets";
import StageBadge from "@/components/common/StageBadge";
import MetricTooltip from "@/components/common/MetricTooltip";
import DeepAnalysisSection from "@/components/narrative/DeepAnalysisSection";
import StructuredAIAnalysisSection from "@/components/narrative/StructuredAIAnalysisSection";
import SignalIntelligenceSection from "@/components/narrative/SignalIntelligenceSection";
import { CatalystsSection, CoordinationRiskSection, EvidenceSection, MarketImpactSection, SourceCoverageSection } from "./components/NarrativeDetailSections";

export default function NarrativeDetailPage() {
  const params = useParams();
  const id = params?.id as string;

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
    if (!id) return;
    setExporting(true);
    try {
      const blob = await exportNarrative(id);
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

        <DeepAnalysisSection sonnetAnalysis={narrative.sonnet_analysis} />
        <StructuredAIAnalysisSection analysis={analysis} onRefresh={() => handleAnalyze(true)} />

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

        <MarketImpactSection correlations={correlations} />

        <SourceCoverageSection sources={sources} />

        <SignalIntelligenceSection signal={narrative.signal} />

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

        <CoordinationRiskSection coordination={narrative.coordination} />

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

        <EvidenceSection signals={narrative.signals} />

        <CatalystsSection catalysts={narrative.catalysts} />

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

