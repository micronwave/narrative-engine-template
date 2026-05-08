"use client";

import type { DeepAnalysis } from "@/lib/api";

type StructuredAIAnalysisSectionProps = {
  analysis: DeepAnalysis | null;
  onRefresh: () => void;
};

export default function StructuredAIAnalysisSection({
  analysis,
  onRefresh,
}: StructuredAIAnalysisSectionProps) {
  if (!analysis) return null;

  return (
    <section className="mb-10" data-testid="deep-analysis-section">
      <div className="flex items-baseline gap-3 mb-4">
        <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
          AI Analysis
        </h2>
        <span className="font-mono text-[11px] text-text-muted">
          Haiku {analysis.cached ? "· cached" : "· fresh"} ·{" "}
          {new Date(analysis.analyzed_at).toLocaleDateString()}
        </span>
        {analysis.cached && (
          <button
            onClick={onRefresh}
            className="font-mono text-[10px] text-accent-text hover:text-text-primary transition-colors"
            data-testid="refresh-analysis-btn"
          >
            Refresh
          </button>
        )}
      </div>

      <div
        className="font-display text-[13px] text-text-secondary leading-[1.7] border-l-[3px] pl-5 mb-6"
        style={{ borderColor: "var(--accent-primary)" }}
        data-testid="analysis-thesis"
      >
        {analysis.thesis}
      </div>

      {analysis.key_drivers.length > 0 && (
        <div className="mb-4">
          <div className="font-mono text-[10px] uppercase text-text-muted mb-2 tracking-[0.06em]">
            Key Drivers
          </div>
          <ul className="flex flex-col gap-1">
            {analysis.key_drivers.map((driver, index) => (
              <li
                key={index}
                className="font-mono text-[12px] text-text-secondary pl-3 border-l"
                style={{ borderColor: "var(--bg-border)" }}
              >
                {driver}
              </li>
            ))}
          </ul>
        </div>
      )}

      {analysis.asset_impact.length > 0 && (
        <div className="mb-4">
          <div className="font-mono text-[10px] uppercase text-text-muted mb-2 tracking-[0.06em]">
            Asset Impact
          </div>
          {analysis.asset_impact.map((assetImpact, index) => (
            <div
              key={index}
              className="flex gap-2 py-1"
              style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
            >
              <span className="font-mono text-[12px] text-text-primary font-semibold shrink-0">
                {assetImpact.asset}
              </span>
              <span className="font-mono text-[11px] text-text-secondary">
                {assetImpact.impact}
              </span>
            </div>
          ))}
        </div>
      )}

      {analysis.risk_factors.length > 0 && (
        <div className="mb-4">
          <div className="font-mono text-[10px] uppercase text-text-muted mb-2 tracking-[0.06em]">
            Risk Factors
          </div>
          <ul className="flex flex-col gap-1">
            {analysis.risk_factors.map((riskFactor, index) => (
              <li
                key={index}
                className="font-mono text-[12px] text-text-secondary pl-3 border-l"
                style={{ borderColor: "var(--intent-danger)" }}
              >
                {riskFactor}
              </li>
            ))}
          </ul>
        </div>
      )}

      {analysis.historical_comparison && (
        <div className="mt-4 font-mono text-[11px] text-text-muted italic">
          {analysis.historical_comparison}
        </div>
      )}
    </section>
  );
}
