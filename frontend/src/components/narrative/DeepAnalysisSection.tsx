"use client";

type DeepAnalysisSectionProps = {
  sonnetAnalysis?: string | null;
};

export default function DeepAnalysisSection({ sonnetAnalysis }: DeepAnalysisSectionProps) {
  if (!sonnetAnalysis) return null;

  return (
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
        {sonnetAnalysis}
      </div>
    </section>
  );
}
