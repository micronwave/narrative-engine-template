"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { ArrowLeft, CheckCircle, XCircle, Clock } from "lucide-react";
import Link from "next/link";
import {
  fetchCorrelation,
  fetchBrief,
  type CorrelationResult,
  type BriefNarrative,
} from "@/lib/api";
import SegmentedControl from "@/components/common/SegmentedControl";
import Skeleton from "@/components/common/Skeleton";

export default function CorrelationPage() {
  return (
    <Suspense fallback={<div className="min-h-screen bg-base" />}>
      <CorrelationContent />
    </Suspense>
  );
}

function CorrelationContent() {
  const searchParams = useSearchParams();
  const ticker = searchParams.get("ticker") || "";
  const narrativeId = searchParams.get("narrative") || "";

  const [results, setResults] = useState<CorrelationResult[]>([]);
  const [narratives, setNarratives] = useState<BriefNarrative[]>([]);
  const [loading, setLoading] = useState(true);
  const [leadDays, setLeadDays] = useState(1);

  // Fetch brief once when ticker/narrative changes
  useEffect(() => {
    if (!ticker) {
      setLoading(false);
      return;
    }
    setLoading(true);
    fetchBrief(ticker)
      .then((brief) => setNarratives(brief.narratives))
      .catch(() => setNarratives([]))
      .finally(() => setLoading(false));
  }, [ticker]);

  // Fetch correlations when narratives or leadDays change
  useEffect(() => {
    if (!ticker || narratives.length === 0) return;
    const targets = narrativeId
      ? narratives.filter((n) => n.id === narrativeId)
      : narratives;
    if (targets.length === 0) { setResults([]); return; }

    setLoading(true);
    Promise.all(
      targets.map((nar) =>
        fetchCorrelation(nar.id, ticker, leadDays).catch(() => ({
          correlation: 0,
          p_value: 1,
          n_observations: 0,
          is_significant: false,
          lead_days: leadDays,
          interpretation: "Unable to compute",
          narrative_id: nar.id,
          ticker,
        }))
      )
    )
      .then(setResults)
      .catch(() => setResults([]))
      .finally(() => setLoading(false));
  }, [narratives, narrativeId, ticker, leadDays]);

  function corrColor(r: number): string {
    const abs = Math.abs(r);
    if (abs >= 0.5) return "text-alert";
    if (abs >= 0.3) return "text-bullish";
    return "text-text-tertiary";
  }

  if (!ticker) {
    return (
      <main className="min-h-screen bg-base text-text-primary">
        <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16 text-center py-16">
          <h1 className="text-[22px] font-semibold text-text-primary font-display mb-4" style={{ letterSpacing: "-0.01em" }}>
            Velocity-Price Correlation
          </h1>
          <p className="text-text-secondary text-[13px]">
            Access this page from an{" "}
            <Link href="/brief" className="text-accent-text hover:text-text-primary transition-colors">
              Intelligence Brief
            </Link>{" "}
            to analyze narrative velocity vs. price correlation.
          </p>
        </div>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Back link */}
        <Link
          href={`/brief/${ticker}`}
          className="font-mono text-[12px] text-text-muted hover:text-text-primary transition-colors inline-flex items-center gap-1 mb-4"
        >
          <ArrowLeft size={12} /> Back to {ticker} Brief
        </Link>

        <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
          {ticker} Velocity-Price Correlation
        </h1>
        <p className="font-mono text-[11px] text-text-muted mt-1">
          Does narrative momentum predict price movement?
        </p>

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {/* Lead days selector */}
        <div className="flex items-center gap-3 mb-8">
          <span className="font-mono text-[10px] uppercase text-text-muted">Lead time</span>
          <SegmentedControl
            options={["1d", "2d", "3d", "5d"]}
            activeOption={`${leadDays}d`}
            onChange={(opt) => setLeadDays(parseInt(opt))}
          />
        </div>

        {/* Results */}
        {loading ? (
          <div className="flex flex-col gap-3">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} height={80} />
            ))}
          </div>
        ) : results.length === 0 ? (
          <p className="font-mono text-[12px] text-text-muted py-8 text-center">
            No narratives linked to {ticker}.
          </p>
        ) : (
          <div className="flex flex-col" data-testid="correlation-results">
            {results.map((r) => {
              const nar = narratives.find((n) => n.id === r.narrative_id);
              const collecting = r.n_observations < 30;

              return (
                <div
                  key={r.narrative_id}
                  className="py-4"
                  style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1">
                      <Link
                        href={`/narrative/${r.narrative_id}`}
                        className="text-accent-text text-[13px] font-semibold hover:text-text-primary transition-colors"
                      >
                        {nar?.name || r.narrative_id}
                      </Link>
                      <div className="flex items-center gap-3 mt-2">
                        {/* Correlation coefficient */}
                        <span
                          className={`font-data-large ${corrColor(r.correlation)}`}
                        >
                          {r.correlation.toFixed(3)}
                        </span>

                        {/* Significance indicator */}
                        {collecting ? (
                          <span className="flex items-center gap-1 text-text-muted font-mono text-[11px]">
                            <Clock size={12} />
                            {r.n_observations}/30 days
                          </span>
                        ) : r.is_significant ? (
                          <span className="flex items-center gap-1 text-bullish font-mono text-[11px]">
                            <CheckCircle size={12} />
                            p={r.p_value.toFixed(3)}
                          </span>
                        ) : (
                          <span className="flex items-center gap-1 text-text-muted font-mono text-[11px]">
                            <XCircle size={12} />
                            p={r.p_value.toFixed(3)}
                          </span>
                        )}

                        <span className="font-mono text-[10px] text-text-muted">
                          {r.lead_days}d lead
                        </span>
                      </div>
                    </div>

                    {/* Direction badge */}
                    {nar && (
                      <span
                        className={`font-mono text-[11px] font-medium px-2 py-0.5 rounded-sm ${
                          nar.direction === "bullish"
                            ? "bg-bullish-bg text-bullish"
                            : nar.direction === "bearish"
                            ? "bg-bearish-bg text-bearish"
                            : "bg-alert-bg text-alert"
                        }`}
                      >
                        {nar.direction}
                      </span>
                    )}
                  </div>

                  {/* Interpretation */}
                  <p className="text-text-secondary text-[12px] mt-2">
                    {r.interpretation}
                  </p>
                </div>
              );
            })}
          </div>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            <span>{ticker} · {leadDays}d lead</span>
          </div>
        </div>
      </div>
    </main>
  );
}
