"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { AlertTriangle, TrendingUp, TrendingDown, ArrowLeftRight, HelpCircle, Shield, ArrowLeft } from "lucide-react";
import Link from "next/link";
import { fetchBrief, fetchPriceHistory, type TickerBrief, type PriceHistoryResponse } from "@/lib/api";
import HistoryChart from "@/components/HistoryChart";
import Skeleton from "@/components/common/Skeleton";

function DirectionBadge({ direction }: { direction: string }) {
  const config: Record<string, { icon: typeof TrendingUp; cls: string }> = {
    bullish: { icon: TrendingUp, cls: "text-bullish bg-bullish-bg" },
    bearish: { icon: TrendingDown, cls: "text-bearish bg-bearish-bg" },
    mixed: { icon: ArrowLeftRight, cls: "text-alert bg-alert-bg" },
    uncertain: { icon: HelpCircle, cls: "text-text-tertiary bg-inset" },
  };
  const { icon: Icon, cls } = config[direction] || config.uncertain;
  return (
    <span className={`inline-flex items-center gap-1 font-mono text-[11px] px-2 py-0.5 rounded-sm font-medium ${cls}`}>
      <Icon size={12} />
      {direction}
    </span>
  );
}

function stageBadgeClass(stage: string): string {
  if (stage === "Growing") return "bg-bullish-bg text-bullish";
  if (stage === "Mature") return "bg-alert-bg text-alert";
  if (stage === "Declining") return "bg-bearish-bg text-bearish";
  if (stage === "Dormant") return "bg-surface-hover text-text-tertiary";
  return "bg-accent-muted text-accent-text";
}

export default function BriefPage() {
  const params = useParams();
  const ticker = (params.ticker as string) || "";

  const [brief, setBrief] = useState<TickerBrief | null>(null);
  const [priceHistory, setPriceHistory] = useState<PriceHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!ticker) return;
    fetchPriceHistory(ticker, 30)
      .then(setPriceHistory)
      .catch(() => setPriceHistory(null));
    fetchBrief(ticker)
      .then(setBrief)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [ticker]);

  if (loading) {
    return (
      <main className="min-h-screen bg-base text-text-primary">
        <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
          <Skeleton width={256} height={28} className="mb-4" />
          <Skeleton width={384} height={16} className="mb-8" />
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {[1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} height={64} />
            ))}
          </div>
        </div>
      </main>
    );
  }

  if (error || !brief) {
    return (
      <main className="min-h-screen bg-base text-text-primary">
        <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16 text-center py-16">
          <p className="font-mono text-[12px] text-bearish">{error || "Brief not found"}</p>
        </div>
      </main>
    );
  }

  const { security, narratives, risk_summary } = brief;

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Back link */}
        <Link
          href="/brief"
          className="font-mono text-[12px] text-text-muted hover:text-text-primary transition-colors inline-flex items-center gap-1 mb-4"
        >
          <ArrowLeft size={12} /> All Briefs
        </Link>

        {/* Header */}
        <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
          {brief.ticker} Intelligence Brief
        </h1>
        {security && (
          <p className="font-mono text-[11px] text-text-muted mt-1">
            Narrative intelligence report for {security.name}
          </p>
        )}

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {/* Security header */}
        {security && (
          <div
            data-testid="brief-security-header"
            className="flex items-center justify-between py-4"
            style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
          >
            <div>
              <span className="font-mono text-[16px] font-semibold text-text-primary">
                {security.symbol}
              </span>
              <span className="text-text-secondary text-[13px] ml-3">{security.name}</span>
              <span className="text-text-muted text-[12px] ml-2">{security.exchange}</span>
            </div>
            <div className="text-right">
              {security.current_price !== null ? (
                <>
                  <span className="font-mono text-[16px] text-text-primary">
                    ${security.current_price.toLocaleString("en-US", {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })}
                  </span>
                  {security.price_change_24h !== null && (
                    <span
                      className={`font-mono text-[13px] ml-2 ${
                        security.price_change_24h >= 0 ? "text-bullish" : "text-bearish"
                      }`}
                    >
                      {security.price_change_24h >= 0 ? "+" : "\u2212"}
                      {Math.abs(security.price_change_24h).toFixed(2)}%
                    </span>
                  )}
                </>
              ) : (
                <span className="text-text-muted">—</span>
              )}
            </div>
          </div>
        )}

        {/* Risk Summary */}
        <div data-testid="brief-risk-summary" className="flex items-center gap-5 mt-6 flex-wrap">
          <div>
            <div className="font-data-large text-accent-text">
              {risk_summary.narrative_count}
            </div>
            <div className="label-micro">Narratives</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <DirectionBadge direction={risk_summary.dominant_direction} />
            <div className="label-micro mt-1">Direction</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <div className="font-data-large text-text-primary">
              {risk_summary.avg_entropy.toFixed(2)}
            </div>
            <div className="label-micro">Avg Diversity</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <div className={`font-data-large ${risk_summary.coordination_detected ? "text-critical" : "text-bullish"}`}>
              {risk_summary.coordination_detected ? (
                <span className="flex items-center gap-1">
                  <Shield size={16} /> Yes
                </span>
              ) : "None"}
            </div>
            <div className="label-micro">Coordination</div>
          </div>
          <div className="w-px h-6 bg-[var(--bg-border)]" />
          <div>
            <div className="font-data-large text-text-primary">
              {risk_summary.highest_burst_ratio.toFixed(1)}x
            </div>
            <div className="label-micro">Peak Burst</div>
          </div>
        </div>

        {/* Entropy assessment */}
        <p className="text-text-secondary text-[12px] mt-3 cursor-help" title="Based on average entropy across all linked narratives">
          {risk_summary.entropy_assessment}
        </p>

        {/* Price History Chart */}
        {priceHistory && priceHistory.available && priceHistory.data.length > 1 && (
          <section className="mt-8 mb-2">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-3">
              Price History
              <span className="font-mono text-[11px] font-normal text-text-muted ml-2">30d</span>
            </h2>
            <HistoryChart
              data={priceHistory.data.map((p) => ({ date: p.date, value: p.close }))}
              color="var(--accent-primary)"
            />
          </section>
        )}

        {/* Affecting Narratives */}
        <div data-testid="brief-narratives" className="mt-8">
          <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-4">
            Affecting Narratives
            <span className="font-mono text-[11px] font-normal text-text-muted ml-2">{narratives.length} linked</span>
          </h2>

          {narratives.length === 0 ? (
            <p className="font-mono text-[12px] text-text-muted">No narratives currently affect this ticker.</p>
          ) : (
            <div className="flex flex-col">
              {narratives.map((nar) => (
                <div
                  key={nar.id}
                  className="py-4"
                  style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                >
                  <div className="flex items-start justify-between gap-2 mb-2">
                    <a
                      href={`/narrative/${nar.id}`}
                      className="text-accent-text text-[13px] font-semibold hover:text-text-primary transition-colors"
                    >
                      {nar.name}
                    </a>
                    <div className="flex items-center gap-2 shrink-0">
                      <span className={`font-mono text-[11px] font-medium px-2 py-[2px] rounded-sm tracking-[0.02em] ${stageBadgeClass(nar.stage)}`}>
                        {nar.stage.toLowerCase()}
                      </span>
                      <DirectionBadge direction={nar.direction} />
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-4 font-mono text-[10px] text-text-muted">
                    <span className="cursor-help" title="Narrative momentum">
                      velocity {nar.velocity_windowed.toFixed(3)}
                    </span>
                    <span className="cursor-help" title={nar.entropy_interpretation}>
                      diversity {nar.entropy !== null ? nar.entropy.toFixed(3) : "—"}
                    </span>
                    <span>
                      {nar.signal_count} signals · {nar.days_active}d active
                    </span>
                    <span className="cursor-help" title="Exposure score">
                      exposure {(nar.exposure_score * 100).toFixed(0)}%
                    </span>
                    {nar.coordination_flags > 0 && (
                      <span className="text-critical flex items-center gap-1">
                        <AlertTriangle size={11} />
                        {nar.coordination_flags} flag{nar.coordination_flags !== 1 ? "s" : ""}
                      </span>
                    )}
                  </div>

                  {/* Correlation link */}
                  <a
                    href="/market-impact"
                    className="inline-block mt-2 text-accent-text text-[12px] hover:text-text-primary transition-colors"
                    data-testid={`correlation-link-${nar.id}`}
                  >
                    View velocity-price correlation →
                  </a>

                  {/* Entropy interpretation */}
                  <p className="text-text-secondary text-[12px] mt-2">
                    {nar.entropy_interpretation}
                  </p>

                  {/* Top signals */}
                  {nar.top_signals.length > 0 && (
                    <div className="mt-3 pt-2" style={{ borderTop: "1px solid rgba(56, 62, 71, 0.13)" }}>
                      <p className="font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted mb-1">Top Signals</p>
                      <ul className="flex flex-col gap-1">
                        {nar.top_signals.map((sig, i) => (
                          <li key={i} className="text-[12px] text-text-secondary line-clamp-1">
                            {sig.headline || "(no headline)"}
                            {sig.source && <span className="text-text-muted ml-1">· {sig.source}</span>}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            <span>{brief.ticker}</span>
          </div>
        </div>
      </div>
    </main>
  );
}
