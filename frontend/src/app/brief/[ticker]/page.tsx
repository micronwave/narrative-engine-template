"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { AlertTriangle, TrendingUp, TrendingDown, ArrowLeftRight, HelpCircle, Shield, ArrowLeft, Bell, X } from "lucide-react";
import Link from "next/link";
import { fetchBrief, fetchPriceHistory, createAlertRule, type TickerBrief, type PriceHistoryResponse } from "@/lib/api";
import CandlestickChart from "@/components/CandlestickChart";
import TimeframeSelector from "@/components/TimeframeSelector";
import IndicatorOverlay, { type IndicatorConfig, DEFAULT_INDICATORS } from "@/components/IndicatorOverlay";
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

function SetAlertModal({
  ticker,
  currentPrice,
  onClose,
}: {
  ticker: string;
  currentPrice: number | null;
  onClose: () => void;
}) {
  const ALERT_TYPES = [
    { value: "price_above", label: "Price Above" },
    { value: "price_below", label: "Price Below" },
    { value: "rsi_overbought", label: "RSI Overbought" },
    { value: "rsi_oversold", label: "RSI Oversold" },
  ];
  const [ruleType, setRuleType] = useState("price_above");
  const [threshold, setThreshold] = useState(
    currentPrice ? String(Math.round(currentPrice * 1.05 * 100) / 100) : "0"
  );
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const quickSets = currentPrice
    ? [
        { label: "+5%", value: Math.round(currentPrice * 1.05 * 100) / 100 },
        { label: "+10%", value: Math.round(currentPrice * 1.10 * 100) / 100 },
        { label: "-5%", value: Math.round(currentPrice * 0.95 * 100) / 100 },
        { label: "-10%", value: Math.round(currentPrice * 0.90 * 100) / 100 },
      ]
    : [];

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const targetType = ruleType.startsWith("rsi") ? "ticker" : "ticker";
      await createAlertRule(ruleType, targetType, ticker, parseFloat(threshold) || 0);
      setSaved(true);
      setTimeout(onClose, 1000);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create alert");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div
        className="bg-surface border border-[var(--bg-border)] rounded-sm w-full max-w-sm mx-4 p-5"
        style={{ boxShadow: "0 4px 32px rgba(0,0,0,0.5)" }}
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-[14px] font-semibold text-text-primary">
            Set Alert — {ticker}
          </h3>
          <button onClick={onClose} className="text-text-muted hover:text-text-primary transition-colors">
            <X size={16} />
          </button>
        </div>

        <div className="flex flex-col gap-3">
          <div>
            <label className="label-micro mb-1 block">Alert Type</label>
            <select
              value={ruleType}
              onChange={(e) => setRuleType(e.target.value)}
              className="w-full bg-inset border border-[var(--bg-border)] rounded-sm font-mono text-[12px] text-text-primary px-2 py-1.5"
            >
              {ALERT_TYPES.map((t) => (
                <option key={t.value} value={t.value}>{t.label}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="label-micro mb-1 block">
              {ruleType.startsWith("rsi") ? "RSI Level" : "Price ($)"}
            </label>
            {quickSets.length > 0 && !ruleType.startsWith("rsi") && (
              <div className="flex gap-1 mb-1.5 flex-wrap">
                {quickSets.map((q) => (
                  <button
                    key={q.label}
                    onClick={() => setThreshold(String(q.value))}
                    className="font-mono text-[10px] px-1.5 py-0.5 border border-[var(--bg-border)] rounded-sm text-text-muted hover:text-text-primary transition-colors"
                  >
                    {q.label}
                  </button>
                ))}
              </div>
            )}
            <input
              type="number"
              value={threshold}
              onChange={(e) => setThreshold(e.target.value)}
              className="w-full bg-inset border border-[var(--bg-border)] rounded-sm font-mono text-[12px] text-text-primary px-2 py-1.5"
            />
          </div>

          {error && <p className="font-mono text-[11px] text-bearish">{error}</p>}
          {saved && <p className="font-mono text-[11px] text-bullish">Alert created!</p>}

          <div className="flex gap-2 mt-1">
            <button
              onClick={handleSave}
              disabled={saving || saved}
              className="flex-1 bg-accent-muted text-accent-text font-mono text-[12px] py-1.5 rounded-sm hover:opacity-80 transition-opacity disabled:opacity-50"
            >
              {saving ? "Saving…" : saved ? "Saved!" : "Create Alert"}
            </button>
            <button
              onClick={onClose}
              className="px-3 border border-[var(--bg-border)] text-text-muted font-mono text-[12px] py-1.5 rounded-sm hover:text-text-primary transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function BriefPage() {
  const params = useParams();
  const ticker = (params.ticker as string) || "";

  const [brief, setBrief] = useState<TickerBrief | null>(null);
  const [priceHistory, setPriceHistory] = useState<PriceHistoryResponse | null>(null);
  const [selectedDays, setSelectedDays] = useState(30);
  const [indicators, setIndicators] = useState<IndicatorConfig[]>(DEFAULT_INDICATORS);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAlertModal, setShowAlertModal] = useState(false);

  useEffect(() => {
    if (!ticker) return;
    fetchPriceHistory(ticker, selectedDays)
      .then(setPriceHistory)
      .catch(() => setPriceHistory(null));
  }, [ticker, selectedDays]);

  useEffect(() => {
    if (!ticker) return;
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
            <div className="flex items-center gap-3">
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
              <button
                onClick={() => setShowAlertModal(true)}
                className="flex items-center gap-1.5 font-mono text-[11px] text-text-muted hover:text-accent-text transition-colors border border-[var(--bg-border)] px-2 py-1 rounded-sm"
                title="Set price alert"
              >
                <Bell size={12} />
                Set Alert
              </button>
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
        <section className="mt-8 mb-2">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
              Price History
            </h2>
            <TimeframeSelector selected={selectedDays} onChange={setSelectedDays} />
          </div>
          {priceHistory && priceHistory.available && priceHistory.data.length > 1 ? (
            <>
              <CandlestickChart symbol={ticker} data={priceHistory.data} height={320} indicators={indicators} />
              <IndicatorOverlay data={priceHistory.data} onIndicatorsChange={setIndicators} />
            </>
          ) : (
            <div className="flex items-center justify-center text-text-tertiary text-xs bg-inset rounded-sm h-20">
              {priceHistory === null ? "Loading…" : "No price data available"}
            </div>
          )}
        </section>

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

      {showAlertModal && (
        <SetAlertModal
          ticker={ticker}
          currentPrice={security?.current_price ?? null}
          onClose={() => setShowAlertModal(false)}
        />
      )}
    </main>
  );
}
