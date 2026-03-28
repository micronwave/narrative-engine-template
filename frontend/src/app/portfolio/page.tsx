"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { fetchPortfolio, addPortfolioHolding, removePortfolioHolding, fetchPortfolioExposure } from "@/lib/api";
import type { PortfolioHolding, NarrativeExposure } from "@/lib/api";
import { Trash2, Plus } from "lucide-react";
import StageBadge from "@/components/common/StageBadge";
import Skeleton from "@/components/common/Skeleton";

function velColor(v: number): string {
  if (v > 5) return "var(--vel-accelerating)";
  if (v < -0.5) return "var(--vel-decelerating)";
  return "var(--vel-stable)";
}

export default function PortfolioPage() {
  const [holdings, setHoldings] = useState<PortfolioHolding[]>([]);
  const [exposures, setExposures] = useState<NarrativeExposure[]>([]);
  const [loading, setLoading] = useState(true);
  const [ticker, setTicker] = useState("");

  function refresh() {
    fetchPortfolio()
      .then((d) => setHoldings(d.holdings))
      .catch(() => setHoldings([]));
    fetchPortfolioExposure()
      .then((d) => setExposures(d.exposures))
      .catch(() => setExposures([]));
  }

  useEffect(() => {
    setLoading(true);
    Promise.all([fetchPortfolio(), fetchPortfolioExposure()])
      .then(([p, e]) => {
        setHoldings(p.holdings);
        setExposures(e.exposures);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  async function handleAdd() {
    if (!ticker.trim()) return;
    await addPortfolioHolding(ticker.trim().toUpperCase());
    setTicker("");
    refresh();
  }

  async function handleRemove(holdingId: string) {
    await removePortfolioHolding(holdingId);
    refresh();
  }

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
          Portfolio
        </h1>
        <p className="font-mono text-[10px] uppercase tracking-[0.06em] text-text-muted mt-1">
          Track narrative exposure for your holdings
        </p>

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {/* Add ticker */}
        <div className="flex gap-2 mb-6">
          <input
            type="text"
            placeholder="Add ticker (e.g. AAPL)"
            value={ticker}
            onChange={(e) => setTicker(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleAdd()}
            className="font-mono text-[13px] px-3 py-1.5 bg-transparent border border-[var(--bg-border)] text-text-primary rounded-sm w-[200px] outline-none"
          />
          <button
            onClick={handleAdd}
            className="font-mono text-[11px] px-3.5 py-1.5 bg-accent-primary text-text-primary border-none cursor-pointer flex items-center gap-1 rounded-sm"
          >
            <Plus size={14} /> Add
          </button>
        </div>

        {loading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            <div className="flex flex-col gap-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} height={44} />)}
            </div>
            <div className="flex flex-col gap-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} height={56} />)}
            </div>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            {/* Holdings */}
            <div>
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-3">
                Holdings
                <span className="font-mono text-[11px] font-normal text-text-muted ml-2">{holdings.length}</span>
              </h2>
              {holdings.length === 0 ? (
                <p className="font-mono text-[12px] text-text-muted py-5">
                  No holdings yet. Add a ticker above to start tracking narrative exposure.
                </p>
              ) : (
                <div className="flex flex-col">
                  {holdings.map((h) => (
                    <div
                      key={h.id}
                      className="flex items-center justify-between px-4 py-2.5 transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)]"
                      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                    >
                      <div>
                        <span className="font-mono text-[14px] font-bold text-text-primary">
                          {h.ticker}
                        </span>
                        {h.current_price != null && (
                          <span className="font-mono text-[12px] text-text-muted ml-3">
                            ${h.current_price.toFixed(2)}
                          </span>
                        )}
                        {h.price_change_24h != null && (
                          <span className={`font-mono text-[11px] ml-1.5 ${h.price_change_24h >= 0 ? "text-bullish" : "text-bearish"}`}>
                            {h.price_change_24h >= 0 ? "+" : ""}{h.price_change_24h.toFixed(2)}%
                          </span>
                        )}
                      </div>
                      <button
                        onClick={() => handleRemove(h.id)}
                        className="bg-transparent border-none text-text-muted cursor-pointer p-1 hover:text-bearish transition-colors"
                        aria-label={`Remove ${h.ticker}`}
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Narrative Exposure */}
            <div>
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-3">
                Narrative Exposure
                <span className="font-mono text-[11px] font-normal text-text-muted ml-2">{exposures.length}</span>
              </h2>
              {exposures.length === 0 ? (
                <p className="font-mono text-[12px] text-text-muted py-5">
                  {holdings.length === 0
                    ? "Add holdings to see which narratives affect your portfolio."
                    : "No narratives currently linked to your holdings."}
                </p>
              ) : (
                <div className="flex flex-col">
                  {exposures.map((e) => (
                    <Link
                      key={e.narrative_id}
                      href={`/narrative/${e.narrative_id}`}
                      className="no-underline"
                    >
                      <div
                        className="px-4 py-2.5 cursor-pointer transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)]"
                        style={{
                          borderBottom: "1px solid rgba(56, 62, 71, 0.13)",
                          borderLeft: `3px solid ${velColor(e.velocity)}`,
                        }}
                      >
                        <div className="flex justify-between items-baseline">
                          <span className="text-[13px] text-text-primary font-display">
                            {e.narrative_name}
                          </span>
                          <span className="font-mono text-[14px] font-bold" style={{ color: velColor(e.velocity) }}>
                            {e.velocity > 0 ? "+" : ""}{e.velocity.toFixed(1)}
                          </span>
                        </div>
                        <div className="flex gap-2 mt-1">
                          <StageBadge stage={e.stage} />
                          {e.affected_tickers.map((t) => (
                            <span key={t} className="font-mono text-[10px] text-text-muted border border-[var(--bg-border)] px-1.5 py-[1px] rounded-sm">
                              {t}
                            </span>
                          ))}
                        </div>
                      </div>
                    </Link>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            <span>{holdings.length} holdings</span>
          </div>
        </div>
      </div>
    </main>
  );
}
