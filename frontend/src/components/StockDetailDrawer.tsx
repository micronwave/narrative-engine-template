"use client";

import { useEffect, useRef, useState } from "react";
import { X, TrendingUp, TrendingDown, ArrowLeftRight, HelpCircle } from "lucide-react";
import type { StockDetail, PriceHistoryResponse } from "@/lib/api";
import { fetchPriceHistory } from "@/lib/api";
import CandlestickChart from "@/components/CandlestickChart";
import TimeframeSelector from "@/components/TimeframeSelector";

type Props = {
  isOpen: boolean;
  stockDetail: StockDetail | null;
  loading?: boolean;
  onClose: () => void;
};

function DirectionIcon({ direction }: { direction: string }) {
  if (direction === "bullish") return <TrendingUp size={12} className="text-bullish" />;
  if (direction === "bearish") return <TrendingDown size={12} className="text-bearish" />;
  if (direction === "mixed") return <ArrowLeftRight size={12} className="text-alert" />;
  return <HelpCircle size={12} className="text-text-tertiary" />;
}

function directionColor(direction: string): string {
  if (direction === "bullish") return "text-bullish bg-bullish-bg";
  if (direction === "bearish") return "text-bearish bg-bearish-bg";
  if (direction === "mixed") return "text-alert bg-alert-bg";
  return "text-text-tertiary bg-inset";
}

export default function StockDetailDrawer({ isOpen, stockDetail, loading, onClose }: Props) {
  const drawerRef = useRef<HTMLDivElement>(null);
  const [priceHistory, setPriceHistory] = useState<PriceHistoryResponse | null>(null);
  const [chartDays, setChartDays] = useState(30);

  useEffect(() => {
    if (!isOpen || !stockDetail) return;
    fetchPriceHistory(stockDetail.symbol, chartDays)
      .then(setPriceHistory)
      .catch(() => setPriceHistory(null));
  }, [isOpen, stockDetail, chartDays]);

  // Focus trap + Escape key
  useEffect(() => {
    if (!isOpen) return;

    function trapFocus(e: KeyboardEvent) {
      const focusable = drawerRef.current?.querySelectorAll<HTMLElement>(
        'button, a, [tabindex]:not([tabindex="-1"])'
      );
      if (!focusable || focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];

      if (e.key === "Tab") {
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
      if (e.key === "Escape") onClose();
    }

    document.addEventListener("keydown", trapFocus);
    const timer = setTimeout(() => {
      drawerRef.current?.querySelector<HTMLElement>("button, a, [tabindex]")?.focus();
    }, 50);

    return () => {
      document.removeEventListener("keydown", trapFocus);
      clearTimeout(timer);
    };
  }, [isOpen, onClose]);

  return (
    <>
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/60 z-40"
          onClick={onClose}
          aria-hidden="true"
        />
      )}

      <div
        ref={drawerRef}
        data-testid="stock-detail-drawer"
        role="dialog"
        aria-modal="true"
        aria-label={`Security detail: ${stockDetail?.symbol ?? "loading"}`}
        className={`fixed top-0 right-0 h-full w-full max-w-md z-50 flex flex-col shadow-xl transition-transform duration-slow ${
          isOpen ? "translate-x-0" : "translate-x-full"
        }`}
        style={{ background: 'var(--bg-surface)', borderLeft: '1px solid var(--bg-border)' }}
      >
        {/* Header */}
        <div className="flex items-start justify-between p-5 border-b border-border-subtle sticky top-0" style={{ background: 'var(--bg-surface)' }}>
          <div className="flex-1 min-w-0">
            {stockDetail ? (
              <>
                <h2 className="text-text-primary font-semibold text-lg font-mono-data font-display">
                  {stockDetail.symbol}
                </h2>
                <p className="text-text-secondary text-xs mt-0.5 line-clamp-1">{stockDetail.name}</p>
              </>
            ) : (
              <h2 className="text-text-tertiary text-sm">{loading ? "Loading\u2026" : "Security"}</h2>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-text-tertiary hover:text-text-primary transition-colors ml-3 shrink-0"
            aria-label="Close security detail drawer"
          >
            <X size={18} />
          </button>
        </div>

        {/* Brief link */}
        {stockDetail && (
          <a
            href={`/brief/${stockDetail.symbol}`}
            className="block px-5 mt-3 text-accent-text text-xs font-medium hover:text-text-primary transition-colors"
            data-testid="brief-link"
          >
            Intelligence Brief →
          </a>
        )}

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-5 flex flex-col gap-5">
          {loading && !stockDetail && (
            <div className="text-text-tertiary text-sm text-center py-8">Loading&hellip;</div>
          )}

          {stockDetail && (
            <>
              {/* Price */}
              <div>
                <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">Current Price</p>
                <p className="font-mono-data text-text-primary text-2xl font-semibold">
                  {stockDetail.current_price !== null
                    ? `$${stockDetail.current_price.toLocaleString("en-US", {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      })}`
                    : "\u2014"}
                </p>
                {stockDetail.price_change_24h !== null && (
                  <p
                    className={`font-mono-data text-sm mt-0.5 ${
                      stockDetail.price_change_24h >= 0 ? "text-bullish" : "text-bearish"
                    }`}
                  >
                    {stockDetail.price_change_24h >= 0 ? "+" : "\u2212"}
                    {Math.abs(stockDetail.price_change_24h).toFixed(2)}% today
                  </p>
                )}
              </div>

              {/* Mini Candlestick Chart */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2">Price Chart</p>
                  <TimeframeSelector selected={chartDays} onChange={setChartDays} small />
                </div>
                {priceHistory && priceHistory.available && priceHistory.data.length > 1 ? (
                  <CandlestickChart symbol={stockDetail.symbol} data={priceHistory.data} height={200} />
                ) : (
                  <div className="flex items-center justify-center text-text-tertiary text-xs bg-inset rounded-sm h-14">
                    {priceHistory === null ? "Loading…" : "No data"}
                  </div>
                )}
              </div>

              {/* Meta */}
              <div className="flex items-center gap-3 text-xs text-text-tertiary">
                <span>{stockDetail.exchange}</span>
                <span>&middot;</span>
                <span>Impact score: <span className="font-mono-data text-text-secondary">{stockDetail.narrative_impact_score}</span></span>
              </div>

              {/* Affecting Narratives */}
              <div data-testid="affecting-narratives">
                <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
                  Affecting Narratives
                </p>
                {stockDetail.narratives.length === 0 ? (
                  <p className="text-text-disabled text-xs">No narratives affect this security.</p>
                ) : (
                  <ul className="flex flex-col gap-3">
                    {stockDetail.narratives.map((nar) => (
                      <li
                        key={nar.narrative_id}
                        className="py-3"
                        style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                        data-testid={`affecting-narrative-${nar.narrative_id}`}
                      >
                        <a
                          href={`/narrative/${nar.narrative_id}`}
                          className="text-accent-text text-xs font-medium hover:text-accent-hover transition-colors"
                          data-testid={`narrative-link-${nar.narrative_id}`}
                        >
                          {nar.narrative_name}
                        </a>
                        <div className="flex items-center gap-2 mt-1.5">
                          <div className="flex-1 h-1 bg-inset overflow-hidden">
                            <div
                              className="h-full bg-accent-primary"
                              style={{ width: `${nar.exposure_score * 100}%` }}
                            />
                          </div>
                          <span className="font-mono-data text-text-tertiary text-xs">
                            {(nar.exposure_score * 100).toFixed(0)}%
                          </span>
                          <span
                            className={`flex items-center gap-1 text-xs px-1.5 py-0.5 rounded ${directionColor(nar.direction)}`}
                            data-testid={`direction-badge-${nar.narrative_id}`}
                          >
                            <DirectionIcon direction={nar.direction} />
                            {nar.direction}
                          </span>
                        </div>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}
