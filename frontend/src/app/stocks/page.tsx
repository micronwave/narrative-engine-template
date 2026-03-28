"use client";

import { useEffect, useState, useMemo } from "react";
import { fetchStocks, fetchAssetClasses, fetchStockDetail } from "@/lib/api";
import type { TrackedSecurity, AssetClass, StockDetail } from "@/lib/api";
import StockDetailDrawer from "@/components/StockDetailDrawer";

function impactBadgeClass(score: number): string {
  if (score >= 67) return "bg-bearish-bg text-bearish";
  if (score >= 34) return "bg-alert-bg text-alert";
  return "bg-bullish-bg text-bullish";
}

function formatPrice(price: number | null): string {
  if (price === null) return "—";
  return `$${price.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatChange(change: number | null): string {
  if (change === null) return "—";
  const sign = change >= 0 ? "+" : "\u2212";
  return `${sign}${Math.abs(change).toFixed(2)}%`;
}

export default function StocksPage() {
  const [allSecurities, setAllSecurities] = useState<TrackedSecurity[]>([]);
  const [assetClasses, setAssetClasses] = useState<AssetClass[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [filterAssetClass, setFilterAssetClass] = useState("all");
  const [filterMinImpact, setFilterMinImpact] = useState(1);
  const [sortBy, setSortBy] = useState("impact");
  const [sortOrder, setSortOrder] = useState("desc");

  const [drawerOpen, setDrawerOpen] = useState(false);
  const [stockDetail, setStockDetail] = useState<StockDetail | null>(null);
  const [drawerLoading, setDrawerLoading] = useState(false);

  useEffect(() => {
    Promise.all([fetchStocks(), fetchAssetClasses()])
      .then(([stocks, classes]) => {
        setAllSecurities(stocks);
        setAssetClasses(classes);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  const filteredSecurities = useMemo(() => {
    let filtered = allSecurities;
    if (filterAssetClass !== "all") {
      filtered = filtered.filter((s) => s.asset_class_id === filterAssetClass);
    }
    if (filterMinImpact > 1) {
      filtered = filtered.filter((s) => s.narrative_impact_score >= filterMinImpact);
    }
    const reversed = sortOrder === "desc";
    return [...filtered].sort((a, b) => {
      const mult = reversed ? -1 : 1;
      let cmp = 0;
      if (sortBy === "impact") cmp = a.narrative_impact_score - b.narrative_impact_score;
      else if (sortBy === "price") cmp = (a.current_price ?? 0) - (b.current_price ?? 0);
      else if (sortBy === "change") cmp = (a.price_change_24h ?? 0) - (b.price_change_24h ?? 0);
      else if (sortBy === "symbol") cmp = a.symbol.localeCompare(b.symbol);
      return mult * cmp || a.id.localeCompare(b.id);
    });
  }, [allSecurities, filterAssetClass, filterMinImpact, sortBy, sortOrder]);

  const handleRowClick = async (symbol: string) => {
    setDrawerOpen(true);
    setStockDetail(null);
    setDrawerLoading(true);
    try {
      const detail = await fetchStockDetail(symbol);
      setStockDetail(detail);
    } catch (e) {
      console.error(e);
    } finally {
      setDrawerLoading(false);
    }
  };

  const handleCloseDrawer = () => {
    setDrawerOpen(false);
    setStockDetail(null);
  };

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Page title row */}
        <div className="flex items-center justify-between flex-wrap gap-4">
          <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
            Narrative-Affected Securities
          </h1>
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2">
              <label className="font-mono text-[10px] uppercase text-text-muted">Class</label>
              <select
                className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
                value={filterAssetClass}
                onChange={(e) => setFilterAssetClass(e.target.value)}
                data-testid="filter-asset-class"
              >
                <option value="all">All</option>
                {assetClasses.map((ac) => (
                  <option key={ac.id} value={ac.id}>{ac.name}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2">
              <label className="font-mono text-[10px] uppercase text-text-muted">Min</label>
              <input
                type="number"
                min="1"
                max="100"
                className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1 w-14"
                value={filterMinImpact}
                onChange={(e) =>
                  setFilterMinImpact(Math.max(1, Math.min(100, parseInt(e.target.value) || 1)))
                }
                data-testid="filter-min-impact"
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="font-mono text-[10px] uppercase text-text-muted">Sort</label>
              <select
                className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
                value={sortBy}
                onChange={(e) => setSortBy(e.target.value)}
                data-testid="filter-sort-by"
              >
                <option value="impact">Impact</option>
                <option value="price">Price</option>
                <option value="change">24h Change</option>
                <option value="symbol">Symbol</option>
              </select>
            </div>
            <select
              className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
              value={sortOrder}
              onChange={(e) => setSortOrder(e.target.value)}
              data-testid="filter-sort-order"
            >
              <option value="desc">Desc</option>
              <option value="asc">Asc</option>
            </select>
          </div>
        </div>

        {/* Title rule */}
        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-8" />

        {/* Loading skeleton */}
        {loading && (
          <div data-testid="stocks-loading">
            {Array.from({ length: 5 }).map((_, i) => (
              <div
                key={i}
                className="h-11 bg-[var(--bg-surface-hover)] rounded-sm mb-1 skeleton-shimmer"
                data-testid="stocks-skeleton"
              />
            ))}
          </div>
        )}

        {/* Error */}
        {error && !loading && (
          <div className="font-mono text-[12px] text-bearish text-center py-8">
            Failed to load: {error}
          </div>
        )}

        {/* Empty state */}
        {!loading && !error && filteredSecurities.length === 0 && (
          <p className="font-mono text-[12px] text-text-muted" data-testid="stocks-empty">
            No tracked securities. Securities are associated with narratives via asset classes.
          </p>
        )}

        {/* Securities table */}
        {!loading && !error && filteredSecurities.length > 0 && (
          <div data-testid="stocks-table">
            {/* Table header */}
            <div
              className="flex items-center justify-between px-4 font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted"
              style={{ height: 32, borderBottom: "1px solid rgba(56, 62, 71, 0.2)" }}
            >
              <div className="flex items-center gap-4">
                <span className="w-14 shrink-0">Symbol</span>
                <span>Name</span>
              </div>
              <div className="flex items-center gap-3 shrink-0">
                <span className="w-20 text-right">Price</span>
                <span className="w-16 text-right">24h</span>
                <span className="w-8 text-center">Imp</span>
              </div>
            </div>
            {filteredSecurities.map((sec) => (
              <div
                key={sec.id}
                className="flex items-center justify-between px-4 cursor-pointer transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)]"
                style={{ height: 44, borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                onClick={() => handleRowClick(sec.symbol)}
                data-testid={`stock-row-${sec.symbol}`}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") handleRowClick(sec.symbol);
                }}
                aria-label={`${sec.symbol} — ${sec.name}`}
              >
                <div className="flex items-center gap-4">
                  <span className="font-mono text-[14px] font-bold text-text-primary w-14 shrink-0">
                    {sec.symbol}
                  </span>
                  <span className="text-[13px] text-text-muted">{sec.name}</span>
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  <span className="font-mono text-[12px] text-text-secondary w-20 text-right">
                    {formatPrice(sec.current_price)}
                  </span>
                  <span
                    className={`font-mono text-[12px] w-16 text-right ${
                      sec.price_change_24h === null
                        ? "text-text-disabled"
                        : sec.price_change_24h >= 0
                        ? "text-bullish"
                        : "text-bearish"
                    }`}
                  >
                    {formatChange(sec.price_change_24h)}
                  </span>
                  <span
                    className={`font-mono text-[12px] px-2 py-0.5 rounded-sm font-semibold w-8 text-center ${impactBadgeClass(
                      sec.narrative_impact_score
                    )}`}
                    data-testid={`impact-score-${sec.symbol}`}
                    title={`Narrative impact score: ${sec.narrative_impact_score} (1-100)`}
                  >
                    {sec.narrative_impact_score}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            <span>{filteredSecurities.length} securities</span>
          </div>
        </div>
      </div>

      <StockDetailDrawer
        isOpen={drawerOpen}
        stockDetail={stockDetail}
        loading={drawerLoading}
        onClose={handleCloseDrawer}
      />
    </main>
  );
}
