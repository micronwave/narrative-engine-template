"use client";

import { TrendingUp, TrendingDown, ArrowLeftRight, HelpCircle } from "lucide-react";
import type { NarrativeAsset } from "@/lib/api";

type Props = {
  assets: NarrativeAsset[];
};

function formatPrice(price: number): string {
  return `$${price.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

// price_change_24h is a percentage (e.g., 2.5 means +2.5%)
function formatChange(change: number): string {
  const sign = change >= 0 ? "+" : "\u2212";
  return `${sign}${Math.abs(change).toFixed(2)}%`;
}

function formatPct(change: number): string {
  const sign = change >= 0 ? "+" : "\u2212";
  return `${sign}${Math.abs(change).toFixed(2)}%`;
}

export default function AffectedAssets({ assets }: Props) {
  if (assets.length === 0) {
    return (
      <p className="text-text-tertiary text-sm" data-testid="affected-assets-empty">
        No asset class associations recorded.
      </p>
    );
  }

  // Show banner when all securities across all assets have null prices
  const hasSomeSecurities = assets.some((a) => a.securities.length > 0);
  const allPricesNull =
    hasSomeSecurities &&
    assets.every((a) => a.securities.every((s) => s.current_price === null));

  return (
    <>
      {allPricesNull && (
        <div
          className="mb-4 text-xs text-alert bg-alert-bg border border-alert/20 rounded-sm px-3 py-2"
          data-testid="finnhub-unavailable-banner"
        >
          Connect a Finnhub API key for live prices
        </div>
      )}

      <div className="flex flex-col" data-testid="affected-assets-list">
        {assets.map((na) => (
          <div
            key={na.id}
            className="py-4"
            style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
            data-testid={`asset-card-${na.id}`}
          >
            {/* Asset class header */}
            <div className="flex items-center gap-2 mb-2">
              <span className="text-text-primary font-semibold text-sm font-display" data-testid={`asset-name-${na.id}`}>
                {na.asset_class.name}
              </span>
              <span
                className="text-xs bg-inset text-text-tertiary border border-border-subtle px-2 py-0.5"
                data-testid={`asset-type-badge-${na.id}`}
              >
                {na.asset_class.type}
              </span>
            </div>

            {/* Exposure bar */}
            <div className="mb-2" data-testid={`exposure-bar-${na.id}`}>
              <div className="flex items-center justify-between text-xs text-text-tertiary mb-1">
                <span>Exposure</span>
                <span className="font-mono-data">{(na.exposure_score * 100).toFixed(0)}%</span>
              </div>
              <div className="h-1.5 w-full bg-inset overflow-hidden">
                <div
                  className="h-full"
                  style={{
                    width: `${na.exposure_score * 100}%`,
                    backgroundColor:
                      na.exposure_score >= 0.67
                        ? "#CD4246"
                        : na.exposure_score >= 0.34
                        ? "#EC9A3C"
                        : "#2D72D2",
                  }}
                />
              </div>
            </div>

            {/* Direction indicator */}
            <div
              className="flex items-center gap-1.5 mb-2 text-xs"
              data-testid={`direction-${na.id}`}
            >
              {na.direction === "bullish" && (
                <>
                  <TrendingUp size={12} className="text-bullish" />
                  <span className="text-bullish">Bullish</span>
                </>
              )}
              {na.direction === "bearish" && (
                <>
                  <TrendingDown size={12} className="text-bearish" />
                  <span className="text-bearish">Bearish</span>
                </>
              )}
              {na.direction === "mixed" && (
                <>
                  <ArrowLeftRight size={12} className="text-alert" />
                  <span className="text-alert">Mixed</span>
                </>
              )}
              {na.direction === "uncertain" && (
                <>
                  <HelpCircle size={12} className="text-text-tertiary" />
                  <span className="text-text-tertiary">Uncertain</span>
                </>
              )}
            </div>

            {/* Rationale */}
            <p
              className="text-text-secondary text-xs leading-relaxed mb-3"
              data-testid={`rationale-${na.id}`}
            >
              {na.rationale}
            </p>

            {/* Securities list */}
            {na.securities.length > 0 && (
              <div className="mt-3">
                <p className="text-xs text-text-tertiary mb-2 uppercase tracking-wider">Securities</p>
                <div className="flex flex-col gap-1">
                  {na.securities.map((sec) => (
                    <div
                      key={sec.id}
                      className="flex items-center justify-between text-xs"
                      data-testid={`security-row-${sec.symbol}`}
                    >
                      <div className="flex items-center gap-2">
                        <span className="font-mono-data text-text-primary font-semibold">
                          {sec.symbol}
                        </span>
                        <span className="text-text-secondary">{sec.name}</span>
                      </div>
                      <div className="flex items-center gap-3 font-mono-data">
                        <span
                          className="text-text-secondary"
                          data-testid={`price-${sec.symbol}`}
                        >
                          {sec.current_price !== null
                            ? formatPrice(sec.current_price)
                            : "—"}
                        </span>
                        <span
                          className={
                            sec.price_change_24h === null
                              ? "text-text-disabled"
                              : sec.price_change_24h >= 0
                              ? "text-bullish"
                              : "text-bearish"
                          }
                          data-testid={`change-${sec.symbol}`}
                        >
                          {sec.price_change_24h !== null
                            ? formatChange(sec.price_change_24h)
                            : "—"}
                        </span>
                        {sec.current_price !== null && sec.price_change_24h !== null && (
                          <span
                            className={`text-xs px-1.5 py-0.5 rounded font-mono-data ${
                              sec.price_change_24h >= 0
                                ? "bg-bullish-bg text-bullish"
                                : "bg-bearish-bg text-bearish"
                            }`}
                            data-testid={`pct-${sec.symbol}`}
                          >
                            {formatPct(sec.price_change_24h)}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
    </>
  );
}
