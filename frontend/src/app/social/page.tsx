"use client";

import { useEffect, useState, useMemo } from "react";
import { fetchTrendingTickers, fetchSocialDetail, fetchMarketSentiment } from "@/lib/api";
import type { TrendingTicker } from "@/lib/api";

// ---------------------------------------------------------------------------
// Trending Tickers card
// ---------------------------------------------------------------------------

function TrendingCard({ ticker, total_mentions, total_bullish, total_bearish }: TrendingTicker) {
  const total = total_mentions || 1;
  const bullishPct = Math.round((total_bullish / total) * 100);
  const bearishPct = Math.round((total_bearish / total) * 100);
  const netScore = (total_bullish - total_bearish) / total;
  const arrow = netScore > 0.1 ? "▲" : netScore < -0.1 ? "▼" : "—";
  const arrowColor = netScore > 0.1
    ? "var(--intent-success)"
    : netScore < -0.1
    ? "var(--intent-danger)"
    : "var(--text-muted)";

  return (
    <div
      data-testid={`trending-card-${ticker}`}
      className="p-4 rounded flex flex-col gap-2"
      style={{
        background: "var(--bg-surface)",
        border: "1px solid var(--color-border)",
        minWidth: 140,
      }}
    >
      <div className="flex items-center justify-between">
        <span style={{ fontWeight: 700, fontSize: "var(--text-label)", color: "var(--text-primary)" }}>
          ${ticker}
        </span>
        <span style={{ color: arrowColor, fontWeight: 700 }}>{arrow}</span>
      </div>
      <div style={{ fontSize: "var(--text-small)", color: "var(--text-muted)" }}>
        {total_mentions.toLocaleString()} mentions
      </div>
      <div className="flex gap-1 h-1.5 rounded overflow-hidden">
        <div style={{ width: `${bullishPct}%`, background: "var(--intent-success)" }} />
        <div style={{ width: `${bearishPct}%`, background: "var(--intent-danger)" }} />
        <div style={{ flex: 1, background: "var(--bg-primary)" }} />
      </div>
      <div className="flex justify-between" style={{ fontSize: 10, color: "var(--text-muted)" }}>
        <span>Bull {bullishPct}%</span>
        <span>Bear {bearishPct}%</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Source Breakdown table
// ---------------------------------------------------------------------------

type BreakdownRow = {
  ticker: string;
  stocktwits_bull: number;
  stocktwits_bear: number;
  stocktwits_trending: boolean;
  signal_direction: string | null;
  signal_confidence: number | null;
};

function SourceBreakdownTable({ rows }: { rows: BreakdownRow[] }) {
  if (!rows.length) {
    return (
      <p style={{ color: "var(--text-muted)", fontSize: "var(--text-small)" }}>
        No source data available yet.
      </p>
    );
  }
  return (
    <div data-testid="source-breakdown-table" style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "var(--text-small)" }}>
        <thead>
          <tr style={{ color: "var(--text-muted)", textAlign: "left" }}>
            <th style={{ padding: "6px 8px" }}>Ticker</th>
            <th style={{ padding: "6px 8px" }}>StockTwits Bull</th>
            <th style={{ padding: "6px 8px" }}>StockTwits Bear</th>
            <th style={{ padding: "6px 8px" }}>Trending</th>
            <th style={{ padding: "6px 8px" }}>Signal Direction</th>
            <th style={{ padding: "6px 8px" }}>Confidence</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={row.ticker}
              style={{ borderTop: "1px solid var(--color-border)", color: "var(--text-primary)" }}
            >
              <td style={{ padding: "6px 8px", fontWeight: 600 }}>${row.ticker}</td>
              <td style={{ padding: "6px 8px", color: "var(--intent-success)" }}>{row.stocktwits_bull}</td>
              <td style={{ padding: "6px 8px", color: "var(--intent-danger)" }}>{row.stocktwits_bear}</td>
              <td style={{ padding: "6px 8px" }}>{row.stocktwits_trending ? "⚡" : "—"}</td>
              <td style={{ padding: "6px 8px", textTransform: "capitalize" }}>
                {row.signal_direction ?? "—"}
              </td>
              <td style={{ padding: "6px 8px" }}>
                {row.signal_confidence != null
                  ? `${Math.round(row.signal_confidence * 100)}%`
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Social Surge Badges
// ---------------------------------------------------------------------------

function SurgeBadge({ ticker, direction, score }: { ticker: string; direction: string; score: number }) {
  const color = direction === "bullish" ? "var(--intent-success)" : "var(--intent-danger)";
  const bg = direction === "bullish" ? "var(--bullish-bg)" : "var(--bearish-bg)";
  return (
    <div
      data-testid={`surge-badge-${ticker}`}
      className="flex items-center gap-2 px-3 py-2 rounded"
      style={{ background: bg, border: `1px solid ${color}`, color }}
    >
      <span style={{ fontWeight: 700, fontSize: "var(--text-label)" }}>{ticker}</span>
      <span style={{ fontSize: "var(--text-small)" }}>
        {direction === "bullish" ? "▲" : "▼"} {score > 0 ? "+" : ""}{score.toFixed(2)}
      </span>
      <span style={{ fontSize: 10 }}>⚡ Spike</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

const TICKERS = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META"];

export default function SocialPage() {
  const [trending, setTrending] = useState<TrendingTicker[]>([]);
  const [breakdownRows, setBreakdownRows] = useState<BreakdownRow[]>([]);
  const [spikes, setSpikes] = useState<{ ticker: string; score: number; direction: string }[]>([]);
  const [filterTicker, setFilterTicker] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchTrendingTickers(24, 10)
      .then((r) => setTrending(r.tickers))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    fetchMarketSentiment()
      .then((r) => setSpikes(r.spikes))
      .catch(() => {});
  }, []);

  useEffect(() => {
    Promise.all(TICKERS.map((t) => fetchSocialDetail(t)))
      .then((results) => {
        const rows: BreakdownRow[] = results.map((r) => {
          const st = r.stocktwits;
          const sigs = r.narrative_signals?.signals as Array<{ direction?: string; confidence?: number }> ?? [];
          const topSig = sigs[0] ?? null;
          return {
            ticker: r.ticker,
            stocktwits_bull: st?.bullish_count ?? 0,
            stocktwits_bear: st?.bearish_count ?? 0,
            stocktwits_trending: st?.trending ?? false,
            signal_direction: topSig?.direction ?? null,
            signal_confidence: typeof topSig?.confidence === "number" ? topSig.confidence : null,
          };
        });
        setBreakdownRows(rows);
      })
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const filteredTrending = useMemo(
    () =>
      filterTicker
        ? trending.filter((t) =>
            t.ticker.toUpperCase().includes(filterTicker.toUpperCase())
          )
        : trending,
    [trending, filterTicker]
  );

  return (
    <div
      className="p-6 space-y-8"
      style={{ marginLeft: 64, minHeight: "100vh", background: "var(--bg-primary)" }}
    >
      <h1
        className="font-display font-semibold"
        style={{ fontSize: "var(--text-heading)", color: "var(--text-primary)" }}
      >
        Social Intelligence
      </h1>

      {/* 1. Trending Tickers */}
      <section
        className="p-6 rounded"
        style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
      >
        <h2
          className="font-semibold mb-4"
          style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
        >
          Trending Tickers (24h)
        </h2>
        {loading ? (
          <p style={{ color: "var(--text-muted)", fontSize: "var(--text-small)" }}>Loading...</p>
        ) : trending.length === 0 ? (
          <p style={{ color: "var(--text-muted)", fontSize: "var(--text-small)" }}>
            No social data yet — sentiment collection starts in the background.
          </p>
        ) : (
          <div data-testid="trending-tickers-section" className="flex flex-wrap gap-3">
            {filteredTrending.map((t) => (
              <TrendingCard key={t.ticker} {...t} />
            ))}
          </div>
        )}
        {trending.length === 0 && !loading && (
          <div data-testid="trending-tickers-section" className="hidden" />
        )}
      </section>

      {/* 2. Source Breakdown */}
      <section
        className="p-6 rounded"
        style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
      >
        <h2
          className="font-semibold mb-4"
          style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
        >
          Source Breakdown
        </h2>
        <SourceBreakdownTable rows={breakdownRows} />
      </section>

      {/* 3. Live Sentiment Stream */}
      <section
        className="p-6 rounded"
        style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
      >
        <div className="flex items-center justify-between mb-4 gap-3">
          <h2
            className="font-semibold"
            style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
          >
            Live Sentiment Stream
          </h2>
          <input
            type="text"
            placeholder="Filter by ticker…"
            value={filterTicker}
            onChange={(e) => setFilterTicker(e.target.value)}
            className="rounded px-2 py-1"
            style={{
              background: "var(--bg-primary)",
              border: "1px solid var(--color-border)",
              color: "var(--text-primary)",
              fontSize: "var(--text-small)",
              width: 160,
            }}
          />
        </div>
        {breakdownRows.length === 0 ? (
          <p style={{ color: "var(--text-muted)", fontSize: "var(--text-small)" }}>
            Collecting social data…
          </p>
        ) : (
          <div className="space-y-2">
            {(filterTicker
              ? breakdownRows.filter((r) =>
                  r.ticker.toUpperCase().includes(filterTicker.toUpperCase())
                )
              : breakdownRows
            ).map((row) => (
              <div
                key={row.ticker}
                className="flex items-center gap-4 px-3 py-2 rounded"
                style={{ background: "var(--bg-primary)", fontSize: "var(--text-small)" }}
              >
                <span style={{ fontWeight: 600, color: "var(--text-primary)", width: 60 }}>
                  ${row.ticker}
                </span>
                <span style={{ color: "var(--text-muted)" }}>StockTwits</span>
                <span style={{ color: "var(--intent-success)" }}>▲ {row.stocktwits_bull}</span>
                <span style={{ color: "var(--intent-danger)" }}>▼ {row.stocktwits_bear}</span>
                {row.signal_direction && (
                  <span
                    style={{
                      color:
                        row.signal_direction === "bullish"
                          ? "var(--intent-success)"
                          : row.signal_direction === "bearish"
                          ? "var(--intent-danger)"
                          : "var(--text-muted)",
                      textTransform: "capitalize",
                    }}
                  >
                    Signal: {row.signal_direction}
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </section>

      {/* 4. Social Surge Badges */}
      {spikes.length > 0 && (
        <section
          className="p-6 rounded"
          style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
        >
          <h2
            className="font-semibold mb-4"
            style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
          >
            Social Surges
          </h2>
          <div className="flex flex-wrap gap-3">
            {spikes.map((s) => (
              <SurgeBadge key={s.ticker} {...s} />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
