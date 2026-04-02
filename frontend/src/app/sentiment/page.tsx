"use client";

import { useEffect, useState } from "react";
import { fetchMarketSentiment, fetchTickerSentiment, fetchSentimentHistory, fetchPriceHistory } from "@/lib/api";
import type { MarketSentiment, TickerSentiment, SentimentRecord, OHLCVBar } from "@/lib/api";
import TimeframeSelector from "@/components/TimeframeSelector";

// ---------------------------------------------------------------------------
// Market Sentiment Gauge (SVG semicircle)
// ---------------------------------------------------------------------------

function scoreToLabel(score: number): string {
  if (score <= -0.6) return "Extreme Bearish";
  if (score <= -0.2) return "Bearish";
  if (score < 0.2) return "Neutral";
  if (score < 0.6) return "Bullish";
  return "Extreme Bullish";
}

function scoreToColor(score: number): string {
  if (score <= -0.4) return "var(--intent-danger)";
  if (score <= -0.1) return "#f97316"; // orange
  if (score < 0.1) return "var(--text-muted)";
  if (score < 0.4) return "#84cc16"; // light green
  return "var(--intent-success)";
}

function SentimentGauge({ score }: { score: number }) {
  // Semicircle: score -1 → 180° (left), 0 → 90° (top), +1 → 0° (right)
  const angle = (1 - score) * 90; // degrees from right (0°=bullish, 180°=bearish)
  const radians = (angle * Math.PI) / 180;
  const cx = 100;
  const cy = 100;
  const r = 70;
  const needleX = cx + r * Math.cos(Math.PI - radians);
  const needleY = cy - r * Math.sin(Math.PI - radians);
  const color = scoreToColor(score);

  return (
    <div data-testid="sentiment-gauge" className="flex flex-col items-center gap-3">
      <svg width="200" height="110" viewBox="0 0 200 110">
        {/* Background arc segments: red → orange → gray → green */}
        {[
          { start: 180, end: 144, color: "var(--intent-danger)" },
          { start: 144, end: 108, color: "#f97316" },
          { start: 108, end: 72, color: "var(--text-muted)" },
          { start: 72, end: 36, color: "#84cc16" },
          { start: 36, end: 0, color: "var(--intent-success)" },
        ].map(({ start, end, color: segColor }) => {
          const a1 = (start * Math.PI) / 180;
          const a2 = (end * Math.PI) / 180;
          const x1 = cx + r * Math.cos(Math.PI - a1);
          const y1 = cy - r * Math.sin(Math.PI - a1);
          const x2 = cx + r * Math.cos(Math.PI - a2);
          const y2 = cy - r * Math.sin(Math.PI - a2);
          return (
            <path
              key={start}
              d={`M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2} Z`}
              fill={segColor}
              opacity={0.25}
            />
          );
        })}
        {/* Arc outline */}
        <path
          d={`M ${cx - r} ${cy} A ${r} ${r} 0 0 1 ${cx + r} ${cy}`}
          fill="none"
          stroke="var(--color-border)"
          strokeWidth="2"
        />
        {/* Needle */}
        <line
          x1={cx}
          y1={cy}
          x2={needleX}
          y2={needleY}
          stroke={color}
          strokeWidth="3"
          strokeLinecap="round"
        />
        <circle cx={cx} cy={cy} r="5" fill={color} />
        {/* Score text */}
        <text
          x={cx}
          y={cy + 22}
          textAnchor="middle"
          fontSize="14"
          fontWeight="700"
          fill={color}
        >
          {score > 0 ? "+" : ""}{score.toFixed(2)}
        </text>
      </svg>
      <div
        className="text-center font-semibold"
        style={{ color, fontSize: "var(--text-small)" }}
      >
        {scoreToLabel(score)}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sentiment Heatmap cell
// ---------------------------------------------------------------------------

function HeatmapCell({ ticker, score, volume }: { ticker: string; score: number; volume: number }) {
  const bg = score > 0.1
    ? `rgba(34,197,94,${Math.min(0.7, score * 0.7)})`
    : score < -0.1
    ? `rgba(239,68,68,${Math.min(0.7, Math.abs(score) * 0.7)})`
    : "var(--bg-surface)";
  const arrow = score > 0.1 ? "▲" : score < -0.1 ? "▼" : "—";
  const arrowColor = score > 0.1
    ? "var(--intent-success)"
    : score < -0.1
    ? "var(--intent-danger)"
    : "var(--text-muted)";

  return (
    <div
      data-testid={`heatmap-cell-${ticker}`}
      className="flex flex-col items-center justify-center p-3 rounded"
      style={{
        background: bg,
        border: "1px solid var(--color-border)",
        minWidth: 80,
        minHeight: 64,
      }}
    >
      <span style={{ fontSize: "var(--text-small)", fontWeight: 700, color: "var(--text-primary)" }}>
        {ticker}
      </span>
      <span style={{ fontSize: 11, color: arrowColor }}>
        {arrow} {score.toFixed(2)}
      </span>
      {volume > 0 && (
        <span style={{ fontSize: 10, color: "var(--text-muted)" }}>{volume}msg</span>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sentiment-Price Overlay (dual normalized lines via SVG)
// ---------------------------------------------------------------------------

function SentimentOverlay({ history, priceData }: { history: SentimentRecord[]; priceData?: OHLCVBar[] }) {
  if (!history || history.length < 2) {
    return (
      <div
        style={{
          height: 120,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--text-muted)",
          fontSize: "var(--text-small)",
        }}
      >
        Not enough history to display chart
      </div>
    );
  }

  const W = 500;
  const H = 100;

  // Sentiment line — normalized to [-1, 1] range mapped to [0, H]
  const scores = history.map((h) => h.composite_score);
  const minS = Math.min(...scores, -1);
  const maxS = Math.max(...scores, 1);
  const sentRange = maxS - minS || 1;
  const toSentY = (s: number) => H - ((s - minS) / sentRange) * H;
  const toSentX = (i: number) => (i / (scores.length - 1)) * W;
  const sentPoints = scores.map((s, i) => `${toSentX(i)},${toSentY(s)}`).join(" ");

  // Price line — independently normalized to [0, H] (right-axis equivalent)
  const hasPriceData = priceData && priceData.length >= 2;
  let pricePoints = "";
  if (hasPriceData) {
    const closes = priceData.map((d) => d.close);
    const minP = Math.min(...closes);
    const maxP = Math.max(...closes);
    const priceRange = maxP - minP || 1;
    const toPriceY = (p: number) => H - ((p - minP) / priceRange) * H;
    const toPriceX = (i: number) => (i / (closes.length - 1)) * W;
    pricePoints = closes.map((p, i) => `${toPriceX(i)},${toPriceY(p)}`).join(" ");
  }

  return (
    <div>
      <div style={{ display: "flex", gap: 16, marginBottom: 8, fontSize: 11, color: "var(--text-muted)" }}>
        <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <span style={{ width: 16, height: 2, background: "var(--accent-primary)", display: "inline-block" }} />
          Sentiment
        </span>
        {hasPriceData && (
          <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <span style={{ width: 16, height: 2, background: "var(--text-muted)", display: "inline-block", opacity: 0.7 }} />
            Price (normalized)
          </span>
        )}
      </div>
      <svg
        data-testid="sentiment-overlay"
        width="100%"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        style={{ display: "block" }}
      >
        {/* Zero line */}
        <line x1="0" y1={toSentY(0)} x2={W} y2={toSentY(0)} stroke="var(--color-border)" strokeDasharray="4" />
        {/* Sentiment line */}
        <polyline points={sentPoints} fill="none" stroke="var(--accent-primary)" strokeWidth="2" />
        {/* Price line — dashed, independently scaled (right Y-axis equivalent) */}
        {hasPriceData && (
          <polyline
            points={pricePoints}
            fill="none"
            stroke="var(--text-muted)"
            strokeWidth="1.5"
            strokeDasharray="4,2"
            opacity={0.7}
          />
        )}
      </svg>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function SentimentPage() {
  const [market, setMarket] = useState<MarketSentiment | null>(null);
  const [heatmapData, setHeatmapData] = useState<{ ticker: string; score: number; volume: number }[]>([]);
  const [history, setHistory] = useState<SentimentRecord[]>([]);
  const [priceHistory, setPriceHistory] = useState<OHLCVBar[]>([]);
  const [overlayTicker, setOverlayTicker] = useState("AAPL");
  const [overlayDays, setOverlayDays] = useState(30);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const SAMPLE_TICKERS = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META"];

  useEffect(() => {
    fetchMarketSentiment()
      .then(setMarket)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    Promise.all(SAMPLE_TICKERS.map((t) => fetchTickerSentiment(t)))
      .then((results) =>
        setHeatmapData(
          results.map((r: TickerSentiment) => ({
            ticker: r.ticker,
            score: r.composite_score,
            volume: r.message_volume_24h,
          }))
        )
      )
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    fetchSentimentHistory(overlayTicker, overlayDays * 24)
      .then((r) => setHistory(r.data))
      .catch(() => setHistory([]));
    fetchPriceHistory(overlayTicker, overlayDays)
      .then((r) => setPriceHistory(r.available ? r.data : []))
      .catch(() => setPriceHistory([]));
  }, [overlayTicker, overlayDays]);

  return (
    <div
      className="p-6 space-y-8"
      style={{ marginLeft: 64, minHeight: "100vh", background: "var(--bg-primary)" }}
    >
      <h1
        className="font-display font-semibold"
        style={{ fontSize: "var(--text-heading)", color: "var(--text-primary)" }}
      >
        Market Sentiment
      </h1>

      {loading && (
        <p style={{ color: "var(--text-muted)", fontSize: "var(--text-small)" }}>Loading...</p>
      )}
      {error && (
        <p style={{ color: "var(--intent-danger)", fontSize: "var(--text-small)" }}>{error}</p>
      )}

      {/* 1. Market Gauge */}
      {market && (
        <section
          className="p-6 rounded"
          style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
        >
          <h2
            className="font-semibold mb-4"
            style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
          >
            Market Composite
          </h2>
          <div className="flex flex-col md:flex-row items-center gap-8">
            <SentimentGauge score={market.market_score} />
            <div className="flex gap-6">
              {[
                { label: "Bullish", pct: market.bullish_pct, color: "var(--intent-success)" },
                { label: "Neutral", pct: market.neutral_pct, color: "var(--text-muted)" },
                { label: "Bearish", pct: market.bearish_pct, color: "var(--intent-danger)" },
              ].map(({ label, pct, color }) => (
                <div key={label} className="text-center">
                  <div style={{ fontSize: "var(--text-heading)", fontWeight: 700, color }}>
                    {pct.toFixed(1)}%
                  </div>
                  <div style={{ fontSize: "var(--text-small)", color: "var(--text-muted)" }}>
                    {label}
                  </div>
                </div>
              ))}
            </div>
            {market.spikes.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {market.spikes.map((s) => (
                  <span
                    key={s.ticker}
                    className="px-2 py-1 rounded text-xs font-semibold"
                    style={{
                      background: s.direction === "bullish" ? "var(--bullish-bg)" : "var(--bearish-bg)",
                      color: s.direction === "bullish" ? "var(--intent-success)" : "var(--intent-danger)",
                    }}
                  >
                    ⚡ {s.ticker}
                  </span>
                ))}
              </div>
            )}
          </div>
        </section>
      )}

      {/* 2. Sentiment Heatmap */}
      {heatmapData.length > 0 && (
        <section
          className="p-6 rounded"
          style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
        >
          <h2
            className="font-semibold mb-4"
            style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
          >
            Sentiment Heatmap
          </h2>
          <div data-testid="sentiment-heatmap" className="flex flex-wrap gap-3">
            {heatmapData.map((d) => (
              <HeatmapCell key={d.ticker} {...d} />
            ))}
          </div>
        </section>
      )}

      {/* 3. Sentiment-Price Overlay */}
      <section
        className="p-6 rounded"
        style={{ background: "var(--bg-surface)", border: "1px solid var(--color-border)" }}
      >
        <div className="flex items-center justify-between mb-4 gap-4 flex-wrap">
          <h2
            className="font-semibold"
            style={{ fontSize: "var(--text-label)", color: "var(--text-secondary)" }}
          >
            Sentiment History
          </h2>
          <div className="flex items-center gap-3">
            <select
              value={overlayTicker}
              onChange={(e) => setOverlayTicker(e.target.value)}
              className="rounded px-2 py-1"
              style={{
                background: "var(--bg-primary)",
                border: "1px solid var(--color-border)",
                color: "var(--text-primary)",
                fontSize: "var(--text-small)",
              }}
            >
              {SAMPLE_TICKERS.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
            <TimeframeSelector selected={overlayDays} onChange={setOverlayDays} />
          </div>
        </div>
        <SentimentOverlay history={history} priceData={priceHistory} />
      </section>
    </div>
  );
}
