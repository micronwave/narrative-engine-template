"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { fetchCorrelationMatrix, type CorrelationPair } from "@/lib/api";

function correlationColor(r: number): string {
  const abs = Math.abs(r);
  if (abs < 0.1) return "var(--text-muted)";
  if (r > 0) return "var(--vel-accelerating)";
  return "var(--vel-decelerating)";
}

function significanceLabel(pair: CorrelationPair): string {
  if (pair.n_observations < 30) return "Insufficient data";
  return pair.is_significant ? "Significant" : "Not significant";
}

export default function MarketImpactPage() {
  const [pairs, setPairs] = useState<CorrelationPair[]>([]);
  const [loading, setLoading] = useState(true);
  const [leadDays, setLeadDays] = useState(1);
  const [cached, setCached] = useState(false);

  useEffect(() => {
    setLoading(true);
    fetchCorrelationMatrix(leadDays, 50)
      .then((d) => {
        setPairs(d.pairs);
        setCached(d.cached);
      })
      .catch(() => setPairs([]))
      .finally(() => setLoading(false));
  }, [leadDays]);

  const top5 = pairs.slice(0, 5);
  const rest = pairs.slice(5);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        <div className="flex items-baseline justify-between flex-wrap gap-4">
          <div>
            <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
              Market Impact
            </h1>
            <p className="font-mono text-[11px] text-text-muted mt-1">
              Velocity-Price Correlations{cached ? " · cached" : ""}
            </p>
          </div>
          <div className="flex gap-2">
            {[0, 1, 2, 3].map((d) => (
              <button
                key={d}
                onClick={() => setLeadDays(d)}
                className="font-mono text-[11px] px-2.5 py-1 cursor-pointer transition-colors duration-[120ms]"
                style={{
                  background: leadDays === d ? "var(--accent-primary)" : "transparent",
                  border: `1px solid ${leadDays === d ? "var(--accent-primary)" : "var(--bg-border)"}`,
                  color: leadDays === d ? "var(--text-primary)" : "var(--text-muted)",
                  borderRadius: 2,
                }}
              >
                {d === 0 ? "Same day" : `+${d}d lead`}
              </button>
            ))}
          </div>
        </div>

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {loading ? (
          <div className="font-mono text-[12px] text-text-muted py-8 text-center">
            Computing correlations...
          </div>
        ) : pairs.length === 0 ? (
          <div className="font-mono text-[12px] text-text-muted py-8 text-center">
            No correlation data available. The pipeline needs at least 5 days of snapshots.
          </div>
        ) : (
          <>
            {/* Top correlations */}
            <section className="mb-10">
              <div className="flex items-baseline gap-3 mb-3">
                <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                  Strongest Correlations
                </h2>
                <span className="font-mono text-[11px] text-text-muted">top {top5.length}</span>
              </div>

              <div className="flex flex-col">
                {top5.map((pair, i) => (
                  <div
                    key={`${pair.narrative_id}-${pair.ticker}`}
                    className="flex items-center gap-4 px-2 py-3 transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)]"
                    style={{
                      borderBottom: "1px solid rgba(56, 62, 71, 0.13)",
                      borderLeft: `2px solid ${correlationColor(pair.correlation)}`,
                    }}
                  >
                    <span className="font-mono text-[10px] text-text-muted w-5">{i + 1}</span>
                    <span className="font-mono text-[22px] font-bold shrink-0 w-[90px]" style={{ color: correlationColor(pair.correlation) }}>
                      {pair.correlation > 0 ? "+" : ""}{pair.correlation.toFixed(3)}
                    </span>
                    <div className="flex-1 min-w-0">
                      <Link
                        href={`/narrative/${pair.narrative_id}`}
                        className="text-[13px] text-text-primary font-display font-medium hover:text-accent-text transition-colors truncate block"
                      >
                        {pair.narrative_name}
                      </Link>
                      <span className="font-mono text-[10px] text-text-muted">
                        {pair.ticker} · p={pair.p_value.toFixed(3)} · n={pair.n_observations} · {significanceLabel(pair)}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </section>

            {/* Full table */}
            {rest.length > 0 && (
              <section className="mb-10">
                <div className="flex items-baseline gap-3 mb-3">
                  <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                    All Correlations
                  </h2>
                  <span className="font-mono text-[11px] text-text-muted">{rest.length} pairs</span>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full font-mono text-[12px]" style={{ borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.2)" }}>
                        {["Narrative", "Ticker", "r", "p-value", "n", "Status", "Interpretation"].map((h) => (
                          <th key={h} className="text-left font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted font-medium" style={{ padding: "0 8px 8px" }}>
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rest.map((pair) => (
                        <tr
                          key={`${pair.narrative_id}-${pair.ticker}`}
                          className="transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)]"
                          style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                        >
                          <td className="text-text-secondary max-w-[250px] overflow-hidden text-ellipsis whitespace-nowrap" style={{ padding: "10px 8px" }}>
                            <Link href={`/narrative/${pair.narrative_id}`} className="text-inherit no-underline hover:text-accent-text transition-colors">
                              {pair.narrative_name}
                            </Link>
                          </td>
                          <td className="text-text-primary font-semibold" style={{ padding: "10px 8px" }}>{pair.ticker}</td>
                          <td className="font-bold" style={{ padding: "10px 8px", color: correlationColor(pair.correlation) }}>
                            {pair.correlation > 0 ? "+" : ""}{pair.correlation.toFixed(3)}
                          </td>
                          <td className="text-text-muted" style={{ padding: "10px 8px" }}>{pair.p_value.toFixed(3)}</td>
                          <td className="text-text-muted" style={{ padding: "10px 8px" }}>{pair.n_observations}</td>
                          <td style={{ padding: "10px 8px" }}>
                            <span
                              className="text-[10px] px-1.5 py-0.5"
                              style={{
                                border: `1px solid ${pair.is_significant ? "var(--vel-accelerating)" : "var(--bg-border)"}`,
                                color: pair.is_significant ? "var(--vel-accelerating)" : "var(--text-muted)",
                                borderRadius: 2,
                              }}
                            >
                              {significanceLabel(pair)}
                            </span>
                          </td>
                          <td className="text-text-muted text-[11px]" style={{ padding: "10px 8px" }}>{pair.interpretation}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            )}
          </>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="font-mono text-[10px] text-text-muted">
            CORRELATION DOES NOT IMPLY CAUSATION. INTELLIGENCE ONLY — NOT FINANCIAL ADVICE.
          </div>
        </div>
      </div>
    </main>
  );
}
