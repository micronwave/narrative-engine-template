"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  fetchPortfolio,
  fetchPortfolioSummary,
  fetchPortfolioAllocation,
  fetchPortfolioPerformance,
  fetchPortfolioCorrelation,
  fetchPortfolioConcentration,
  fetchPortfolioExposure,
  addPortfolioHolding,
  removePortfolioHolding,
} from "@/lib/api";
import type {
  PortfolioHolding,
  NarrativeExposure,
  PortfolioSummary,
  AllocationGroup,
  PerformanceData,
  CorrelationMatrix,
  ConcentrationData,
} from "@/lib/api";
import { Trash2, Plus, ChevronDown, ChevronUp, AlertTriangle, TrendingUp, TrendingDown } from "lucide-react";
import StageBadge from "@/components/common/StageBadge";
import Skeleton from "@/components/common/Skeleton";

// ─── helpers ──────────────────────────────────────────────────────────────────

function fmt(n: number): string {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function pct(n: number): string {
  return `${n >= 0 ? "+" : ""}${(n * 100).toFixed(2)}%`;
}

function velColor(v: number): string {
  if (v > 5) return "var(--vel-accelerating)";
  if (v < -0.5) return "var(--vel-decelerating)";
  return "var(--vel-stable)";
}

// ─── Header stat card ─────────────────────────────────────────────────────────

function StatCard({ label, value, sub, positive }: { label: string; value: string; sub?: string; positive?: boolean }) {
  const subColor = positive === undefined
    ? "var(--text-muted)"
    : positive
    ? "var(--intent-success)"
    : "var(--intent-danger)";
  return (
    <div className="flex flex-col gap-1 px-4 py-3 bg-[var(--bg-panel)] border border-[var(--bg-border)] rounded-sm">
      <span className="font-mono text-[10px] uppercase tracking-[0.06em] text-text-muted">{label}</span>
      <span className="font-display text-[22px] font-bold text-text-primary">{value}</span>
      {sub && <span className="font-mono text-[11px]" style={{ color: subColor }}>{sub}</span>}
    </div>
  );
}

// ─── Allocation Treemap (pure SVG) ────────────────────────────────────────────

function AllocationTreemap({
  data,
  onGroupClick,
  activeGroup,
}: {
  data: AllocationGroup[];
  onGroupClick: (g: string | null) => void;
  activeGroup: string | null;
}) {
  if (!data.length) return (
    <p className="font-mono text-[12px] text-text-muted py-4">No allocation data.</p>
  );

  const W = 600;
  const H = 220;
  const total = data.reduce((s, g) => s + g.value, 0);

  // Simple squarified layout (row-based)
  let cells: { x: number; y: number; w: number; h: number; group: AllocationGroup }[] = [];
  let cx = 0;
  for (const g of data) {
    const w = (g.value / total) * W;
    cells.push({ x: cx, y: 0, w, h: H, group: g });
    cx += w;
  }

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className="w-full"
      style={{ maxHeight: 220 }}
    >
      {cells.map((c, i) => {
        const isActive = activeGroup === null || activeGroup === c.group.group;
        const fillBase = c.group.pnl >= 0 ? "#1d4a2a" : "#4a1d1d";
        const fillActive = c.group.pnl >= 0 ? "#2d6b3e" : "#6b2d2d";
        return (
          <g key={i} onClick={() => onGroupClick(activeGroup === c.group.group ? null : c.group.group)} style={{ cursor: "pointer" }}>
            <rect
              x={c.x + 1}
              y={1}
              width={Math.max(c.w - 2, 0)}
              height={H - 2}
              fill={isActive ? fillActive : fillBase}
              stroke={activeGroup === c.group.group ? "var(--intent-primary)" : "var(--bg-border)"}
              strokeWidth={activeGroup === c.group.group ? 2 : 1}
              rx={2}
              opacity={isActive ? 1 : 0.4}
            />
            {c.w > 40 && (
              <>
                <text x={c.x + c.w / 2} y={H / 2 - 8} textAnchor="middle" fontSize={11} fill="var(--text-primary)" fontFamily="monospace">{c.group.group}</text>
                <text x={c.x + c.w / 2} y={H / 2 + 8} textAnchor="middle" fontSize={10} fill="var(--text-muted)" fontFamily="monospace">{pct(c.group.pct)}</text>
              </>
            )}
          </g>
        );
      })}
    </svg>
  );
}

// ─── Performance mini-chart (pure SVG) ────────────────────────────────────────

function PerfChart({ data }: { data: PerformanceData }) {
  if (!data.portfolio.length) return (
    <p className="font-mono text-[12px] text-text-muted py-4">No performance data.</p>
  );

  const W = 600;
  const H = 140;
  const PAD = 8;

  const allVals = [...data.portfolio.map((p) => p.value), ...data.benchmark.map((p) => p.value)].filter(Boolean);
  if (!allVals.length) return null;
  const minV = Math.min(...allVals);
  const maxV = Math.max(...allVals);
  const range = maxV - minV || 1;

  function toSVG(points: { date: string; value: number }[]): string {
    if (!points.length) return "";
    const n = points.length;
    return points
      .map((p, i) => {
        const x = PAD + (i / (n - 1)) * (W - PAD * 2);
        const y = PAD + ((maxV - p.value) / range) * (H - PAD * 2);
        return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
  }

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: H }}>
      {data.benchmark.length > 0 && (
        <path d={toSVG(data.benchmark)} fill="none" stroke="var(--text-muted)" strokeWidth={1.5} strokeDasharray="4 2" />
      )}
      <path d={toSVG(data.portfolio)} fill="none" stroke="var(--intent-primary)" strokeWidth={2} />
    </svg>
  );
}

// ─── Correlation heatmap ───────────────────────────────────────────────────────

function CorrelationHeatmap({ data }: { data: CorrelationMatrix }) {
  const n = data.tickers.length;
  if (n < 2) return null;

  const CELL = 44;
  const LABEL = 56;
  const W = LABEL + n * CELL;
  const H = LABEL + n * CELL;

  function corColor(r: number): string {
    // blue=negative, white=zero, red=positive
    if (r >= 0) {
      const t = r;
      const R = Math.round(180 * t + 40 * (1 - t));
      const G = Math.round(40 * (1 - t));
      const B = Math.round(40 * (1 - t));
      return `rgb(${R},${G},${B})`;
    } else {
      const t = -r;
      const R = Math.round(40 * (1 - t));
      const G = Math.round(40 * (1 - t));
      const B = Math.round(180 * t + 40 * (1 - t));
      return `rgb(${R},${G},${B})`;
    }
  }

  const warnPairs = new Set(data.warnings.map((w) => `${w.pair[0]}:${w.pair[1]}`));

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: H }}>
      {data.tickers.map((t, i) => (
        <text key={`rl-${i}`} x={LABEL - 4} y={LABEL + i * CELL + CELL / 2 + 4} textAnchor="end" fontSize={10} fill="var(--text-muted)" fontFamily="monospace">{t}</text>
      ))}
      {data.tickers.map((t, j) => (
        <text key={`cl-${j}`} x={LABEL + j * CELL + CELL / 2} y={LABEL - 4} textAnchor="middle" fontSize={10} fill="var(--text-muted)" fontFamily="monospace">{t}</text>
      ))}
      {data.matrix.map((row, i) =>
        row.map((r, j) => {
          const isWarn = warnPairs.has(`${data.tickers[i]}:${data.tickers[j]}`) || warnPairs.has(`${data.tickers[j]}:${data.tickers[i]}`);
          return (
            <g key={`${i}-${j}`}>
              <rect
                x={LABEL + j * CELL}
                y={LABEL + i * CELL}
                width={CELL - 1}
                height={CELL - 1}
                fill={corColor(r)}
                stroke={isWarn ? "var(--intent-warning)" : "transparent"}
                strokeWidth={isWarn ? 2 : 0}
              />
              <text
                x={LABEL + j * CELL + CELL / 2}
                y={LABEL + i * CELL + CELL / 2 + 4}
                textAnchor="middle"
                fontSize={9}
                fill={Math.abs(r) > 0.5 ? "#fff" : "var(--text-muted)"}
                fontFamily="monospace"
              >
                {r.toFixed(2)}
              </text>
            </g>
          );
        })
      )}
    </svg>
  );
}

// ─── Main page ─────────────────────────────────────────────────────────────────

type SortCol = "ticker" | "shares" | "price" | "value" | "change" | "pnl" | "weight";

export default function PortfolioPage() {
  const [holdings, setHoldings] = useState<PortfolioHolding[]>([]);
  const [exposures, setExposures] = useState<NarrativeExposure[]>([]);
  const [summary, setSummary] = useState<PortfolioSummary | null>(null);
  const [allocation, setAllocation] = useState<AllocationGroup[]>([]);
  const [performance, setPerformance] = useState<PerformanceData | null>(null);
  const [correlation, setCorrelation] = useState<CorrelationMatrix | null>(null);
  const [concentration, setConcentration] = useState<ConcentrationData | null>(null);
  const [groupBy, setGroupBy] = useState<"sector" | "asset_class" | "risk">("sector");
  const [activeGroup, setActiveGroup] = useState<string | null>(null);
  const [perfDays, setPerfDays] = useState(90);
  const [sortCol, setSortCol] = useState<SortCol>("value");
  const [sortAsc, setSortAsc] = useState(false);
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [ticker, setTicker] = useState("");
  const [loading, setLoading] = useState(true);

  function refresh() {
    fetchPortfolio().then((d) => setHoldings(d.holdings)).catch(() => setHoldings([]));
    fetchPortfolioExposure().then((d) => setExposures(d.exposures)).catch(() => setExposures([]));
    fetchPortfolioSummary().then(setSummary).catch(() => setSummary(null));
    fetchPortfolioCorrelation().then(setCorrelation).catch(() => setCorrelation(null));
    fetchPortfolioConcentration().then(setConcentration).catch(() => setConcentration(null));
  }

  useEffect(() => {
    setLoading(true);
    Promise.all([
      fetchPortfolio(),
      fetchPortfolioExposure(),
      fetchPortfolioSummary(),
      fetchPortfolioAllocation(groupBy),
      fetchPortfolioPerformance(perfDays),
      fetchPortfolioCorrelation(),
      fetchPortfolioConcentration(),
    ])
      .then(([p, e, s, a, perf, cor, con]) => {
        setHoldings(p.holdings);
        setExposures(e.exposures);
        setSummary(s);
        setAllocation(a);
        setPerformance(perf);
        setCorrelation(cor);
        setConcentration(con);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    fetchPortfolioAllocation(groupBy).then(setAllocation).catch(() => setAllocation([]));
  }, [groupBy]);

  useEffect(() => {
    fetchPortfolioPerformance(perfDays).then(setPerformance).catch(() => setPerformance(null));
  }, [perfDays]);

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

  function handleSort(col: SortCol) {
    if (sortCol === col) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(col);
      setSortAsc(false);
    }
  }

  const totalValue = summary?.total_value || holdings.reduce((s, h) => s + (h as any).current_value || 0, 0);

  const activeGroupTickers = activeGroup
    ? new Set((allocation.find((g) => g.group === activeGroup)?.tickers ?? []).map((t) => t.toUpperCase()))
    : null;

  const filteredHoldings = activeGroupTickers
    ? holdings.filter((h) => activeGroupTickers.has(h.ticker.toUpperCase()))
    : holdings;

  // Sort holdings
  const sortedHoldings = [...filteredHoldings].sort((a, b) => {
    const av = (h: PortfolioHolding) => {
      if (sortCol === "ticker") return h.ticker;
      if (sortCol === "shares") return h.shares;
      if (sortCol === "price") return (h as any).current_price || 0;
      if (sortCol === "value") return (h as any).current_value || 0;
      if (sortCol === "change") return h.price_change_24h || 0;
      if (sortCol === "pnl") return (h as any).pnl || 0;
      if (sortCol === "weight") return totalValue ? ((h as any).current_value || 0) / totalValue : 0;
      return 0;
    };
    const va = av(a);
    const vb = av(b);
    const cmp = typeof va === "string" ? va.localeCompare(vb as string) : (va as number) - (vb as number);
    return sortAsc ? cmp : -cmp;
  });

  function SortHeader({ col, label }: { col: SortCol; label: string }) {
    return (
      <th
        className="font-mono text-[10px] uppercase tracking-[0.06em] text-text-muted px-3 py-2 text-right cursor-pointer select-none"
        onClick={() => handleSort(col)}
      >
        {label} {sortCol === col ? (sortAsc ? <ChevronUp size={10} className="inline" /> : <ChevronDown size={10} className="inline" />) : null}
      </th>
    );
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
          <div className="flex flex-col gap-4">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {[1, 2, 3, 4].map((i) => <Skeleton key={i} height={72} />)}
            </div>
            {[1, 2, 3].map((i) => <Skeleton key={i} height={120} />)}
          </div>
        ) : (
          <div className="flex flex-col gap-8">

            {/* ── 1. Header Stats ── */}
            {summary && (
              <section>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3" data-testid="portfolio-summary">
                  <StatCard label="Total Value" value={fmt(summary.total_value)} />
                  <StatCard
                    label="Total P&L"
                    value={fmt(Math.abs(summary.total_pnl))}
                    sub={`${summary.total_pnl >= 0 ? "+" : "-"}${Math.abs(summary.total_pnl / (summary.total_value || 1) * 100).toFixed(2)}%`}
                    positive={summary.total_pnl >= 0}
                  />
                  <StatCard
                    label="Day Change"
                    value={fmt(Math.abs(summary.day_change))}
                    sub={`${summary.day_change >= 0 ? "+" : ""}${summary.day_change_pct}%`}
                    positive={summary.day_change >= 0}
                  />
                  <StatCard label="Positions" value={String(summary.position_count)} />
                </div>
              </section>
            )}

            {/* ── 2. Concentration Warnings ── */}
            {concentration && (concentration.top3_warning || concentration.sector_concentrated || concentration.single_stock_warnings.length > 0) && (
              <section data-testid="concentration-warnings">
                <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-3 flex items-center gap-2">
                  <AlertTriangle size={14} className="text-[var(--intent-warning)]" />
                  Concentration Warnings
                </h2>
                <div className="flex flex-col gap-2">
                  {concentration.top3_warning && (
                    <div className="px-4 py-3 border border-[var(--intent-warning)] border-opacity-40 bg-[var(--bg-panel)] rounded-sm">
                      <p className="font-mono text-[12px] text-[var(--intent-warning)]">
                        Top 3 holdings account for {(concentration.top3_pct * 100).toFixed(1)}% of portfolio (&gt;60% threshold)
                      </p>
                    </div>
                  )}
                  {concentration.sector_concentrated && (
                    <div className="px-4 py-3 border border-[var(--intent-warning)] border-opacity-40 bg-[var(--bg-panel)] rounded-sm">
                      <p className="font-mono text-[12px] text-[var(--intent-warning)]">
                        Sector concentration (HHI {concentration.sector_hhi.toFixed(0)}) exceeds 2500 — limited diversification
                      </p>
                    </div>
                  )}
                  {concentration.single_stock_warnings.map((w) => (
                    <div key={w.ticker} className="px-4 py-3 border border-[var(--intent-warning)] border-opacity-40 bg-[var(--bg-panel)] rounded-sm">
                      <p className="font-mono text-[12px] text-[var(--intent-warning)]">
                        {w.ticker} is {(w.pct * 100).toFixed(1)}% of portfolio (&gt;25% single-stock concentration)
                      </p>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {/* ── 3. Allocation Treemap ── */}
            {allocation.length > 0 && (
              <section data-testid="allocation-treemap">
                <div className="flex items-center justify-between mb-3">
                  <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                    Allocation
                  </h2>
                  <div className="flex gap-1">
                    {(["sector", "asset_class", "risk"] as const).map((g) => (
                      <button
                        key={g}
                        onClick={() => { setGroupBy(g); setActiveGroup(null); }}
                        className={`font-mono text-[10px] px-2.5 py-1 rounded-sm border transition-colors ${
                          groupBy === g
                            ? "bg-accent-primary border-accent-primary text-text-primary"
                            : "bg-transparent border-[var(--bg-border)] text-text-muted hover:text-text-primary"
                        }`}
                      >
                        {g.replace("_", " ")}
                      </button>
                    ))}
                  </div>
                </div>
                <AllocationTreemap data={allocation} onGroupClick={setActiveGroup} activeGroup={activeGroup} />
                {activeGroup && (
                  <p className="font-mono text-[11px] text-text-muted mt-2">
                    Filtered to: <span className="text-text-primary">{activeGroup}</span>
                    <button className="ml-2 underline" onClick={() => setActiveGroup(null)}>clear</button>
                  </p>
                )}
              </section>
            )}

            {/* ── 4. Performance Chart ── */}
            <section>
              <div className="flex items-center justify-between mb-3">
                <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                  Performance
                  {performance && (
                    <span className="font-mono text-[11px] font-normal text-text-muted ml-2">
                      vs SPY —
                      <span className={performance.total_return_pct >= 0 ? " text-bullish" : " text-bearish"}>
                        {" "}{performance.total_return_pct >= 0 ? "+" : ""}{performance.total_return_pct}%
                      </span>
                      {" "}vs
                      <span className={performance.benchmark_return_pct >= 0 ? " text-bullish" : " text-bearish"}>
                        {" "}{performance.benchmark_return_pct >= 0 ? "+" : ""}{performance.benchmark_return_pct}%
                      </span>
                    </span>
                  )}
                </h2>
                <div className="flex gap-1">
                  {([30, 90, 180, 365] as const).map((d) => (
                    <button
                      key={d}
                      onClick={() => setPerfDays(d)}
                      className={`font-mono text-[10px] px-2 py-1 rounded-sm border transition-colors ${
                        perfDays === d
                          ? "bg-accent-primary border-accent-primary text-text-primary"
                          : "bg-transparent border-[var(--bg-border)] text-text-muted hover:text-text-primary"
                      }`}
                    >
                      {d === 30 ? "1M" : d === 90 ? "3M" : d === 180 ? "6M" : "1Y"}
                    </button>
                  ))}
                </div>
              </div>
              <div className="bg-[var(--bg-panel)] border border-[var(--bg-border)] rounded-sm p-3">
                {performance ? <PerfChart data={performance} /> : <Skeleton height={140} />}
                <div className="flex gap-4 mt-2">
                  <span className="font-mono text-[10px] text-text-muted flex items-center gap-1">
                    <span style={{ display: "inline-block", width: 16, height: 2, background: "var(--intent-primary)" }} /> Portfolio
                  </span>
                  <span className="font-mono text-[10px] text-text-muted flex items-center gap-1">
                    <span style={{ display: "inline-block", width: 16, height: 2, background: "var(--text-muted)", borderTop: "1px dashed var(--text-muted)" }} /> Benchmark (SPY)
                  </span>
                </div>
              </div>
            </section>

            {/* ── 5. Holdings Table ── */}
            <section>
              <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-3">
                Holdings
                <span className="font-mono text-[11px] font-normal text-text-muted ml-2">{holdings.length}</span>
              </h2>
              {holdings.length === 0 ? (
                <p className="font-mono text-[12px] text-text-muted py-5">
                  No holdings yet. Add a ticker above to start tracking narrative exposure.
                </p>
              ) : (
                <div className="bg-[var(--bg-panel)] border border-[var(--bg-border)] rounded-sm overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr style={{ borderBottom: "1px solid var(--bg-border)" }}>
                        <th className="font-mono text-[10px] uppercase tracking-[0.06em] text-text-muted px-3 py-2 text-left">Ticker</th>
                        <SortHeader col="shares" label="Shares" />
                        <SortHeader col="price" label="Price" />
                        <SortHeader col="value" label="Value" />
                        <SortHeader col="change" label="Day %" />
                        <SortHeader col="pnl" label="P&L" />
                        <SortHeader col="weight" label="Weight" />
                        <th className="w-8" />
                      </tr>
                    </thead>
                    <tbody>
                      {sortedHoldings.map((h) => {
                        const ext = h as any;
                        const isExpanded = expandedRow === h.id;
                        const weight = totalValue ? ((ext.current_value || 0) / totalValue) : 0;
                        return (
                          <>
                            <tr
                              key={h.id}
                              className="cursor-pointer hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
                              style={{ borderBottom: "1px solid rgba(56,62,71,0.13)" }}
                              onClick={() => setExpandedRow(isExpanded ? null : h.id)}
                            >
                              <td className="font-mono text-[13px] font-bold text-text-primary px-3 py-2.5">{h.ticker}</td>
                              <td className="font-mono text-[12px] text-text-muted px-3 py-2.5 text-right">{h.shares}</td>
                              <td className="font-mono text-[12px] text-text-muted px-3 py-2.5 text-right">{ext.current_price != null ? `$${ext.current_price.toFixed(2)}` : "—"}</td>
                              <td className="font-mono text-[12px] text-text-primary px-3 py-2.5 text-right">{ext.current_value != null ? fmt(ext.current_value) : "—"}</td>
                              <td className={`font-mono text-[12px] px-3 py-2.5 text-right ${(h.price_change_24h || 0) >= 0 ? "text-bullish" : "text-bearish"}`}>
                                {h.price_change_24h != null ? `${h.price_change_24h >= 0 ? "+" : ""}${h.price_change_24h.toFixed(2)}%` : "—"}
                              </td>
                              <td className={`font-mono text-[12px] px-3 py-2.5 text-right ${(ext.pnl || 0) >= 0 ? "text-bullish" : "text-bearish"}`}>
                                {ext.pnl != null ? fmt(ext.pnl) : "—"}
                              </td>
                              <td className="font-mono text-[11px] text-text-muted px-3 py-2.5 text-right">{(weight * 100).toFixed(1)}%</td>
                              <td className="px-2 py-2.5">
                                <button
                                  onClick={(e) => { e.stopPropagation(); handleRemove(h.id); }}
                                  className="bg-transparent border-none text-text-muted cursor-pointer p-1 hover:text-bearish transition-colors"
                                  aria-label={`Remove ${h.ticker}`}
                                >
                                  <Trash2 size={13} />
                                </button>
                              </td>
                            </tr>
                            {isExpanded && (
                              <tr key={`${h.id}-exp`} style={{ borderBottom: "1px solid rgba(56,62,71,0.13)" }}>
                                <td colSpan={8} className="px-4 py-3 bg-[var(--bg-base)]">
                                  <p className="font-mono text-[11px] text-text-muted">
                                    Added: {h.added_at ? new Date(h.added_at).toLocaleDateString() : "—"} &nbsp;|&nbsp;
                                    Cost basis: {(h as any).cost_basis != null ? `$${(h as any).cost_basis.toFixed(2)}/share` : "—"}
                                  </p>
                                </td>
                              </tr>
                            )}
                          </>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </section>

            {/* ── 6. Correlation Matrix ── */}
            {correlation && correlation.tickers.length >= 3 && (
              <section data-testid="correlation-matrix">
                <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary mb-3">
                  Correlation Matrix
                  {correlation.warnings.length > 0 && (
                    <span className="font-mono text-[10px] font-normal text-[var(--intent-warning)] ml-2">
                      {correlation.warnings.length} high-correlation pair{correlation.warnings.length > 1 ? "s" : ""}
                    </span>
                  )}
                </h2>
                <div className="bg-[var(--bg-panel)] border border-[var(--bg-border)] rounded-sm p-4 overflow-x-auto">
                  <CorrelationHeatmap data={correlation} />
                </div>
              </section>
            )}

            {/* ── 7. Narrative Exposure ── */}
            <section>
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
                    <Link key={e.narrative_id} href={`/narrative/${e.narrative_id}`} className="no-underline">
                      <div
                        className="px-4 py-2.5 cursor-pointer transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)]"
                        style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)", borderLeft: `3px solid ${velColor(e.velocity)}` }}
                      >
                        <div className="flex justify-between items-baseline">
                          <span className="text-[13px] text-text-primary font-display">{e.narrative_name}</span>
                          <span className="font-mono text-[14px] font-bold" style={{ color: velColor(e.velocity) }}>
                            {e.velocity > 0 ? "+" : ""}{e.velocity.toFixed(1)}
                          </span>
                        </div>
                        <div className="flex gap-2 mt-1">
                          <StageBadge stage={e.stage} />
                          {e.affected_tickers.map((t) => (
                            <span key={t} className="font-mono text-[10px] text-text-muted border border-[var(--bg-border)] px-1.5 py-[1px] rounded-sm">{t}</span>
                          ))}
                        </div>
                      </div>
                    </Link>
                  ))}
                </div>
              )}
            </section>

          </div>
        )}

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
