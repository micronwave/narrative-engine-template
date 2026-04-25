"use client";

import { useQuery } from "@tanstack/react-query";
import { X } from "lucide-react";
import type { WidgetType } from "./WidgetCatalog";
import {
  fetchAlerts,
  fetchMarketSentiment,
  fetchNarratives,
  fetchPortfolioSummary,
  fetchSectorConvergence,
  fetchSignalLeaderboard,
  fetchStocks,
  fetchUpcomingEarnings,
  fetchWatchlist,
  type AlertNotification,
  type Narrative,
  type SignalLeaderboardEntry,
  type VisibleNarrative,
} from "@/lib/api";

interface WidgetRendererProps {
  id: string;
  type: WidgetType;
  title: string;
  isEditing: boolean;
  compact?: boolean;
  onRemove: (id: string) => void;
}

function asVisibleNarrative(narrative: Narrative): VisibleNarrative {
  if (!("blurred" in narrative) || narrative.blurred === false) {
    return narrative as VisibleNarrative;
  }
  return {
    id: narrative.id,
    name: "Narrative",
    descriptor: "Narrative data is loading.",
    velocity_summary: "+0.0% signal velocity over 7d",
    entropy: null,
    saturation: 0,
    velocity_timeseries: [],
    signals: [],
    catalysts: [],
    mutations: [],
    blurred: false,
  };
}

// Shared loading placeholder — keeps the original "loading…" text for each type
function LoadingState({ label }: { label: string }) {
  return (
    <div className="p-3 font-mono text-[11px] text-text-muted flex items-center justify-center h-full">
      {label} — loading…
    </div>
  );
}

// ─── 1. Narrative Radar ───────────────────────────────────────────────────────
function NarrativeRadarWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<Narrative[]>({
    queryKey: ["narratives"],
    queryFn: fetchNarratives,
  });
  if (isLoading) return <LoadingState label="Narrative Radar" />;
  const narratives = (Array.isArray(data) ? data : [])
    .map(asVisibleNarrative)
    .slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-narrative_radar" className="overflow-y-auto h-full p-2">
      {narratives.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">No active narratives</div>
      ) : (
        narratives.map((n) => (
          <div
            key={n.id}
            className="py-1.5 px-2 border-b border-[var(--bg-border)] flex items-center justify-between gap-2"
          >
            <span className="font-mono text-[11px] text-text-primary truncate">{n.name}</span>
            {n.stage && (
              <span className="font-mono text-[10px] text-text-muted shrink-0">{n.stage}</span>
            )}
          </div>
        ))
      )}
    </div>
  );
}

// ─── 2. Signal Leaderboard ────────────────────────────────────────────────────
type SignalEntry = Pick<SignalLeaderboardEntry, "narrative_id" | "name" | "direction" | "confidence" | "stage">;
type LeaderboardResponse = SignalEntry[] | { signals: SignalEntry[] };

function SignalLeaderboardWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<LeaderboardResponse>({
    queryKey: ["signal-leaderboard"],
    queryFn: () => fetchSignalLeaderboard(),
  });
  if (isLoading) return <LoadingState label="Signal Leaderboard" />;
  const signals: SignalEntry[] = Array.isArray(data)
    ? data
    : ((data as { signals?: SignalEntry[] })?.signals ?? []);
  const rows = signals.slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-signal_leaderboard" className="overflow-y-auto h-full p-2">
      {rows.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">No signals</div>
      ) : (
        rows.map((s, i) => (
          <div
            key={s.narrative_id || i}
            className="py-1.5 px-2 border-b border-[var(--bg-border)] flex items-center gap-2"
          >
            <span className="font-mono text-[10px] text-text-muted w-4 shrink-0">{i + 1}</span>
            <span className="font-mono text-[11px] text-text-primary flex-1 truncate">{s.name}</span>
            <span
              className={`font-mono text-[10px] shrink-0 ${
                s.direction === "bullish"
                  ? "text-bullish"
                  : s.direction === "bearish"
                  ? "text-bearish"
                  : "text-text-muted"
              }`}
            >
              {s.direction}
            </span>
            <span className="font-mono text-[10px] text-text-muted shrink-0">
              {s.confidence != null ? `${Math.round(s.confidence * 100)}%` : "—"}
            </span>
          </div>
        ))
      )}
    </div>
  );
}

// ─── 3. Top Movers ────────────────────────────────────────────────────────────
type StockItem = {
  id?: string;
  symbol: string;
  name: string;
  current_price: number | null;
  price_change_24h: number | null;
};

function TopMoversWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<StockItem[]>({
    queryKey: ["stocks-top-movers"],
    queryFn: () =>
      fetchStocks({ sort_by: "change", sort_order: "desc" }) as Promise<StockItem[]>,
  });
  if (isLoading) return <LoadingState label="Top Movers" />;
  const stocks = (Array.isArray(data) ? data : []).slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-top_movers" className="overflow-y-auto h-full p-2">
      {stocks.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">No data</div>
      ) : (
        stocks.map((s) => (
          <div
            key={s.symbol}
            className="py-1.5 px-2 border-b border-[var(--bg-border)] flex items-center justify-between gap-2"
          >
            <span className="font-mono text-[11px] font-medium text-text-primary w-14 shrink-0">
              {s.symbol}
            </span>
            <span className="font-mono text-[10px] text-text-muted flex-1 truncate">{s.name}</span>
            {s.price_change_24h != null && (
              <span
                className={`font-mono text-[11px] shrink-0 ${
                  s.price_change_24h >= 0 ? "text-bullish" : "text-bearish"
                }`}
              >
                {s.price_change_24h >= 0 ? "+" : ""}
                {s.price_change_24h.toFixed(2)}%
              </span>
            )}
          </div>
        ))
      )}
    </div>
  );
}

// ─── 4. Sentiment Meter ───────────────────────────────────────────────────────
type MarketSentiment = {
  market_score: number;
  bullish_pct: number;
  bearish_pct: number;
  neutral_pct: number;
};

function SentimentMeterWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<MarketSentiment>({
    queryKey: ["sentiment-market"],
    queryFn: fetchMarketSentiment,
  });
  if (isLoading) return <LoadingState label="Sentiment Meter" />;
  const score = data?.market_score ?? 0;
  const bullish = data?.bullish_pct ?? 0;
  const bearish = data?.bearish_pct ?? 0;
  const neutral = data?.neutral_pct ?? 100;
  const pct = Math.max(0, Math.min(100, Math.round((score + 1) * 50)));
  const label = score > 0.2 ? "Bullish" : score < -0.2 ? "Bearish" : "Neutral";
  const labelColor =
    score > 0.2
      ? "var(--intent-success)"
      : score < -0.2
      ? "var(--intent-danger)"
      : "var(--text-muted)";
  return (
    <div data-testid="widget-body-sentiment_meter" className="p-3 flex flex-col gap-3 h-full">
      <div>
        <div className="flex justify-between mb-1">
          <span className="font-mono text-[10px] text-bearish">Bear</span>
          <span className="font-mono text-[11px] font-medium" style={{ color: labelColor }}>
            {label}
          </span>
          <span className="font-mono text-[10px] text-bullish">Bull</span>
        </div>
        <div className="h-2 rounded-full bg-[var(--bg-border)] overflow-hidden">
          <div
            className="h-full rounded-full"
            style={{ width: `${pct}%`, background: labelColor, transition: "width 0.3s ease" }}
          />
        </div>
      </div>
      {!compact && (
        <div className="grid grid-cols-3 gap-1">
          <div className="text-center">
            <div className="font-mono text-[12px] text-bullish font-medium">{bullish.toFixed(0)}%</div>
            <div className="font-mono text-[9px] text-text-muted uppercase tracking-wide">Bull</div>
          </div>
          <div className="text-center">
            <div className="font-mono text-[12px] text-text-muted font-medium">
              {neutral.toFixed(0)}%
            </div>
            <div className="font-mono text-[9px] text-text-muted uppercase tracking-wide">Neutral</div>
          </div>
          <div className="text-center">
            <div className="font-mono text-[12px] text-bearish font-medium">{bearish.toFixed(0)}%</div>
            <div className="font-mono text-[9px] text-text-muted uppercase tracking-wide">Bear</div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── 5. Alert Feed ────────────────────────────────────────────────────────────
type AlertItem = {
  id: string;
  title: string;
  message: string;
  is_read: boolean;
  rule_type?: string;
};

function AlertFeedWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<AlertItem[]>({
    queryKey: ["alerts-feed"],
    queryFn: async () => {
      const alerts: AlertNotification[] = await fetchAlerts();
      return alerts.map((a) => ({ ...a, is_read: Boolean(a.is_read) }));
    },
  });
  if (isLoading) return <LoadingState label="Alert Feed" />;
  const alerts = (Array.isArray(data) ? data : [])
    .filter((a) => !a.is_read)
    .slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-alert_feed" className="overflow-y-auto h-full p-2">
      {alerts.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">No unread alerts</div>
      ) : (
        alerts.map((a) => (
          <div key={a.id} className="py-1.5 px-2 border-b border-[var(--bg-border)]">
            <div className="font-mono text-[11px] text-text-primary truncate">{a.title}</div>
            {!compact && (
              <div className="font-mono text-[10px] text-text-muted truncate">{a.message}</div>
            )}
          </div>
        ))
      )}
    </div>
  );
}

// ─── 6. Watchlist ─────────────────────────────────────────────────────────────
type WatchlistItem = {
  id: string;
  item_id: string;
  item_type: "narrative" | "ticker";
  name?: string;
  current_price?: number | null;
  price_change_24h?: number | null;
};

function WatchlistWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<{ items: WatchlistItem[]; watchlist_id: string | null }>({
    queryKey: ["watchlist"],
    queryFn: () => fetchWatchlist() as Promise<{ items: WatchlistItem[]; watchlist_id: string | null }>,
  });
  if (isLoading) return <LoadingState label="Watchlist" />;
  const items = (data?.items ?? []).slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-watchlist" className="overflow-y-auto h-full p-2">
      {items.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">Nothing in watchlist</div>
      ) : (
        items.map((item) => (
          <div
            key={item.id}
            className="py-1.5 px-2 border-b border-[var(--bg-border)] flex items-center justify-between gap-2"
          >
            <span className="font-mono text-[11px] text-text-primary truncate">
              {item.item_type === "ticker" ? item.item_id : (item.name ?? item.item_id)}
            </span>
            {item.item_type === "ticker" && item.current_price != null && (
              <span className="font-mono text-[11px] text-text-muted shrink-0">
                ${item.current_price.toFixed(2)}
              </span>
            )}
          </div>
        ))
      )}
    </div>
  );
}

// ─── 7. Market Heatmap ────────────────────────────────────────────────────────
function MarketHeatmapWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<StockItem[]>({
    queryKey: ["stocks-heatmap"],
    queryFn: () => fetchStocks() as Promise<StockItem[]>,
  });
  if (isLoading) return <LoadingState label="Market Heatmap" />;
  const stocks = (Array.isArray(data) ? data : []).slice(0, compact ? 9 : 15);
  return (
    <div
      data-testid="widget-body-market_heatmap"
      className="p-2 grid grid-cols-3 gap-1 content-start overflow-y-auto h-full"
    >
      {stocks.length === 0 ? (
        <div className="col-span-3 font-mono text-[11px] text-text-muted p-2">No data</div>
      ) : (
        stocks.map((s) => {
          const chg = s.price_change_24h ?? 0;
          const bg =
            chg > 1
              ? "rgba(15,160,67,0.18)"
              : chg > 0
              ? "rgba(15,160,67,0.09)"
              : chg < -1
              ? "rgba(219,55,55,0.18)"
              : chg < 0
              ? "rgba(219,55,55,0.09)"
              : "var(--bg-border)";
          const color =
            chg > 0
              ? "var(--intent-success)"
              : chg < 0
              ? "var(--intent-danger)"
              : "var(--text-muted)";
          return (
            <div
              key={s.symbol}
              className="rounded-sm p-1.5 flex flex-col items-center justify-center"
              style={{ background: bg, minHeight: 36 }}
            >
              <span className="font-mono text-[11px] font-medium text-text-primary">{s.symbol}</span>
              <span className="font-mono text-[9px]" style={{ color }}>
                {chg >= 0 ? "+" : ""}
                {chg.toFixed(1)}%
              </span>
            </div>
          );
        })
      )}
    </div>
  );
}

// ─── 8. Portfolio Summary ─────────────────────────────────────────────────────
type PortfolioSummary = {
  total_value: number;
  total_pnl: number;
  day_change: number;
  day_change_pct: number;
  position_count: number;
};

function PortfolioSummaryWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<PortfolioSummary>({
    queryKey: ["portfolio-summary"],
    queryFn: fetchPortfolioSummary,
  });
  if (isLoading) return <LoadingState label="Portfolio Summary" />;
  const totalValue = data?.total_value ?? 0;
  const totalPnl = data?.total_pnl ?? 0;
  const dayChange = data?.day_change ?? 0;
  const dayChangePct = data?.day_change_pct ?? 0;
  const posCount = data?.position_count ?? 0;
  return (
    <div data-testid="widget-body-portfolio_summary" className="p-3 grid grid-cols-2 gap-2">
      <div className="flex flex-col">
        <span className="font-mono text-[9px] text-text-muted uppercase tracking-wide">Value</span>
        <span className="font-mono text-[13px] text-text-primary font-medium">
          ${totalValue.toLocaleString()}
        </span>
      </div>
      <div className="flex flex-col">
        <span className="font-mono text-[9px] text-text-muted uppercase tracking-wide">P&L</span>
        <span
          className={`font-mono text-[13px] font-medium ${
            totalPnl >= 0 ? "text-bullish" : "text-bearish"
          }`}
        >
          {totalPnl >= 0 ? "+" : ""}${totalPnl.toLocaleString()}
        </span>
      </div>
      {!compact && (
        <>
          <div className="flex flex-col">
            <span className="font-mono text-[9px] text-text-muted uppercase tracking-wide">Day</span>
            <span
              className={`font-mono text-[13px] font-medium ${
                dayChange >= 0 ? "text-bullish" : "text-bearish"
              }`}
            >
              {dayChange >= 0 ? "+" : ""}${dayChange.toFixed(0)} ({dayChangePct >= 0 ? "+" : ""}
              {dayChangePct.toFixed(1)}%)
            </span>
          </div>
          <div className="flex flex-col">
            <span className="font-mono text-[9px] text-text-muted uppercase tracking-wide">
              Positions
            </span>
            <span className="font-mono text-[13px] text-text-primary font-medium">{posCount}</span>
          </div>
        </>
      )}
    </div>
  );
}

// ─── 9. Convergence Radar ─────────────────────────────────────────────────────
type ConvergenceSector = {
  name: string;
  narrative_count: number;
  weighted_pressure: number;
};

function ConvergenceRadarWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<{ sectors?: ConvergenceSector[] }>({
    queryKey: ["convergence-sectors"],
    queryFn: () => fetchSectorConvergence() as Promise<{ sectors?: ConvergenceSector[] }>,
  });
  if (isLoading) return <LoadingState label="Convergence Radar" />;
  const sectors = (data?.sectors ?? []).slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-convergence_radar" className="overflow-y-auto h-full p-2">
      {sectors.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">No convergence data</div>
      ) : (
        sectors.map((s) => (
          <div
            key={s.name}
            className="py-1.5 px-2 border-b border-[var(--bg-border)] flex items-center justify-between gap-2"
          >
            <span className="font-mono text-[11px] text-text-primary truncate">{s.name}</span>
            <div className="flex items-center gap-2 shrink-0">
              <span className="font-mono text-[10px] text-text-muted">{s.narrative_count} nar</span>
              <span className="font-mono text-[10px] text-alert">
                {s.weighted_pressure.toFixed(1)}
              </span>
            </div>
          </div>
        ))
      )}
    </div>
  );
}

// ─── 10. Economic Calendar ────────────────────────────────────────────────────
type EarningsItem = {
  ticker: string;
  company?: string;
  date?: string;
  days_until?: number;
};

function EconomicCalendarWidget({ compact }: { compact?: boolean }) {
  const { data, isLoading } = useQuery<EarningsItem[]>({
    queryKey: ["earnings-upcoming"],
    queryFn: () => fetchUpcomingEarnings() as Promise<EarningsItem[]>,
  });
  if (isLoading) return <LoadingState label="Economic Calendar" />;
  const items = (Array.isArray(data) ? data : []).slice(0, compact ? 3 : 5);
  return (
    <div data-testid="widget-body-economic_calendar" className="overflow-y-auto h-full p-2">
      {items.length === 0 ? (
        <div className="font-mono text-[11px] text-text-muted p-2">No upcoming earnings</div>
      ) : (
        items.map((item, i) => (
          <div
            key={`${item.ticker}-${i}`}
            className="py-1.5 px-2 border-b border-[var(--bg-border)] flex items-center justify-between gap-2"
          >
            <span className="font-mono text-[11px] font-medium text-text-primary shrink-0 w-12">
              {item.ticker}
            </span>
            <span className="font-mono text-[10px] text-text-muted flex-1 truncate">
              {item.company ?? ""}
            </span>
            {item.days_until != null && (
              <span className="font-mono text-[10px] text-text-muted shrink-0">
                {item.days_until === 0 ? "Today" : `${item.days_until}d`}
              </span>
            )}
          </div>
        ))
      )}
    </div>
  );
}

// ─── Widget body dispatch ─────────────────────────────────────────────────────
function WidgetBody({ type, compact }: { type: WidgetType; compact?: boolean }) {
  switch (type) {
    case "narrative_radar":
      return <NarrativeRadarWidget compact={compact} />;
    case "signal_leaderboard":
      return <SignalLeaderboardWidget compact={compact} />;
    case "top_movers":
      return <TopMoversWidget compact={compact} />;
    case "sentiment_meter":
      return <SentimentMeterWidget compact={compact} />;
    case "alert_feed":
      return <AlertFeedWidget compact={compact} />;
    case "watchlist":
      return <WatchlistWidget compact={compact} />;
    case "market_heatmap":
      return <MarketHeatmapWidget compact={compact} />;
    case "portfolio_summary":
      return <PortfolioSummaryWidget compact={compact} />;
    case "convergence_radar":
      return <ConvergenceRadarWidget compact={compact} />;
    case "economic_calendar":
      return <EconomicCalendarWidget compact={compact} />;
    default:
      return null;
  }
}

export default function WidgetRenderer({
  id,
  type,
  title,
  isEditing,
  compact,
  onRemove,
}: WidgetRendererProps) {
  return (
    <div
      className="bg-surface border border-[var(--bg-border)] rounded-sm flex flex-col h-full"
      data-testid={`widget-${type}`}
    >
      <div
        className="flex items-center justify-between px-3 py-2"
        style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
      >
        <span className="font-mono text-[11px] uppercase tracking-[0.05em] text-text-secondary">
          {title}
        </span>
        {isEditing && (
          <button
            onClick={() => onRemove(id)}
            className="text-text-muted hover:text-bearish transition-colors"
            aria-label={`Remove ${title} widget`}
          >
            <X size={12} />
          </button>
        )}
      </div>
      <div className="flex-1 min-h-0">
        <WidgetBody type={type} compact={compact} />
      </div>
    </div>
  );
}
