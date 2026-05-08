"use client";

import { X } from "lucide-react";

export type WidgetType =
  | "narrative_radar"
  | "watchlist"
  | "market_heatmap"
  | "top_movers"
  | "sentiment_meter"
  | "portfolio_summary"
  | "alert_feed"
  | "convergence_radar"
  | "signal_leaderboard"
  | "economic_calendar";

export interface WidgetDefinition {
  type: WidgetType;
  title: string;
  description: string;
}

export const WIDGET_DEFINITIONS: WidgetDefinition[] = [
  { type: "narrative_radar", title: "Narrative Radar", description: "Top narratives by Ns score" },
  { type: "signal_leaderboard", title: "Signal Leaderboard", description: "Top directional signals" },
  { type: "top_movers", title: "Top Movers", description: "Biggest price movers" },
  { type: "sentiment_meter", title: "Sentiment Meter", description: "Market sentiment gauge" },
  { type: "alert_feed", title: "Alert Feed", description: "Recent notifications" },
  { type: "watchlist", title: "Watchlist", description: "Your watchlisted tickers and narratives" },
  { type: "market_heatmap", title: "Market Heatmap", description: "Ticker sentiment heatmap" },
  { type: "portfolio_summary", title: "Portfolio Summary", description: "Portfolio value and P&L" },
  { type: "convergence_radar", title: "Convergence Radar", description: "Top convergence pressure tickers" },
  { type: "economic_calendar", title: "Economic Calendar", description: "Upcoming earnings and FOMC" },
];

interface WidgetCatalogProps {
  activeTypes: WidgetType[];
  onAdd: (type: WidgetType) => void;
  onClose: () => void;
}

export default function WidgetCatalog({ activeTypes, onAdd, onClose }: WidgetCatalogProps) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60" data-testid="widget-catalog">
      <div
        className="bg-surface border border-[var(--bg-border)] rounded-sm w-full max-w-lg mx-4 p-5"
        style={{ boxShadow: "0 4px 32px rgba(0,0,0,0.5)", maxHeight: "80vh", overflowY: "auto" }}
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-[14px] font-semibold text-text-primary">Add Widget</h3>
          <button onClick={onClose} className="text-text-muted hover:text-text-primary transition-colors">
            <X size={16} />
          </button>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
          {WIDGET_DEFINITIONS.map((w) => {
            const isActive = activeTypes.includes(w.type);
            return (
              <div
                key={w.type}
                className={`p-3 border border-[var(--bg-border)] rounded-sm ${isActive ? "opacity-40" : "hover:border-accent-text transition-colors"}`}
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0">
                    <p className="text-[12px] font-semibold text-text-primary">{w.title}</p>
                    <p className="font-mono text-[10px] text-text-muted mt-0.5">{w.description}</p>
                  </div>
                  <button
                    onClick={() => !isActive && onAdd(w.type)}
                    disabled={isActive}
                    className="shrink-0 font-mono text-[10px] text-accent-text border border-[var(--bg-border)] px-2 py-1 rounded-sm hover:opacity-80 transition-opacity disabled:opacity-30 disabled:cursor-not-allowed"
                    data-testid={`add-widget-${w.type}`}
                  >
                    {isActive ? "Added" : "Add"}
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
