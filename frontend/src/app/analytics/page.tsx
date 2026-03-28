"use client";

import { useState } from "react";
import GlobalTimeRange from "@/components/analytics/GlobalTimeRange";
import MomentumLeaderboard from "@/components/analytics/MomentumLeaderboard";
import NarrativePriceTimeline from "@/components/analytics/NarrativePriceTimeline";
import NarrativeOverlapHeatmap from "@/components/analytics/NarrativeOverlapHeatmap";
import SectorConvergenceBubbles from "@/components/analytics/SectorConvergenceBubbles";
import ContrarianSignalCards from "@/components/analytics/ContrarianSignalCards";
import LifecycleFunnel from "@/components/analytics/LifecycleFunnel";
import LeadTimeHistogram from "@/components/analytics/LeadTimeHistogram";

export default function AnalyticsPage() {
  const [timeRange, setTimeRange] = useState("30d");

  return (
    <div className="max-w-[1280px] mx-auto px-8 pt-6 pb-16">
      {/* Header */}
      <div className="flex items-baseline justify-between mb-2">
        <h1 className="font-display text-[22px] font-semibold tracking-[-0.01em] text-text-primary m-0">
          Analytics
        </h1>
        <GlobalTimeRange value={timeRange} onChange={setTimeRange} />
      </div>
      <div className="h-px bg-bg-border opacity-50 mb-8" />

      {/* Momentum Leaderboard — full width */}
      <div className="mb-10">
        <MomentumLeaderboard timeRange={timeRange} />
      </div>

      {/* Narrative → Price Timeline — full width */}
      <div className="mb-10">
        <NarrativePriceTimeline timeRange={timeRange} />
      </div>

      {/* Two-column row: Heatmap + Sector Convergence */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-10">
        <NarrativeOverlapHeatmap timeRange={timeRange} />
        <SectorConvergenceBubbles timeRange={timeRange} />
      </div>

      {/* Contrarian Signals */}
      <div className="mb-10">
        <ContrarianSignalCards timeRange={timeRange} />
      </div>

      {/* Two-column row: Lifecycle Funnel + Lead Time Histogram */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-10">
        <LifecycleFunnel timeRange={timeRange} />
        <LeadTimeHistogram timeRange={timeRange} />
      </div>
    </div>
  );
}
