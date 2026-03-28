"use client";

import { useEffect, useState } from "react";
import { LogIn, LogOut, ChevronDown, ChevronUp } from "lucide-react";
import NarrativeCard from "@/components/NarrativeCard";
import InvestigateDrawer from "@/components/InvestigateDrawer";
import SegmentedControl from "@/components/common/SegmentedControl";
import MetricTooltip from "@/components/common/MetricTooltip";
import { fetchNarratives, fetchTicker, type VisibleNarrative } from "@/lib/api";
import { useAuth } from "@/contexts/AuthContext";
import { useRealtimeData } from "@/hooks/useRealtimeData";
import type { Narrative, TickerItem } from "@/lib/api";

/** Relative time for live sync display */
function syncLabel(narratives: VisibleNarrative[]): string {
  const latest = narratives
    .map((n) => n.last_evidence_at || "")
    .filter(Boolean)
    .sort()
    .pop();
  if (!latest) return "—";
  const diff = Date.now() - new Date(latest).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "JUST NOW";
  if (mins < 60) return `${mins}M AGO`;
  return `${Math.floor(mins / 60)}H AGO`;
}

export default function GatewayPage() {
  const { isSignedIn, signIn, signOut } = useAuth();
  const [narratives, setNarratives] = useState<Narrative[]>([]);
  const [tickerItems, setTickerItems] = useState<TickerItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [drawerNarrativeId, setDrawerNarrativeId] = useState<string | null>(null);
  const [topicFilter, setTopicFilter] = useState<string>("all");
  const [minVelocity, setMinVelocity] = useState<number>(0);
  const [showRemaining, setShowRemaining] = useState(false);
  const [sortBy, setSortBy] = useState<string>("Strongest");
  const [stageFilter, setStageFilter] = useState<string>("All");

  useEffect(() => {
    let cancelled = false;

    async function load(attempt = 1) {
      try {
        setLoading(true);
        setError(null);
        const [narData, tickData] = await Promise.all([
          fetchNarratives(),
          fetchTicker(),
        ]);
        if (cancelled) return;
        setNarratives(narData);
        setTickerItems(tickData);
        setLoading(false);
      } catch (err) {
        if (cancelled) return;
        if (attempt < 3) {
          setTimeout(() => { if (!cancelled) load(attempt + 1); }, 1500);
        } else {
          setError((err as Error).message);
          setLoading(false);
        }
      }
    }

    load();
    return () => { cancelled = true; };
  }, []);

  const { data: realtimeTickerData } = useRealtimeData<TickerItem[]>({ endpoint: "/api/ticker", interval: 10000 });
  const currentTickerItems = realtimeTickerData ?? tickerItems;

  // All narratives are visible (monetization removed in D4)
  const visibleNarratives = narratives
    .filter((n): n is VisibleNarrative => !n.blurred)
    .filter((n) =>
      topicFilter === "all" ? true : (n.topic_tags || []).includes(topicFilter)
    )
    .filter((n) => {
      if (minVelocity <= 0) return true;
      const match = n.velocity_summary.match(/([+-]?\d+\.?\d*)/);
      return match ? Math.abs(parseFloat(match[1])) >= minVelocity : true;
    });

  // Computed metrics
  const allVisible = narratives.filter((n): n is VisibleNarrative => !n.blurred);
  const surgeCount = allVisible.filter((n) => n.burst_velocity?.is_burst).length;
  const avgDiversity = allVisible.length > 0
    ? allVisible.reduce((sum, n) => sum + (n.entropy || 0), 0) / allVisible.length
    : 0;
  const emergingCount = allVisible.filter((n) => n.stage === "Emerging").length;

  // Parse velocity number from summary string
  function parseVelocity(n: VisibleNarrative): number {
    const match = n.velocity_summary.match(/([+-]?\d+\.?\d*)/);
    return match ? parseFloat(match[1]) : 0;
  }

  // Apply stage filter
  const stageFiltered = stageFilter === "All"
    ? visibleNarratives
    : visibleNarratives.filter((n) => n.stage === stageFilter);

  // Apply sort
  const sorted = (() => {
    const arr = [...stageFiltered];
    switch (sortBy) {
      case "Fastest":
        return arr.sort((a, b) => parseVelocity(b) - parseVelocity(a));
      case "Newest":
        return arr.sort((a, b) => {
          const ta = a.last_evidence_at || "";
          const tb = b.last_evidence_at || "";
          return tb.localeCompare(ta);
        });
      case "SURGE":
        return arr.sort((a, b) => {
          const aBurst = a.burst_velocity?.is_burst ? 1 : 0;
          const bBurst = b.burst_velocity?.is_burst ? 1 : 0;
          if (aBurst !== bBurst) return bBurst - aBurst;
          return parseVelocity(b) - parseVelocity(a);
        });
      default: // Strongest — ns_score (saturation) desc
        return arr.sort((a, b) => b.saturation - a.saturation);
    }
  })();

  const heroNarrative = sorted[0] ?? null;
  const secondaryNarratives = sorted.slice(1, 4);
  const compactNarratives = sorted.slice(4, 12);
  const remainingNarratives = sorted.slice(12);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">

        {/* Page title row */}
        <div className="flex items-center justify-between flex-wrap gap-4">
          <div className="flex items-center gap-4">
            <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
              Narrative Intelligence
            </h1>
            {!loading && allVisible.length > 0 && (
              <div className="flex items-center gap-1.5">
                <div
                  className="animate-pulse-accent"
                  style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--intent-primary)" }}
                />
                <span className="font-mono text-[10px] text-text-disabled tracking-wide">
                  LIVE · {syncLabel(allVisible)}
                </span>
              </div>
            )}
          </div>

          <div className="flex items-center gap-3">
            {isSignedIn ? (
              <button
                onClick={signOut}
                className="flex items-center gap-1.5 text-text-muted hover:text-text-primary transition-all text-[10px]"
                aria-label="Sign out"
              >
                <LogOut size={12} /> Sign out
              </button>
            ) : (
              <button
                onClick={signIn}
                className="flex items-center gap-1.5 text-text-primary bg-accent-primary px-2.5 py-1 rounded-sm text-[10px] transition-all"
                aria-label="Sign in"
              >
                <LogIn size={12} /> Sign in
              </button>
            )}
          </div>
        </div>

        {/* Title rule */}
        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {/* Summary metrics + controls row */}
        {!loading && allVisible.length > 0 && (
          <div className="flex items-start justify-between flex-wrap gap-4 mb-10">
            {/* Metrics */}
            <div className="flex items-center gap-5" data-testid="summary-metrics">
              <MetricTooltip metricKey="ns_score">
                <div>
                  <div className="font-data-large text-text-primary">{allVisible.length}</div>
                  <div className="label-micro">active</div>
                </div>
              </MetricTooltip>
              <div className="w-px h-6 bg-[var(--bg-border)]" />
              <MetricTooltip metricKey="burst_ratio">
                <div>
                  <div className="font-data-large text-text-primary">{surgeCount}</div>
                  <div className="label-micro">surges</div>
                </div>
              </MetricTooltip>
              <div className="w-px h-6 bg-[var(--bg-border)]" />
              <MetricTooltip metricKey="entropy">
                <div>
                  <div className="font-data-large text-text-primary">{avgDiversity.toFixed(1)}</div>
                  <div className="label-micro">diversity</div>
                </div>
              </MetricTooltip>
              <div className="w-px h-6 bg-[var(--bg-border)]" />
              <div>
                <div className="font-data-large text-text-primary">{emergingCount}</div>
                <div className="label-micro">emerging</div>
              </div>
            </div>

            {/* Sort + filter controls */}
            <div className="flex items-center gap-4 flex-wrap">
              <SegmentedControl
                options={["Strongest", "Fastest", "Newest", "SURGE"]}
                activeOption={sortBy}
                onChange={setSortBy}
              />
              <SegmentedControl
                options={["All", "Emerging", "Growing", "Mature", "Declining", "Dormant"]}
                activeOption={stageFilter}
                onChange={setStageFilter}
              />
              <select
                data-testid="topic-filter"
                value={topicFilter}
                onChange={(e) => setTopicFilter(e.target.value)}
                className="font-mono text-[11px] text-text-muted bg-transparent border border-[var(--bg-border)] rounded-sm px-2 py-1"
              >
                <option value="all">All Topics</option>
                <option value="regulatory">Regulatory</option>
                <option value="earnings">Earnings</option>
                <option value="geopolitical">Geopolitical</option>
                <option value="macro">Macro</option>
                <option value="esg">ESG</option>
                <option value="m&a">M&A</option>
                <option value="crypto">Crypto</option>
              </select>
              <div className="flex items-center gap-1">
                <input
                  type="number"
                  step="1"
                  min="0"
                  value={minVelocity}
                  onChange={(e) => {
                    const v = Number(e.target.value);
                    setMinVelocity(Number.isFinite(v) && v >= 0 ? v : 0);
                  }}
                  className="font-mono text-[11px] text-text-muted bg-transparent border border-[var(--bg-border)] rounded-sm px-1.5 py-1 w-10"
                  aria-label="Minimum velocity filter"
                />
                <span className="text-[9px] text-text-disabled">%</span>
              </div>
            </div>
          </div>
        )}

        {loading && (
          <div className="font-mono text-[12px] text-text-muted py-8 text-center">Loading narratives...</div>
        )}
        {error && (
          <div className="font-mono text-[12px] text-bearish py-8 text-center">Failed to load: {error}</div>
        )}

        {/* Empty state when filters exclude everything */}
        {!loading && !error && sorted.length === 0 && allVisible.length > 0 && (
          <div className="font-mono text-[12px] text-text-muted py-8 text-center">
            No narratives match the current filters.
          </div>
        )}

        {/* Hero + Secondary panels */}
        {!loading && heroNarrative && (
          <section aria-label="Active narratives">
            <div className="grid grid-cols-1 xl:grid-cols-[1fr_380px] gap-4 mb-10">
              <NarrativeCard
                narrative={heroNarrative}
                variant="hero"
                onInvestigateClick={(id) => setDrawerNarrativeId(id)}
              />
              {secondaryNarratives.length > 0 && (
                <div className="flex flex-col gap-3">
                  {secondaryNarratives.map((n, i) => (
                    <NarrativeCard
                      key={n.id}
                      narrative={n}
                      variant="secondary"
                      showSummary={i === 0}
                      onInvestigateClick={(id) => setDrawerNarrativeId(id)}
                    />
                  ))}
                </div>
              )}
            </div>

            {/* Compact signal table */}
            {compactNarratives.length > 0 && (
              <>
                {/* Section header */}
                <div className="flex items-baseline gap-3 mb-4">
                  <span className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                    All Signals
                  </span>
                  <span className="font-mono text-[11px] text-text-muted">
                    {stageFiltered.length} active
                  </span>
                </div>

                {/* Column headers */}
                <div
                  className="flex items-center gap-3 px-4 font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted"
                  style={{ height: 28, borderBottom: "1px solid rgba(56, 62, 71, 0.2)" }}
                >
                  <span style={{ width: 14 }} />
                  <span style={{ width: 72, textAlign: "right" }}>VELOCITY</span>
                  <span style={{ width: 28 }}>TREND</span>
                  <span className="flex-1">SIGNAL</span>
                  <span style={{ width: 56, textAlign: "center" }}>STAGE</span>
                  <span className="hidden xl:inline" style={{ width: 80 }}>TOPICS</span>
                  <span style={{ width: 36, textAlign: "right" }}>DOCS</span>
                  <span style={{ width: 32, textAlign: "right" }}>UPD</span>
                </div>
                {compactNarratives.map((n) => (
                  <NarrativeCard
                    key={n.id}
                    narrative={n}
                    variant="compact"
                    onInvestigateClick={(id) => setDrawerNarrativeId(id)}
                  />
                ))}
              </>
            )}

            {/* Remaining (collapsible) */}
            {remainingNarratives.length > 0 && (
              <div className="mt-4">
                <button
                  onClick={() => setShowRemaining(!showRemaining)}
                  className="flex items-center gap-1.5 font-mono text-[12px] text-text-muted hover:text-text-primary transition-all cursor-pointer bg-transparent border-none mb-2"
                >
                  {showRemaining ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                  {showRemaining ? "Hide" : `Show ${remainingNarratives.length} more signals`}
                </button>
                {showRemaining && (
                  <div>
                    {remainingNarratives.map((n) => (
                      <NarrativeCard
                        key={n.id}
                        narrative={n}
                        variant="compact"
                        onInvestigateClick={(id) => setDrawerNarrativeId(id)}
                      />
                    ))}
                  </div>
                )}
              </div>
            )}
          </section>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            <span>Last sync: {!loading && allVisible.length > 0 ? syncLabel(allVisible) : "—"}</span>
          </div>
        </div>
      </div>

      <InvestigateDrawer
        narrativeId={drawerNarrativeId}
        onClose={() => setDrawerNarrativeId(null)}
      />
    </main>
  );
}
