"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertTriangle, TrendingUp, ArrowRightLeft, FileStack, RefreshCw,
  Zap, Bell, BellOff, Trash2, Plus, X, Activity,
} from "lucide-react";
import {
  fetchSignals, fetchActivity, fetchWatchlist, fetchAlertRules,
  removeFromWatchlist, createAlertRule, deleteAlertRule,
  toggleAlertRule, markAllAlertsRead,
} from "@/lib/api";
import type { Signal, ActivityItem, WatchlistItem, AlertRule } from "@/lib/api";
import SegmentedControl from "@/components/common/SegmentedControl";
import StageBadge from "@/components/common/StageBadge";

const COORDINATION_TOOLTIP =
  "This signal shows patterns consistent with coordinated amplification.";

/** Only allow relative paths or http(s) links as safe hrefs */
function safeHref(link: string | undefined | null): string | undefined {
  if (!link) return undefined;
  if (link.startsWith("/")) return link;
  try {
    const url = new URL(link, window.location.origin);
    if (url.protocol === "http:" || url.protocol === "https:") return link;
  } catch { /* invalid URL */ }
  return undefined;
}

/** Relative time display */
function relTime(iso: string): string {
  if (!iso) return "—";
  try {
    const diff = Date.now() - new Date(iso).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "now";
    if (mins < 60) return `${mins}m ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  } catch { return "—"; }
}

/** Mutation type icon */
function MutationIcon({ subtype }: { subtype: string }) {
  const size = 14;
  switch (subtype) {
    case "score_spike": return <TrendingUp size={size} />;
    case "stage_change": return <ArrowRightLeft size={size} />;
    case "doc_surge": return <FileStack size={size} />;
    case "velocity_reversal": return <RefreshCw size={size} />;
    default: return <Zap size={size} />;
  }
}

/** Activity type color */
function activityColor(type: string, subtype: string): string {
  if (type === "alert") return "var(--vel-accelerating)";
  if (type === "mutation") {
    if (subtype === "score_spike" || subtype === "doc_surge") return "var(--vel-accelerating)";
    if (subtype === "velocity_reversal") return "var(--vel-decelerating)";
    return "var(--vel-stable)";
  }
  return "var(--text-disabled)";
}

/** Rule type display name */
const RULE_LABELS: Record<string, string> = {
  ns_above: "Score Above",
  ns_below: "Score Below",
  new_narrative: "New Narrative",
  mutation: "Any Mutation",
  stage_change: "Stage Change",
  catalyst: "Catalyst",
};

export default function SignalsPage() {
  // Legacy signals (kept for D4-I1 test compatibility)
  const [signals, setSignals] = useState<Signal[]>([]);
  // New data
  const [activity, setActivity] = useState<ActivityItem[]>([]);
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [alertRules, setAlertRules] = useState<AlertRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // UI state
  const [tab, setTab] = useState<"activity" | "signals">("activity");
  const [showAddRule, setShowAddRule] = useState(false);
  const [newRuleType, setNewRuleType] = useState("mutation");
  const [newRuleTarget, setNewRuleTarget] = useState("");
  const [newRuleThreshold, setNewRuleThreshold] = useState(0.5);

  useEffect(() => {
    setLoading(true);
    // Signals must load (test-critical); others are best-effort
    fetchSignals()
      .then(setSignals)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
    // Activity, watchlist, alerts — load independently, non-blocking
    fetchActivity().then(setActivity).catch(() => {});
    fetchWatchlist().then((wl) => setWatchlist(wl.items)).catch(() => {});
    fetchAlertRules().then(setAlertRules).catch(() => {});
  }, []);

  async function handleRemoveWatch(itemId: string) {
    try {
      await removeFromWatchlist(itemId);
      setWatchlist((prev) => prev.filter((i) => i.id !== itemId));
    } catch (err) {
      setError(`Failed to remove item: ${(err as Error).message}`);
    }
  }

  async function handleCreateRule() {
    if (!newRuleTarget.trim()) return;
    try {
      await createAlertRule(newRuleType, "narrative", newRuleTarget.trim(), newRuleThreshold);
      const rules = await fetchAlertRules();
      setAlertRules(rules);
      setShowAddRule(false);
      setNewRuleTarget("");
    } catch (err) {
      setError(`Failed to create rule: ${(err as Error).message}`);
    }
  }

  async function handleDeleteRule(ruleId: string) {
    try {
      await deleteAlertRule(ruleId);
      setAlertRules((prev) => prev.filter((r) => r.id !== ruleId));
    } catch (err) {
      setError(`Failed to delete rule: ${(err as Error).message}`);
    }
  }

  async function handleToggleRule(ruleId: string) {
    try {
      await toggleAlertRule(ruleId);
      setAlertRules((prev) => prev.map((r) =>
        r.id === ruleId ? { ...r, enabled: r.enabled ? 0 : 1 } : r
      ));
    } catch (err) {
      setError(`Failed to toggle rule: ${(err as Error).message}`);
    }
  }

  const unreadAlerts = activity.filter((a) => a.type === "alert" && !a.metadata?.is_read);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        {/* Page title row */}
        <div className="flex items-center justify-between">
          <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
            Inbox
          </h1>
          <SegmentedControl
            options={["Activity", "Evidence"]}
            activeOption={tab === "activity" ? "Activity" : "Evidence"}
            onChange={(opt) => setTab(opt === "Activity" ? "activity" : "signals")}
          />
        </div>
        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        <div className="grid grid-cols-1 xl:grid-cols-[1fr_320px] gap-8">

          {/* LEFT: Activity Feed + Signals */}
          <div>
            {/* Unread alert banner */}
            {unreadAlerts.length > 0 && (
              <div
                className="flex items-center justify-between px-4 py-2 mb-4 border-l-[3px]"
                style={{ background: "var(--vel-accelerating-bg)", borderColor: "var(--vel-accelerating)" }}
              >
                <span className="font-mono text-[12px] font-semibold" style={{ color: "var(--vel-accelerating)" }}>
                  {unreadAlerts.length} unread alert{unreadAlerts.length > 1 ? "s" : ""}
                </span>
                <button
                  onClick={async () => { try { await markAllAlertsRead(); const act = await fetchActivity(); setActivity(act); } catch (err) { setError(`Failed to mark read: ${(err as Error).message}`); } }}
                  className="font-mono text-[12px] font-medium transition-all hover:opacity-70 cursor-pointer bg-transparent border-none"
                  style={{ color: "var(--vel-accelerating)" }}
                >
                  Mark all read
                </button>
              </div>
            )}

            {loading && <div className="font-mono text-[12px] text-text-muted py-16 text-center">Loading...</div>}
            {error && <div className="font-mono text-[12px] text-bearish py-16 text-center">Failed to load: {error}</div>}

            {/* ACTIVITY TAB */}
            {!loading && (
            <div style={{ display: tab === "activity" ? undefined : "none" }}>
              <div className="flex flex-col">
                {activity.length === 0 && (
                  <p className="font-mono text-[12px] text-text-muted py-8 text-center">No activity yet. Run the pipeline to generate mutations.</p>
                )}
                {activity.map((item, i) => (
                  <a
                    key={`${item.type}-${item.timestamp}-${i}`}
                    href={safeHref(item.link)}
                    className="flex items-start gap-3 px-4 py-3 transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)] no-underline"
                    style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
                  >
                    <div className="shrink-0 mt-0.5" style={{ color: activityColor(item.type, item.subtype) }}>
                      {item.type === "mutation" ? <MutationIcon subtype={item.subtype} /> :
                       item.type === "alert" ? <Bell size={14} /> :
                       <Activity size={14} />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="text-text-primary font-medium truncate text-[13px]">
                          {item.title}
                        </span>
                        <span className="font-mono text-[10px] uppercase text-text-disabled shrink-0">
                          {relTime(item.timestamp)}
                        </span>
                      </div>
                      {item.message && (
                        <p className="text-text-muted text-[12px] line-clamp-2 mt-0.5">
                          {item.message}
                        </p>
                      )}
                    </div>
                  </a>
                ))}
              </div>
            </div>
            )}

            {/* SIGNALS TAB (legacy — preserves D4-I1 test compatibility) */}
            {!loading && (
            <div style={{ display: tab === "signals" ? undefined : "none" }}>
              <div className="flex flex-col">
                {signals.length === 0 && (
                  <p className="font-mono text-[12px] text-text-muted py-8 text-center">No signals available.</p>
                )}
                {signals.map((sig) => (
                  <article
                    key={sig.id}
                    data-testid={`signal-${sig.id}`}
                    className="py-3 px-4 flex flex-col gap-2 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
                    style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
                    aria-label={`Signal: ${sig.headline}`}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <p className="text-text-primary text-[13px] leading-snug line-clamp-2 flex-1">
                        {sig.headline || "(no headline)"}
                      </p>
                      {sig.coordination_flag && (
                        <>
                          <span
                            data-testid="coordination-flag"
                            title={COORDINATION_TOOLTIP}
                            className="shrink-0 flex items-center gap-1 bg-alert-bg text-alert border border-alert/30 text-[11px] font-medium px-2 py-0.5 rounded-sm cursor-help"
                            aria-label={`Coordination Flag: ${COORDINATION_TOOLTIP}`}
                          >
                            <AlertTriangle size={10} />
                            Coordination Flag
                          </span>
                          <a
                            href="/manipulation"
                            className="text-[11px] text-alert underline ml-1"
                            data-testid={`view-campaign-${sig.id}`}
                          >
                            View campaign →
                          </a>
                        </>
                      )}
                    </div>
                    <div className="flex items-center gap-3 font-mono text-[11px] text-text-muted">
                      <span className="font-medium text-text-secondary">{sig.source.name}</span>
                      <span>·</span>
                      <span>{sig.timestamp ? new Date(sig.timestamp).toLocaleDateString() : "—"}</span>
                      <span>·</span>
                      <span>sentiment {sig.sentiment.toFixed(2)}</span>
                    </div>
                  </article>
                ))}
              </div>
            </div>
            )}
          </div>

          {/* RIGHT: Watchlist + Alert Rules */}
          <div>
            {/* Watchlist */}
            <div className="mb-10">
              <div className="flex items-baseline gap-3 mb-4">
                <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                  Watchlist
                </h2>
                <span className="font-mono text-[11px] text-text-muted">{watchlist.length} items</span>
              </div>
              {watchlist.length === 0 && (
                <p className="font-mono text-[12px] text-text-muted">
                  No items watched. Add narratives or tickers from the dashboard.
                </p>
              )}
              <div className="flex flex-col">
                {watchlist.map((item) => (
                  <div
                    key={item.id}
                    className="flex items-center gap-2 px-3 py-2 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
                    style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
                  >
                    <span
                      className="font-mono text-[10px] uppercase shrink-0"
                      style={{ color: item.item_type === "narrative" ? "var(--vel-accelerating)" : "var(--vel-stable)", width: 12 }}
                    >
                      {item.item_type === "narrative" ? "N" : "$"}
                    </span>
                    <Link
                      href={item.item_type === "narrative" ? `/narrative/${item.item_id}` : `/brief/${item.item_id}`}
                      className="flex-1 min-w-0"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <span className="text-text-primary truncate block hover:text-accent-text transition-colors text-[12px] font-display">
                        {item.name || item.item_id}
                      </span>
                      <span className="font-mono text-[10px] text-text-muted">
                        {item.item_type === "narrative"
                          ? <>
                              {item.stage && <StageBadge stage={item.stage} className="mr-1 text-[9px]" />}
                              vel {((item.velocity || 0) * 100).toFixed(1)}%
                            </>
                          : item.current_price ? `$${item.current_price.toLocaleString()}` : "—"
                        }
                      </span>
                    </Link>
                    <button
                      onClick={() => handleRemoveWatch(item.id)}
                      className="shrink-0 transition-all hover:text-bearish cursor-pointer bg-transparent border-none"
                      style={{ color: "var(--text-disabled)" }}
                      aria-label="Remove from watchlist"
                    >
                      <X size={12} />
                    </button>
                  </div>
                ))}
              </div>
            </div>

            {/* Alert Rules */}
            <div>
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-[13px] font-semibold uppercase tracking-[0.06em] text-text-secondary">
                  Alert Rules
                </h2>
                <button
                  onClick={() => setShowAddRule(!showAddRule)}
                  className="flex items-center gap-1 font-mono text-[11px] text-text-muted hover:text-text-primary transition-all cursor-pointer bg-transparent border-none"
                >
                  <Plus size={12} /> Add
                </button>
              </div>

              {/* Add rule form */}
              {showAddRule && (
                <div className="mb-4 flex flex-col gap-2">
                  <select
                    value={newRuleType}
                    onChange={(e) => setNewRuleType(e.target.value)}
                    className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
                  >
                    {Object.entries(RULE_LABELS).map(([k, v]) => (
                      <option key={k} value={k}>{v}</option>
                    ))}
                  </select>
                  <input
                    placeholder="Narrative ID or ticker"
                    value={newRuleTarget}
                    onChange={(e) => setNewRuleTarget(e.target.value)}
                    className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1"
                  />
                  {(newRuleType === "ns_above" || newRuleType === "ns_below") && (
                    <input
                      type="number"
                      step="0.1"
                      placeholder="Threshold"
                      value={newRuleThreshold}
                      onChange={(e) => setNewRuleThreshold(Number(e.target.value))}
                      className="font-mono text-[11px] bg-transparent border border-[var(--bg-border)] text-text-secondary rounded-sm px-2 py-1 w-20"
                    />
                  )}
                  <button
                    onClick={handleCreateRule}
                    className="font-display text-[13px] font-medium bg-accent-primary text-text-primary rounded-sm px-4 py-2 hover:brightness-110 transition-all cursor-pointer border-none"
                  >
                    Create Rule
                  </button>
                </div>
              )}

              {alertRules.length === 0 && !showAddRule && (
                <p className="font-mono text-[12px] text-text-muted">
                  No alert rules. Add one to get notified when narratives change.
                </p>
              )}
              <div className="flex flex-col">
                {alertRules.map((rule) => (
                  <div
                    key={rule.id}
                    className="flex items-center gap-2 px-3 py-2"
                    style={{ borderBottom: "1px solid var(--border-subtle-soft)", opacity: rule.enabled ? 1 : 0.5 }}
                  >
                    <div className="flex-1 min-w-0">
                      <span className="text-text-primary block text-[12px]">
                        {RULE_LABELS[rule.rule_type] || rule.rule_type}
                      </span>
                      <span className="font-mono text-[10px] text-text-muted">
                        {rule.target_id?.slice(0, 12) || "any"}
                        {rule.threshold ? ` · ${rule.threshold}` : ""}
                      </span>
                    </div>
                    <button
                      onClick={() => handleToggleRule(rule.id)}
                      className="shrink-0 transition-all cursor-pointer bg-transparent border-none"
                      style={{ color: rule.enabled ? "var(--vel-accelerating)" : "var(--text-disabled)" }}
                      aria-label={rule.enabled ? "Disable rule" : "Enable rule"}
                    >
                      {rule.enabled ? <Bell size={12} /> : <BellOff size={12} />}
                    </button>
                    <button
                      onClick={() => handleDeleteRule(rule.id)}
                      className="shrink-0 transition-all hover:text-bearish cursor-pointer bg-transparent border-none"
                      style={{ color: "var(--text-disabled)" }}
                      aria-label="Delete rule"
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="font-mono text-[10px] text-text-muted">
            INTELLIGENCE ONLY — NOT FINANCIAL ADVICE
          </div>
        </div>
      </div>
    </main>
  );
}

