"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertTriangle,
  TrendingUp,
  ArrowRightLeft,
  FileStack,
  RefreshCw,
  Zap,
  Bell,
  Activity,
} from "lucide-react";
import { fetchSignals, fetchActivity } from "@/lib/api";
import type { Signal, ActivityItem } from "@/lib/api";
import SegmentedControl from "@/components/common/SegmentedControl";

const COORDINATION_TOOLTIP =
  "This signal shows patterns consistent with coordinated amplification.";

function safeHref(link: string | undefined | null): string | undefined {
  if (!link) return undefined;
  if (link.startsWith("/")) return link;
  try {
    const url = new URL(link, window.location.origin);
    if (url.protocol === "http:" || url.protocol === "https:") return link;
  } catch {
    /* invalid URL */
  }
  return undefined;
}

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
  } catch {
    return "—";
  }
}

function MutationIcon({ subtype }: { subtype: string }) {
  const size = 14;
  switch (subtype) {
    case "score_spike":
      return <TrendingUp size={size} />;
    case "stage_change":
      return <ArrowRightLeft size={size} />;
    case "doc_surge":
      return <FileStack size={size} />;
    case "velocity_reversal":
      return <RefreshCw size={size} />;
    default:
      return <Zap size={size} />;
  }
}

function activityColor(type: string, subtype: string): string {
  if (type === "alert") return "var(--vel-accelerating)";
  if (type === "mutation") {
    if (subtype === "score_spike" || subtype === "doc_surge") return "var(--vel-accelerating)";
    if (subtype === "velocity_reversal") return "var(--vel-decelerating)";
    return "var(--vel-stable)";
  }
  return "var(--text-disabled)";
}

export default function SignalsPage() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [activity, setActivity] = useState<ActivityItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<"activity" | "signals">("activity");

  useEffect(() => {
    setLoading(true);
    fetchSignals()
      .then(setSignals)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
    fetchActivity().then(setActivity).catch(() => {});
  }, []);

  const unreadAlerts = activity.filter((a) => a.type === "alert" && !a.metadata?.is_read);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
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

        {unreadAlerts.length > 0 && (
          <div
            className="flex items-center justify-between px-4 py-2 mb-4 border-l-[3px]"
            style={{ background: "var(--vel-accelerating-bg)", borderColor: "var(--vel-accelerating)" }}
          >
            <span className="font-mono text-[12px] font-semibold" style={{ color: "var(--vel-accelerating)" }}>
              {unreadAlerts.length} unread alert{unreadAlerts.length > 1 ? "s" : ""}
            </span>
          </div>
        )}

        {loading && <div className="font-mono text-[12px] text-text-muted py-16 text-center">Loading...</div>}
        {error && <div className="font-mono text-[12px] text-bearish py-16 text-center">Failed to load: {error}</div>}

        {!loading && (
          <div className="grid grid-cols-1">
            <div style={{ display: tab === "activity" ? undefined : "none" }}>
              <div className="flex flex-col">
                {activity.length === 0 ? (
                  <p className="font-mono text-[12px] text-text-muted py-8 text-center">
                    No activity yet. Run the pipeline to generate mutations.
                  </p>
                ) : (
                  activity.map((item, i) => (
                    <a
                      key={`${item.type}-${item.timestamp}-${i}`}
                      href={safeHref(item.link)}
                      className="flex items-start gap-3 px-4 py-3 transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)] no-underline"
                      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                    >
                      <div className="shrink-0 mt-0.5" style={{ color: activityColor(item.type, item.subtype) }}>
                        {item.type === "mutation" ? (
                          <MutationIcon subtype={item.subtype} />
                        ) : item.type === "alert" ? (
                          <Bell size={14} />
                        ) : (
                          <Activity size={14} />
                        )}
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
                  ))
                )}
              </div>
            </div>

            <div style={{ display: tab === "signals" ? undefined : "none" }}>
              <div className="flex flex-col">
                {signals.length === 0 ? (
                  <p className="font-mono text-[12px] text-text-muted py-8 text-center">No signals available.</p>
                ) : (
                  signals.map((sig) => (
                    <article
                      key={sig.id}
                      data-testid={`signal-${sig.id}`}
                      className="py-3 px-4 flex flex-col gap-2 hover:bg-[var(--accent-primary-hover)] transition-colors duration-[120ms]"
                      style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
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
                  ))
                )}
              </div>
            </div>
          </div>
        )}

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
