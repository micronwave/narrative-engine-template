"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, AlertTriangle } from "lucide-react";
import type { Mutation, Signal } from "@/lib/api";

type Props = {
  mutations: Mutation[];
  signals?: Signal[];
};

function formatRelativeTime(timestamp: string): string {
  if (!timestamp) return "Unknown";
  try {
    const d = new Date(timestamp);
    const diffMs = Date.now() - d.getTime();
    const hours = Math.floor(diffMs / 3600000);
    if (hours < 1) return "< 1h ago";
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    if (days === 1) return "1d ago";
    if (days < 30) return `${days}d ago`;
    return `${Math.floor(days / 30)}mo ago`;
  } catch {
    return timestamp;
  }
}

function parseDelta(
  from: string,
  to: string
): { value: number; display: string } | null {
  const fromNum = parseFloat(from);
  const toNum = parseFloat(to);
  if (isNaN(fromNum) || isNaN(toNum)) return null;
  const delta = toNum - fromNum;
  const sign = delta >= 0 ? "+" : "";
  const decimals = delta % 1 === 0 ? 0 : 2;
  return { value: delta, display: `${sign}${delta.toFixed(decimals)}` };
}

function getContributingSources(
  mutation: Mutation,
  signals: Signal[]
): Signal[] {
  if (!mutation.timestamp || !signals.length) return [];
  const mutTime = new Date(mutation.timestamp).getTime();
  const windowMs = 4 * 3600000;
  return signals
    .filter((s) => {
      if (!s.timestamp) return false;
      return Math.abs(new Date(s.timestamp).getTime() - mutTime) <= windowMs;
    })
    .slice(0, 3);
}

const COLLAPSED_COUNT = 3;

export default function NarrativeChangelog({ mutations, signals = [] }: Props) {
  const [expanded, setExpanded] = useState(false);

  if (mutations.length === 0) {
    return (
      <p
        className="font-mono text-[12px] text-text-muted"
        data-testid="changelog-empty"
      >
        No changes recorded.
      </p>
    );
  }

  const sorted = [...mutations].sort((a, b) =>
    (b.timestamp || "").localeCompare(a.timestamp || "")
  );

  const visible = expanded ? sorted : sorted.slice(0, COLLAPSED_COUNT);
  const hasMore = sorted.length > COLLAPSED_COUNT;

  return (
    <div data-testid="narrative-changelog">
      <ol className="relative border-l flex flex-col gap-0" style={{ borderColor: "var(--bg-border)" }}>
        {visible.map((mut, i) => {
          const delta = parseDelta(mut.from_state, mut.to_state);
          const contributing = getContributingSources(mut, signals);

          return (
            <li
              key={mut.id}
              data-testid={`changelog-entry-${i}`}
              className="pl-5 pb-5 relative"
            >
              {/* Timeline dot */}
              <span
                className="absolute left-[-5px] top-1 w-2.5 h-2.5 rounded-sm"
                style={{
                  backgroundColor: "var(--accent-primary)",
                  border: "2px solid var(--bg-base)",
                }}
              />

              {/* Header: mutation type + timestamp */}
              <div className="flex items-center justify-between mb-1">
                <span
                  className="font-mono text-[11px] font-semibold text-text-secondary uppercase tracking-[0.04em]"
                  data-testid={`changelog-type-${i}`}
                >
                  {mut.mutation_type || "Change"}
                </span>
                {mut.timestamp && (
                  <time
                    dateTime={mut.timestamp}
                    className="font-mono text-[10px] text-text-muted"
                    title={new Date(mut.timestamp).toLocaleString()}
                    data-testid={`changelog-time-${i}`}
                  >
                    {formatRelativeTime(mut.timestamp)}
                  </time>
                )}
              </div>

              {/* Delta display */}
              <div className="flex items-center gap-2 text-xs font-mono mb-1">
                <span className="text-text-tertiary">
                  {mut.from_state || "\u2014"}
                </span>
                <span className="text-text-disabled">{"\u2192"}</span>
                <span className="text-text-primary font-semibold">
                  {mut.to_state || "\u2014"}
                </span>
                {delta && (
                  <span
                    className="font-mono font-semibold text-[12px]"
                    style={{
                      color:
                        delta.value > 0
                          ? "var(--intent-success)"
                          : delta.value < 0
                            ? "var(--intent-danger)"
                            : "var(--text-muted)",
                    }}
                    data-testid={`changelog-delta-${i}`}
                  >
                    {delta.display}
                  </span>
                )}
              </div>

              {/* Description */}
              {mut.description && (
                <p
                  className="text-text-secondary text-xs leading-relaxed mb-1"
                  data-testid={`changelog-desc-${i}`}
                >
                  {mut.description}
                </p>
              )}

              {/* Contributing sources */}
              {contributing.length > 0 && (
                <div className="mt-2 pl-3 border-l" style={{ borderColor: "var(--bg-border)" }}>
                  <div className="font-mono text-[10px] text-text-muted uppercase mb-1">
                    Recent sources
                  </div>
                  {contributing.map((sig) => (
                    <div
                      key={sig.id}
                      className="text-[11px] text-text-tertiary py-0.5 line-clamp-1"
                    >
                      {sig.headline}{" "}
                      <span className="text-text-disabled">
                        &mdash; {sig.source.name}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </li>
          );
        })}
      </ol>

      {/* Caveat */}
      <div className="flex items-center gap-1.5 mt-2 mb-2">
        <AlertTriangle size={11} className="text-text-muted shrink-0" />
        <span className="font-mono text-[10px] text-text-muted">
          Source correlations are temporal, not causal
        </span>
      </div>

      {hasMore && (
        <button
          onClick={() => setExpanded((e) => !e)}
          className="flex items-center gap-1.5 text-xs text-accent-text hover:text-text-primary transition-colors"
          aria-expanded={expanded}
          data-testid="changelog-toggle"
        >
          {expanded ? (
            <>
              <ChevronUp size={13} /> Collapse
            </>
          ) : (
            <>
              <ChevronDown size={13} /> Show all (
              {sorted.length - COLLAPSED_COUNT} more)
            </>
          )}
        </button>
      )}
    </div>
  );
}
