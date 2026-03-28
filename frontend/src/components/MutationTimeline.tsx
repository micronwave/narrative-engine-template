"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import type { Mutation } from "@/lib/api";

type Props = {
  mutations: Mutation[];
};

function formatRelativeTime(timestamp: string): string {
  if (!timestamp) return "Unknown date";
  try {
    const d = new Date(timestamp);
    const now = Date.now();
    const diff = now - d.getTime();
    const days = Math.floor(diff / 86400000);
    if (days === 0) return "Today";
    if (days === 1) return "Yesterday";
    if (days < 30) return `${days} days ago`;
    if (days < 365) return `${Math.floor(days / 30)} months ago`;
    return `${Math.floor(days / 365)} years ago`;
  } catch {
    return timestamp;
  }
}

function formatFullDate(timestamp: string): string {
  if (!timestamp) return "";
  try {
    return new Date(timestamp).toLocaleDateString(undefined, {
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  } catch {
    return timestamp;
  }
}

const COLLAPSED_COUNT = 2;

/**
 * Collapsible mutation timeline.
 *
 * Default state: shows the 2 most recent mutations.
 * Expand/Collapse toggle reveals or hides older mutations.
 */
export default function MutationTimeline({ mutations }: Props) {
  const [expanded, setExpanded] = useState(false);

  if (mutations.length === 0) {
    return (
      <p className="text-text-disabled text-sm" data-testid="mutation-timeline-empty">
        No mutations recorded.
      </p>
    );
  }

  // Sort newest first
  const sorted = [...mutations].sort((a, b) =>
    (b.timestamp || "").localeCompare(a.timestamp || "")
  );

  const visible = expanded ? sorted : sorted.slice(0, COLLAPSED_COUNT);
  const hasMore = sorted.length > COLLAPSED_COUNT;

  return (
    <div data-testid="mutation-timeline">
      <ol className="relative border-l border-border-default flex flex-col gap-0">
        {visible.map((mut, i) => (
          <li
            key={mut.id}
            data-testid={`mutation-entry-${i}`}
            className="pl-5 pb-5 relative"
          >
            {/* Timeline dot */}
            <span className="absolute left-[-5px] top-1 w-2.5 h-2.5 rounded-sm bg-accent-primary border-2 border-base" />

            {/* from → to states */}
            <div className="flex items-center gap-2 text-xs font-mono mb-1">
              <span
                className="text-text-tertiary"
                data-testid={`mut-from-${i}`}
              >
                {mut.from_state || "—"}
              </span>
              <span className="text-text-disabled">→</span>
              <span
                className="text-text-primary font-semibold"
                data-testid={`mut-to-${i}`}
              >
                {mut.to_state || "—"}
              </span>
            </div>

            {/* Description */}
            {mut.description && (
              <p
                className="text-text-secondary text-xs leading-relaxed mb-1"
                data-testid={`mut-description-${i}`}
              >
                {mut.description}
              </p>
            )}

            {/* Trigger (linked catalyst reference) */}
            {mut.trigger && (
              <p
                className="text-text-disabled text-xs mt-0.5"
                data-testid={`mut-trigger-${i}`}
              >
                Triggered by:{" "}
                <span className="text-alert font-mono-data">{mut.trigger}</span>
              </p>
            )}

            {/* Timestamp */}
            {mut.timestamp && (
              <time
                dateTime={mut.timestamp}
                title={formatFullDate(mut.timestamp)}
                className="text-text-disabled text-xs font-mono cursor-help"
                data-testid={`mut-timestamp-${i}`}
              >
                {formatRelativeTime(mut.timestamp)}
              </time>
            )}
          </li>
        ))}
      </ol>

      {hasMore && (
        <button
          onClick={() => setExpanded((e) => !e)}
          className="flex items-center gap-1.5 text-xs text-accent-text hover:text-accent-hover transition-colors mt-1"
          aria-expanded={expanded}
          aria-label={expanded ? "Collapse mutation timeline" : "Expand mutation timeline"}
          data-testid="mutation-timeline-toggle"
        >
          {expanded ? (
            <>
              <ChevronUp size={13} /> Collapse
            </>
          ) : (
            <>
              <ChevronDown size={13} /> Expand ({sorted.length - COLLAPSED_COUNT} more)
            </>
          )}
        </button>
      )}
    </div>
  );
}
