"use client";

import { useEffect, useRef, useState } from "react";
import { X } from "lucide-react";
import type { NarrativeDetail } from "@/lib/api";
import VelocitySparkline from "./VelocitySparkline";
import { useNarrativeInvestigation } from "@/hooks/useNarrativeInvestigation";

type Props = {
  narrativeId: string | null;
  onClose: () => void;
};

/**
 * Slide-in investigation drawer.
 *
 * On open (narrativeId becomes non-null):
 *  Fetches GET /api/narratives/{id} and renders detail for signed-in users.
 *
 * Accessibility: aria-modal, role=dialog, Escape to close, focus trap on Tab,
 * backdrop click to close.
 */
export default function InvestigateDrawer({ narrativeId, onClose }: Props) {
  const drawerRef = useRef<HTMLDivElement>(null);

  const { data, loading, error: fetchError } = useNarrativeInvestigation(narrativeId);
  const [sourceRows, setSourceRows] = useState<Array<NarrativeDetail["signals"][number]["source"]>>([]);

  useEffect(() => {
    if (!data) {
      setSourceRows([]);
      return;
    }
    setSourceRows(Array.from(new Map(data.signals.map((s) => [s.source.id, s.source])).values()));
  }, [data]);

  // Focus trap + Escape key
  useEffect(() => {
    if (!narrativeId) return;

    function trapFocus(e: KeyboardEvent) {
      const focusable = drawerRef.current?.querySelectorAll<HTMLElement>(
        'button, a, [tabindex]:not([tabindex="-1"])'
      );
      if (!focusable || focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];

      if (e.key === "Tab") {
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
      if (e.key === "Escape") onClose();
    }

    document.addEventListener("keydown", trapFocus);

    // Auto-focus the close button on open
    const timer = setTimeout(() => {
      drawerRef.current
        ?.querySelector<HTMLElement>('button, a, [tabindex]')
        ?.focus();
    }, 50);

    return () => {
      document.removeEventListener("keydown", trapFocus);
      clearTimeout(timer);
    };
  }, [narrativeId, onClose]);

  const isOpen = narrativeId !== null;

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/60 z-40"
          onClick={onClose}
          aria-hidden="true"
        />
      )}

      {/* Drawer panel */}
      <div
        ref={drawerRef}
        data-testid="investigate-drawer"
        role="dialog"
        aria-modal="true"
        aria-label="Narrative investigation"
        className={`fixed top-0 right-0 h-full w-full max-w-md z-50 flex flex-col shadow-xl transition-transform duration-slow ${
          isOpen ? "translate-x-0" : "translate-x-full"
        }`}
        style={{ background: 'var(--bg-surface)', borderLeft: '1px solid var(--bg-border)' }}
      >
        {/* Header */}
        <div className="flex items-start justify-between p-5 border-b border-border-subtle sticky top-0" style={{ background: 'var(--bg-surface)' }}>
          <div className="flex-1 min-w-0">
            <h2 className="text-text-primary font-semibold text-sm leading-snug line-clamp-2 tracking-tight font-display">
              {data?.name ?? (loading ? "Loading\u2026" : isOpen ? "Investigation" : "")}
            </h2>
            {data?.entropy !== null && data?.entropy !== undefined && (
              <span
                className="mt-1 inline-block text-xs font-mono-data bg-inset text-text-tertiary px-1.5 py-0.5 rounded cursor-help"
                title="Source diversity — how complex and multi-sourced this narrative is"
              >
                diversity {data.entropy.toFixed(3)}
              </span>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-text-tertiary hover:text-text-primary transition-colors ml-3 shrink-0"
            aria-label="Close investigation drawer"
          >
            <X size={18} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-5 flex flex-col gap-5">
          {loading && (
            <div className="text-text-tertiary text-sm text-center py-8">
              Loading&hellip;
            </div>
          )}

          {fetchError && (
            <div className="text-bearish text-sm text-center py-8">
              {fetchError}
            </div>
          )}

          {data && (
            <>
              {/* Velocity sparkline */}
              <div>
                <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
                  Momentum trend
                </p>
                <VelocitySparkline
                  timeseries={data.velocity_timeseries}
                  width={200}
                  height={40}
                />
              </div>

              {/* Reasoning trace */}
              <div>
                <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
                  Analysis
                </p>
                <p className="text-text-secondary text-xs leading-relaxed">
                  {data.descriptor}
                </p>
                <p
                  className="text-text-tertiary text-xs leading-relaxed mt-1 cursor-help"
                  title="Narrative momentum — how fast this story is evolving"
                >
                  {data.velocity_summary}
                </p>
              </div>

              {/* Evidence — top 5 signals */}
              {data.signals.length > 0 && (
                <div>
                  <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
                    Evidence
                  </p>
                  <ul className="flex flex-col gap-2">
                    {data.signals.slice(0, 5).map((sig) => (
                      <li
                        key={sig.id}
                        className="py-2"
                        style={{ borderBottom: "1px solid var(--border-subtle-soft)" }}
                      >
                        <p className="text-text-primary text-xs font-medium leading-snug line-clamp-2">
                          {sig.headline || "(no headline)"}
                        </p>
                        <p className="text-text-tertiary text-xs mt-0.5">
                          {sig.source.name}
                          {sig.timestamp
                            ? ` \u00b7 ${new Date(sig.timestamp).toLocaleDateString()}`
                            : ""}
                        </p>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Sources with credibility score bars */}
              {data.signals.length > 0 && (
                <div>
                  <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
                    Sources
                  </p>
                  <ul className="flex flex-col gap-1">
                    {sourceRows.map((src) => (
                      <li
                        key={src.id}
                        className="flex items-center justify-between text-xs"
                      >
                        <span className="text-text-secondary">{src.name}</span>
                        <div className="flex items-center gap-1">
                          <div className="w-12 h-1 bg-inset overflow-hidden">
                            <div
                              className="h-full bg-accent-primary"
                              style={{
                                width: `${src.credibility_score * 100}%`,
                              }}
                            />
                          </div>
                          <span className="text-text-disabled font-mono-data">
                            {(src.credibility_score * 100).toFixed(0)}
                          </span>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Catalyst timeline */}
              {data.catalysts.length > 0 && (
                <div>
                  <p className="text-xs uppercase tracking-widest text-accent-text font-medium border-l-2 border-l-accent-primary pl-2 mb-3">
                    Catalyst timeline
                  </p>
                  <ol className="flex flex-col gap-2">
                    {[...data.catalysts]
                      .sort((a, b) => a.timestamp.localeCompare(b.timestamp))
                      .map((cat) => (
                        <li key={cat.id} className="flex gap-2 text-xs">
                          <span className="text-text-disabled font-mono-data shrink-0">
                            {cat.timestamp
                              ? new Date(cat.timestamp).toLocaleDateString()
                              : "\u2014"}
                          </span>
                          <span className="text-text-secondary">{cat.description}</span>
                          <span className="ml-auto text-text-disabled font-mono-data shrink-0">
                            {(cat.impact_score * 100).toFixed(0)}%
                          </span>
                        </li>
                      ))}
                  </ol>
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        {data && (
          <div className="p-5 border-t border-border-subtle sticky bottom-0" style={{ background: 'var(--bg-surface)' }}>
            <a
              href={`/narrative/${narrativeId}`}
              className="text-accent-text text-xs hover:text-accent-hover transition-colors"
            >
              Full Report &rarr;
            </a>
          </div>
        )}
      </div>
    </>
  );
}

