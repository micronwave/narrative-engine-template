"use client";

import { useEffect, useState } from "react";
import { fetchSecurities, type TrackedSecurity } from "@/lib/api";
import Skeleton from "@/components/common/Skeleton";

export default function BriefsIndexPage() {
  const [securities, setSecurities] = useState<TrackedSecurity[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchSecurities()
      .then(setSecurities)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        <h1 className="text-[22px] font-semibold text-text-primary font-display" style={{ letterSpacing: "-0.01em" }}>
          Intelligence Briefs
        </h1>
        <p className="font-mono text-[11px] text-text-muted mt-1">
          Select a security to view its narrative intelligence brief
        </p>

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-8" />

        {/* Header row */}
        <div
          className="flex items-center justify-between px-4 font-mono text-[10px] uppercase tracking-[0.05em] text-text-muted"
          style={{ height: 28, borderBottom: "1px solid rgba(56, 62, 71, 0.2)" }}
        >
          <span>Security</span>
          <span>Price</span>
        </div>

        {loading ? (
          <div className="flex flex-col gap-1 mt-1">
            {[1, 2, 3, 4, 5, 6].map((i) => (
              <Skeleton key={i} height={48} />
            ))}
          </div>
        ) : (
          <div className="flex flex-col">
            {securities.map((sec) => (
              <a
                key={sec.id}
                href={`/brief/${sec.symbol}`}
                className="flex items-center justify-between px-4 py-3 transition-colors duration-[120ms] hover:bg-[var(--accent-primary-hover)] cursor-pointer"
                style={{ borderBottom: "1px solid rgba(56, 62, 71, 0.13)" }}
                data-testid={`brief-link-${sec.symbol}`}
              >
                <div>
                  <span className="font-mono text-[13px] font-semibold text-text-primary">
                    {sec.symbol}
                  </span>
                  <span className="text-text-secondary text-[12px] ml-2">{sec.name}</span>
                </div>
                <div className="text-right">
                  {sec.current_price !== null ? (
                    <span className="font-mono text-[13px] text-text-primary">
                      ${sec.current_price.toFixed(2)}
                    </span>
                  ) : (
                    <span className="font-mono text-[12px] text-text-muted">—</span>
                  )}
                </div>
              </a>
            ))}
          </div>
        )}

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
