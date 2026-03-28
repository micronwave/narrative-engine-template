"use client";

import { useEffect, useState } from "react";
import { fetchConstellation, type ConstellationData } from "@/lib/api";
import ConstellationMap from "@/components/ConstellationMap";
import Skeleton from "@/components/common/Skeleton";

export default function ConstellationPage() {
  const [data, setData] = useState<ConstellationData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchConstellation()
      .then(setData)
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  return (
    <main className="min-h-screen bg-base text-text-primary">
      <div className="max-w-[1280px] mx-auto px-4 lg:px-8 pt-6 pb-16">
        <h1
          className="text-[22px] font-semibold text-text-primary font-display"
          style={{ letterSpacing: "-0.01em" }}
        >
          Constellation
        </h1>
        <p className="font-mono text-[11px] text-text-muted mt-1">
          Narrative relationships and catalyst triggers
        </p>

        <div className="h-px bg-[var(--bg-border)] opacity-50 mt-4 mb-6" />

        {loading && <Skeleton height={560} />}

        {error && (
          <div className="font-mono text-[12px] text-bearish py-8 text-center">
            Failed to load constellation: {error}
          </div>
        )}

        {!loading && !error && data && data.nodes.length > 0 && (
          <ConstellationMap data={data} />
        )}

        {!loading && !error && (!data || data.nodes.length === 0) && (
          <p className="font-mono text-[12px] text-text-muted py-8 text-center">
            No constellation data available. Run the pipeline to generate
            narrative relationships.
          </p>
        )}

        {/* Footer */}
        <div className="mt-10">
          <div className="h-px bg-[var(--bg-border)] opacity-30 mb-4" />
          <div className="flex items-center justify-between font-mono text-[10px] text-text-muted">
            <span>INTELLIGENCE ONLY — NOT FINANCIAL ADVICE</span>
            {data && (
              <span>
                {data.nodes.length} nodes · {data.edges.length} edges
              </span>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}
