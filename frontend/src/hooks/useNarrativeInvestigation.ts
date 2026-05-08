"use client";

import { useEffect, useState } from "react";
import { fetchNarrativeDetail, type NarrativeDetail } from "@/lib/api";

export function useNarrativeInvestigation(narrativeId: string | null) {
  const [data, setData] = useState<NarrativeDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!narrativeId) {
      setData(null);
      setError(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    fetchNarrativeDetail(narrativeId)
      .then((detail) => {
        if (!cancelled) setData(detail);
      })
      .catch((err) => {
        if (!cancelled) setError((err as Error).message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [narrativeId]);

  return { data, loading, error };
}
