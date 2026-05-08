"use client";

import { useEffect, useState } from "react";
import { fetchPriceHistory, type PriceHistoryResponse } from "@/lib/api";

export function useStockDetailHistory(
  isOpen: boolean,
  symbol: string | undefined,
  chartDays: number
) {
  const [priceHistory, setPriceHistory] = useState<PriceHistoryResponse | null>(null);

  useEffect(() => {
    if (!isOpen || !symbol) {
      setPriceHistory(null);
      return;
    }

    let cancelled = false;
    fetchPriceHistory(symbol, chartDays)
      .then((result) => {
        if (!cancelled) setPriceHistory(result);
      })
      .catch(() => {
        if (!cancelled) setPriceHistory(null);
      });

    return () => {
      cancelled = true;
    };
  }, [isOpen, symbol, chartDays]);

  return { priceHistory };
}
