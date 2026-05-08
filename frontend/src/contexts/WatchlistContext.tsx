"use client";

import { createContext, useContext, useCallback, useEffect, useState, type ReactNode } from "react";
import { fetchWatchlist, addToWatchlist, removeFromWatchlist } from "@/lib/api";

type WatchlistContextValue = {
  watchedIds: Set<string>;
  isWatched: (id: string) => boolean;
  toggleWatch: (type: "narrative" | "ticker", id: string) => Promise<void>;
  refresh: () => void;
};

const WatchlistContext = createContext<WatchlistContextValue>({
  watchedIds: new Set(),
  isWatched: () => false,
  toggleWatch: async () => {},
  refresh: () => {},
});

export function WatchlistProvider({ children }: { children: ReactNode }) {
  const [watchedIds, setWatchedIds] = useState<Set<string>>(new Set());
  const [itemMap, setItemMap] = useState<Map<string, string>>(new Map()); // item_id → watchlist_item_id

  const refresh = useCallback(() => {
    fetchWatchlist()
      .then((data) => {
        const ids = new Set<string>();
        const map = new Map<string, string>();
        for (const item of data.items) {
          ids.add(item.item_id);
          map.set(item.item_id, item.id);
        }
        setWatchedIds(ids);
        setItemMap(map);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const isWatched = useCallback((id: string) => watchedIds.has(id), [watchedIds]);

  const toggleWatch = useCallback(async (type: "narrative" | "ticker", id: string) => {
    if (watchedIds.has(id)) {
      const watchlistItemId = itemMap.get(id);
      if (watchlistItemId) {
        await removeFromWatchlist(watchlistItemId);
      }
    } else {
      await addToWatchlist(type, id);
    }
    refresh();
  }, [watchedIds, itemMap, refresh]);

  return (
    <WatchlistContext.Provider value={{ watchedIds, isWatched, toggleWatch, refresh }}>
      {children}
    </WatchlistContext.Provider>
  );
}

export function useWatchlist() {
  return useContext(WatchlistContext);
}
