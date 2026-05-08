"use client";

import { createContext, useContext, useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { fetchAlertCount, markAlertRead as apiMarkRead, markAllAlertsRead as apiMarkAll } from "@/lib/api";

type AlertContextValue = {
  unreadCount: number;
  markRead: (id: string) => void;
  markAllRead: () => void;
  refresh: () => void;
};

const AlertContext = createContext<AlertContextValue>({
  unreadCount: 0,
  markRead: () => {},
  markAllRead: () => {},
  refresh: () => {},
});

export function AlertProvider({ children }: { children: ReactNode }) {
  const [unreadCount, setUnreadCount] = useState(0);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const refresh = useCallback(() => {
    fetchAlertCount().then((d) => setUnreadCount(d.unread)).catch(() => {});
  }, []);

  const startPolling = useCallback(() => {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = setInterval(refresh, 30000);
  }, [refresh]);

  useEffect(() => {
    refresh();

    if (typeof window === "undefined" || !("EventSource" in window)) {
      startPolling();
      return () => {
        if (pollRef.current) clearInterval(pollRef.current);
      };
    }

    let es: EventSource | null = null;

    const connect = () => {
      es = new EventSource("/api/alerts/stream", { withCredentials: true });

      es.addEventListener("alert", () => {
        setUnreadCount((c) => c + 1);
      });

      es.onerror = () => {
        es?.close();
        es = null;
        // Fall back to polling on SSE failure
        startPolling();
      };
    };

    connect();

    return () => {
      es?.close();
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [refresh, startPolling]);

  const markRead = useCallback((id: string) => {
    apiMarkRead(id).then(refresh).catch(() => {});
  }, [refresh]);

  const markAllRead = useCallback(() => {
    apiMarkAll().then(refresh).catch(() => {});
  }, [refresh]);

  return (
    <AlertContext.Provider value={{ unreadCount, markRead, markAllRead, refresh }}>
      {children}
    </AlertContext.Provider>
  );
}

export function useAlerts() {
  return useContext(AlertContext);
}
