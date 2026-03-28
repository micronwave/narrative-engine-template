"use client";

import { createContext, useContext, useCallback, useEffect, useState, type ReactNode } from "react";
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

  const refresh = useCallback(() => {
    fetchAlertCount().then((d) => setUnreadCount(d.unread)).catch(() => {});
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 30000); // poll every 30s
    return () => clearInterval(interval);
  }, [refresh]);

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
