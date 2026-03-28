"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import {
  fetchSubscription,
  toggleSubscription as apiToggle,
  type SubscriptionStatus,
} from "@/lib/api";
import { useAuth } from "./AuthContext";

type SubscriptionContextValue = {
  subscribed: boolean;
  status: SubscriptionStatus | null;
  toggle: () => Promise<void>;
  refetch: () => void;
};

export const SubscriptionContext = createContext<SubscriptionContextValue>({
  subscribed: false,
  status: null,
  toggle: () => Promise.resolve(),
  refetch: () => {},
});

export function SubscriptionProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const { isSignedIn, token } = useAuth();
  const [status, setStatus] = useState<SubscriptionStatus | null>(null);

  const refetch = useCallback(() => {
    if (!isSignedIn || !token) {
      setStatus(null);
      return;
    }
    fetchSubscription(token)
      .then(setStatus)
      .catch(() => setStatus(null));
  }, [isSignedIn, token]);

  const toggle = useCallback(async () => {
    if (!token) return;
    const updated = await apiToggle(token);
    setStatus(updated);
  }, [token]);

  useEffect(() => {
    refetch();
  }, [refetch]);

  return (
    <SubscriptionContext.Provider
      value={{
        subscribed: status?.subscribed ?? false,
        status,
        toggle,
        refetch,
      }}
    >
      {children}
    </SubscriptionContext.Provider>
  );
}

export function useSubscription() {
  return useContext(SubscriptionContext);
}
