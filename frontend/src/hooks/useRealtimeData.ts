import { useEffect, useRef, useState } from "react";

type Config<T> = {
  endpoint: string;
  interval?: number;
  onUpdate?: (data: T) => void;
};

type State<T> = {
  data: T | null;
  isConnected: boolean;
  error: string | null;
};

/**
 * Polling-based real-time data hook.
 *
 * Primary strategy: setInterval polling every `interval` ms (default 10s).
 * SSE is available server-side at GET /api/stream for C4 direct EventSource use —
 * Next.js proxy buffers chunked responses, so SSE through the proxy is unreliable.
 *
 * Usage:
 *   const { data, isConnected, error } = useRealtimeData<TickerItem[]>({
 *     endpoint: '/api/ticker',
 *     interval: 10000,
 *   });
 */
export function useRealtimeData<T>({
  endpoint,
  interval = 10000,
  onUpdate,
}: Config<T>): State<T> {
  const [state, setState] = useState<State<T>>({
    data: null,
    isConnected: false,
    error: null,
  });

  // Keep onUpdate in a ref so the polling closure always calls the latest version
  // without needing to be in the effect dependency array.
  const onUpdateRef = useRef(onUpdate);
  onUpdateRef.current = onUpdate;

  useEffect(() => {
    let active = true;

    async function poll() {
      try {
        const res = await fetch(endpoint);
        if (!res.ok) throw new Error(`${endpoint} fetch failed: ${res.status}`);
        const data: T = await res.json();
        if (!active) return;
        setState({ data, isConnected: true, error: null });
        onUpdateRef.current?.(data);
      } catch (err) {
        if (!active) return;
        setState((prev) => ({ ...prev, error: (err as Error).message }));
      }
    }

    // Initial fetch immediately, then poll on interval
    poll();
    const id = setInterval(poll, interval);

    return () => {
      active = false;
      clearInterval(id);
    };
  }, [endpoint, interval]);

  return state;
}
