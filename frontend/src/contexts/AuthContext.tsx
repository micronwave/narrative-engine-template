"use client";

import { createContext, useContext, useEffect, useState, useCallback } from "react";
import { setApiToken } from "@/lib/api";

const STUB_TOKEN = "stub-auth-token";
const STORAGE_KEY = "auth_token";

type AuthContextValue = {
  isSignedIn: boolean;
  token: string | null;
  signIn: () => void;
  signOut: () => void;
};

export const AuthContext = createContext<AuthContextValue>({
  isSignedIn: false,
  token: null,
  signIn: () => {},
  signOut: () => {},
});

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(null);
  const [checked, setChecked] = useState(false);

  // H7: Check auth via /api/auth/me on mount (cookie-based)
  useEffect(() => {
    fetch("/api/auth/me", { credentials: "include" })
      .then((res) => {
        if (res.ok) return res.json();
        return null;
      })
      .then((data) => {
        if (data) {
          // Server confirms we are authenticated — set a marker token
          // For stub mode, use the stub token; for JWT mode, use a sentinel
          // since the actual JWT is now in an HttpOnly cookie
          const effectiveToken = data.auth_mode === "stub" ? STUB_TOKEN : "cookie-auth";
          setToken(effectiveToken);
        }
      })
      .catch(() => {})
      .finally(() => setChecked(true));
  }, []);

  // Backward compat: also check localStorage for stub mode
  useEffect(() => {
    if (checked && token === null) {
      try {
        const stored = localStorage.getItem(STORAGE_KEY);
        if (stored === STUB_TOKEN) setToken(stored);
      } catch {
        // localStorage unavailable (SSR or privacy mode)
      }
    }
  }, [checked, token]);

  // Sync token to the API module for automatic header injection
  useEffect(() => {
    setApiToken(token);
  }, [token]);

  const signIn = useCallback(() => {
    try {
      localStorage.setItem(STORAGE_KEY, STUB_TOKEN);
    } catch {
      // ignore
    }
    setToken(STUB_TOKEN);
  }, []);

  const signOut = useCallback(() => {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
    // H7: Call logout endpoint to clear HttpOnly cookie
    fetch("/api/auth/logout", { method: "POST", credentials: "include" }).catch(
      () => {}
    );
    setToken(null);
  }, []);

  return (
    <AuthContext.Provider value={{ isSignedIn: token !== null, token, signIn, signOut }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
