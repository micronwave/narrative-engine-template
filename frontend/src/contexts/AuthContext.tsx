"use client";

import { createContext, useContext, useEffect, useState } from "react";

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

  // Read from localStorage on mount (client-only)
  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored === STUB_TOKEN) setToken(stored);
    } catch {
      // localStorage unavailable (SSR or privacy mode)
    }
  }, []);

  function signIn() {
    try {
      localStorage.setItem(STORAGE_KEY, STUB_TOKEN);
    } catch {
      // ignore
    }
    setToken(STUB_TOKEN);
  }

  function signOut() {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
    setToken(null);
  }

  return (
    <AuthContext.Provider value={{ isSignedIn: token !== null, token, signIn, signOut }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
