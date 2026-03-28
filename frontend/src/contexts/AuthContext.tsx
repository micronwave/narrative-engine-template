"use client";

import { createContext, useContext, useEffect, useState } from "react";

const STORAGE_KEY = "auth_token";

type AuthContextValue = {
  isSignedIn: boolean;
  token: string | null;
  signIn: (token: string) => void;
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

  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) setToken(stored);
    } catch {
      /* localStorage unavailable */
    }
  }, []);

  function signIn(authToken: string) {
    try {
      localStorage.setItem(STORAGE_KEY, authToken);
    } catch {
      /* ignore */
    }
    setToken(authToken);
  }

  function signOut() {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      /* ignore */
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
