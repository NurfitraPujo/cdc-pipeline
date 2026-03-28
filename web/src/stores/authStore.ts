/**
 * SECURITY NOTICE: localStorage Token Storage
 *
 * This store persists the authentication token in localStorage, which is
 * standard practice for Single Page Applications (SPAs). However, this
 * approach is vulnerable to XSS (Cross-Site Scripting) attacks.
 *
 * XSS PROTECTION RECOMMENDATIONS:
 * 1. Implement a strict Content Security Policy (CSP) to prevent inline scripts
 * 2. Sanitize all user input and HTML rendering to prevent script injection
 * 3. Use HttpOnly cookies as an alternative if XSS is a critical concern
 * 4. Implement short token expiration and refresh token rotation
 * 5. Clear tokens on any suspected security event
 */

import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

interface AuthState {
  token: string | null;
  isAuthenticated: boolean;
}

interface AuthActions {
  setToken: (token: string | null) => void;
  logout: () => void;
}

type AuthStore = AuthState & AuthActions;

export const useAuthStore = create<AuthStore>()(
  persist(
    (set) => ({
      token: null,
      isAuthenticated: false,
      setToken: (token: string | null) =>
        set({
          token,
          isAuthenticated: token !== null && token !== '',
        }),
      logout: () =>
        set({
          token: null,
          isAuthenticated: false,
        }),
    }),
    {
      name: 'cdc-auth-storage',
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        token: state.token,
        isAuthenticated: state.isAuthenticated,
      }),
    }
  )
);

export type { AuthState, AuthActions, AuthStore };
