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
