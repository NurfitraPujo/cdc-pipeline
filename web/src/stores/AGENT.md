# Global State Stores

The `src/stores/` directory holds the application's global client-side state, powered by **Zustand** for lightweight and high-performance reactivity.

## Active Stores

- **`authStore.ts`**: Coordinates the active user session and security credentials.
  - **State**:
    - `token` (string | null): The current JSON Web Token (JWT) retrieved from the API control plane.
    - `isAuthenticated` (boolean): Flag asserting whether there is a valid, active session.
  - **Actions**:
    - `setToken(token: string | null)`: Updates the token state and sets the authentication flag accordingly.
    - `logout()`: Clears the credentials and marks the session as unauthenticated.
  - **Middleware**: Utilizes `persist` middleware with `createJSONStorage(() => localStorage)` to store the authentication credentials under the key `"cdc-auth-storage"`.

## Security Conventions

> [!WARNING]
> Storing raw JWT tokens in browser `localStorage` exposes them to potential Cross-Site Scripting (XSS) extraction attacks.
> 
> **Protection Mandates**:
> 1. Restrict inline execution using a strong Content Security Policy (CSP).
> 2. Ensure all dynamic data render targets are strictly sanitized.
> 3. Limit token longevity on the backend to reduce the impact of compromised storage tokens.
