# CDC Data Pipeline: Web Frontend

A modern, responsive, and real-time frontend dashboard for visualizing and managing the Change Data Capture (CDC) pipeline. It provides control plane capabilities, real-time metrics, and configuration management.

## Tech Stack

- **Framework & Language**: React 19, TypeScript, Vite
- **Routing**: TanStack Router (Typesafe, file-based routing)
- **State Management**: TanStack Query (Server State), Zustand (Client/UI State)
- **Styling**: Tailwind CSS
- **Code Quality**: Biome (Linting and formatting)
- **Testing**: Vitest, React Testing Library

## Directory Structure

Detailed technical documentation is organized by subdirectory:

- **[`src/api/`](src/api/AGENT.md)**: API Client wrapper around `fetch` with automatic JWT handling and 401 redirection.
- **[`src/components/`](src/components/AGENT.md)**: Reusable UI components, layouts, and primitive UI design elements.
- **[`src/hooks/`](src/hooks/AGENT.md)**: Custom React hooks (e.g., `useSSE` for real-time metrics).
- **[`src/routes/`](src/routes/AGENT.md)**: Routing hierarchy, layouts, and page-level views.
- **[`src/stores/`](src/stores/AGENT.md)**: Zustand client-side global state stores.
- **[`src/test/`](src/test/AGENT.md)**: Automated unit and integration testing suite.

## Building and Running

Commands are configured using `pnpm`:

- **Install Dependencies**: `pnpm install`
- **Development Mode**: `pnpm dev`
- **Production Build**: `pnpm build`
- **Lint and Format**: `pnpm check`
- **Run Tests**: `pnpm test`

## Core Concepts & Design Principles

1. **Strict Type Safety**: End-to-end type safety from Go backend models using shared TypeScript interface definitions in `src/api/types.ts`.
2. **Real-time Synchronization**: Uses Server-Sent Events (SSE) via the `useSSE` hook to display real-time LSN progress and pipeline operational statistics.
3. **Optimistic & Resilient Auth**: JWT token lifecycle is managed transparently via the combination of `useAuthStore` and `apiClient` middleware.
