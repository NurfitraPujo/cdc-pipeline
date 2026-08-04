# Container/Node image for the dashboard.
#
# This targets the Node build (`pnpm build`), NOT the Cloudflare Workers build.
# Both come from the same source -- see BUILD_TARGET in vite.config.ts. To ship
# to Cloudflare instead, use `pnpm run deploy` from a workstation or CI; this
# image is not involved in that path.
#
# The previous version of this file ran `npx wrangler dev` as its entrypoint,
# which boots Cloudflare's *development* server (workerd runtime + dev proxy +
# file watchers) and was OOMKilled within 8s against the chart's 256Mi limit.

# Stage 1: Build
FROM node:24-slim AS builder
WORKDIR /app
RUN npm install -g pnpm

# Copy package config and lock files
COPY package.json pnpm-lock.yaml* pnpm-workspace.yaml* .npmrc* ./

# Install dependencies (dev deps included -- vite/tanstack are needed to build)
RUN pnpm install --frozen-lockfile

# Copy source code and compile the TanStack Start project for Node
COPY . .
RUN pnpm run build

# Stage 2: Prune to production dependencies only.
# dist/server/server.js imports react/@tanstack/* at runtime as externals, so
# node_modules is still required -- but the build-only half of it is not.
FROM node:24-slim AS deps
WORKDIR /app
RUN npm install -g pnpm
COPY package.json pnpm-lock.yaml* pnpm-workspace.yaml* .npmrc* ./
RUN pnpm install --frozen-lockfile --prod

# Stage 3: Runner
FROM node:24-slim AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/server-node.mjs ./server-node.mjs
COPY --from=deps /app/node_modules ./node_modules

# Drop root -- the node image ships an unprivileged `node` user.
USER node

EXPOSE 3000

CMD ["node", "server-node.mjs"]
