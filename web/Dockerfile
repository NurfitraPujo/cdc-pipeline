# Stage 1: Build
FROM node:24-slim AS builder
WORKDIR /app
RUN npm install -g pnpm

# Copy package config and lock files
COPY package.json pnpm-lock.yaml* pnpm-workspace.yaml* .npmrc* ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy source code and compile TanStack Start project
COPY . .
RUN pnpm run build

# Stage 2: Runner
FROM node:24-slim AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000

# Copy build/SSR artifacts and dependencies
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/node_modules ./node_modules

EXPOSE 3000

# Serve the Cloudflare Worker build locally using wrangler dev
CMD ["npx", "wrangler", "dev", "--port", "3000", "--ip", "0.0.0.0", "--config", "dist/server/wrangler.json"]
