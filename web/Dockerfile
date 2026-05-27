FROM node:22-slim

WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy the entire source code (including node_modules if present on the host)
COPY . .

# Set CI environment variable to avoid TTY issues
ENV CI=true

# Only run pnpm install if node_modules does not exist (reusing host node_modules if available)
RUN if [ ! -d "node_modules" ]; then pnpm install --config.confirm-modules-purge=false; fi

# Expose port 3000
EXPOSE 3000

# Start Vite dev server after ensuring all dependencies are reconciled without frozen-lockfile issues
CMD ["sh", "-c", "pnpm install --no-frozen-lockfile && pnpm dev --host"]
