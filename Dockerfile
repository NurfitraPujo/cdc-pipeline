# Stage 1: Build
FROM golang:1.26.1-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

# Copy go mod and sum
COPY go.mod go.sum ./
COPY vendor ./vendor

# Copy source code
COPY . .

# Build both binaries
RUN go build -mod=vendor -ldflags="-w -s" -o /app/bin/api ./cmd/api/main.go
RUN go build -mod=vendor -ldflags="-w -s" -o /app/bin/worker ./cmd/pipeline/main.go

# Stage 2: Runtime
FROM alpine:latest

# Add ca-certificates for NATS and other TLS connections
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/bin/api /app/api
COPY --from=builder /app/bin/worker /app/worker

# Default environment variables
ENV LOG_LEVEL=info
ENV ENV=production

# Expose ports (8080 for API, 8081 for Worker health/metrics)
EXPOSE 8080 8081

# Default to running the worker if no command is provided
# In Kubernetes, we will override this with 'command: ["/app/api"]' or 'command: ["/app/worker"]'
ENTRYPOINT ["/app/worker"]
