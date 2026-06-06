# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev openssh-client

RUN --mount=type=ssh git config --global url."git@bitbucket.org:".insteadOf "https://bitbucket.org/" && \
    mkdir -p -m 0600 ~/.ssh && \
    ssh-keyscan bitbucket.org >> ~/.ssh/known_hosts

# Copy go mod and sum
COPY go.mod go.sum ./

# Copy internal/vendor directory due to replace directive in go.mod
COPY internal/vendor ./internal/vendor

# Download dependencies using SSH agent forwarding and configure git for private bitbucket modules
RUN --mount=type=ssh \
    git config --global url."git@bitbucket.org:".insteadOf "https://bitbucket.org/" && \
    mkdir -p -m 0600 ~/.ssh && \
    ssh-keyscan bitbucket.org >> ~/.ssh/known_hosts && \
    go mod download

# Copy source code
COPY . .

# Copy config.example.yaml into pipeline cmd directory for embedding
RUN cp config.example.yaml cmd/pipeline/config.example.yaml

# Build both binaries
RUN go build -ldflags="-w -s" -o /app/bin/api ./cmd/api/main.go
RUN go build -ldflags="-w -s" -o /app/bin/worker ./cmd/pipeline/main.go

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
