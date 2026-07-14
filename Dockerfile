# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /dockwatch \
    ./cmd/dockwatch

# ── Runtime stage ─────────────────────────────────────────────────────────────
# We use alpine:latest to guarantee the latest OS security patches, and
# install docker-cli directly from the Alpine repositories.
FROM alpine:latest
RUN apk add --no-cache tzdata ca-certificates docker-cli curl && \
    mkdir -p /usr/libexec/docker/cli-plugins && \
    curl -sSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" -o /usr/libexec/docker/cli-plugins/docker-compose && \
    chmod +x /usr/libexec/docker/cli-plugins/docker-compose

COPY --from=builder /dockwatch /usr/local/bin/dockwatch

# Web UI + SSE
EXPOSE 3010

# Docker socket is mounted at runtime:
#   -v /var/run/docker.sock:/var/run/docker.sock

ENTRYPOINT ["/usr/local/bin/dockwatch"]
