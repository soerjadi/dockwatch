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
# docker:cli provides the docker CLI + compose plugin needed for the
# compose-first updater. The dockwatch binary is statically linked (CGO_ENABLED=0)
# so it runs fine in an Alpine-based image.
FROM docker:cli

COPY --from=builder /dockwatch /usr/local/bin/dockwatch

# Web UI + SSE
EXPOSE 3010

# Docker socket is mounted at runtime:
#   -v /var/run/docker.sock:/var/run/docker.sock

ENTRYPOINT ["/usr/local/bin/dockwatch"]
