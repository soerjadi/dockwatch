# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /dockwatch \
    ./cmd/dockwatch

# ── Runtime stage ─────────────────────────────────────────────────────────────
# Distroless: no shell, no package manager — minimal attack surface.
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /dockwatch /dockwatch

# Web UI + SSE
EXPOSE 3010

# Docker socket is mounted at runtime:
#   -v /var/run/docker.sock:/var/run/docker.sock

ENTRYPOINT ["/dockwatch"]
