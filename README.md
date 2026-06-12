# dockwatch

> Event-driven, in-memory Docker container update manager.  

---

## High-Level Design

```
┌────────────────────────────────────────────────────────────────┐
│                         LAYER 1 — Sources                      │
│                                                                │
│  ┌──────────────────────┐  ┌───────────────┐  ┌────────────┐  │
│  │ Docker Daemon Stream  │  │ Inbound       │  │ Registry   │  │
│  │ (primary)             │  │ Webhook       │  │ Poller     │  │
│  │                       │  │ (preferred)   │  │ (last      │  │
│  │ /var/run/docker.sock  │  │               │  │ resort)    │  │
│  │                       │  │ POST          │  │            │  │
│  │ Listens for:          │  │ /webhook/push │  │ HEAD only  │  │
│  │ • container start     │  │               │  │ cron-based │  │
│  │ • container die       │  │ POST          │  │ fallback   │  │
│  │ • health_status       │  │ /webhook/     │  │            │  │
│  │ • container destroy   │  │  dockerhub    │  │            │  │
│  └──────────────────────┘  └───────────────┘  └────────────┘  │
│                                    ▲                           │
│                         CI/CD calls here                       │
│                         after docker push                      │
└───────────────────────────┬────────────────────────────────────┘
                            │ publishes to
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                    LAYER 2 — In-Memory Event Bus               │
│                                                                │
│          Go channels · buffered · topic fanout                 │
│          Zero external dependencies                            │
│                                                                │
│  ┌──────────────┐ ┌──────────────────────┐ ┌───────────────┐  │
│  │image.updated │ │container.unhealthy   │ │update.applied │  │
│  └──────────────┘ └──────────────────────┘ └───────────────┘  │
│  ┌──────────────┐ ┌──────────────────────┐                    │
│  │update.skipped│ │rollback.done         │                    │
│  └──────────────┘ └──────────────────────┘                    │
└───────────────────────────┬────────────────────────────────────┘
                            │ consumed by
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                  LAYER 3 — Internal Consumers                  │
│                  (all goroutines in same process)              │
│                                                                │
│  ┌──────────────────┐ ┌─────────────────┐ ┌────────────────┐  │
│  │ Update Executor  │ │ Health Monitor  │ │   Notifier     │  │
│  │                  │ │                 │ │                │  │
│  │ Subscribes to:   │ │ Subscribes to:  │ │ Subscribes to: │  │
│  │ image.updated    │ │ update.applied  │ │ all topics     │  │
│  │                  │ │                 │ │                │  │
│  │ Applies semver   │ │ Watches health  │ │ Logs + SSE     │  │
│  │ strategy rules   │ │ for grace window│ │ push to UI     │  │
│  │ per container    │ │                 │ │                │  │
│  │ label            │ │ Publishes       │ │ Fires per-     │  │
│  │                  │ │ container.      │ │ event, not     │  │
│  │ Publishes:       │ │ unhealthy if    │ │ per-session    │  │
│  │ update.applied   │ │ health check    │ │                │  │
│  │ update.skipped   │ │ fails           │ └────────────────┘  │
│  └──────────────────┘ └─────────────────┘                     │
└───────────────────────────┬────────────────────────────────────┘
                            │ rollback path
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                LAYER 4 — Rollback Engine                       │
│                *** NOT present in Watchtower or WUD ***        │
│                                                                │
│  Subscribes to: container.unhealthy                           │
│                                                                │
│  1. Reads previous image digest from in-memory store          │
│     (ring buffer, last 5 digests per container)               │
│  2. Re-pulls image by digest: image@sha256:<prev>             │
│  3. Stops + removes current container                         │
│  4. Recreates container with previous image                   │
│  5. Publishes rollback.done                                   │
└───────────────────────────┬────────────────────────────────────┘
                            │ exposes
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                    LAYER 5 — Outputs                           │
│                                                                │
│  ┌─────────────────┐ ┌──────────────────┐ ┌────────────────┐  │
│  │ Web UI + SSE    │ │   REST API       │ │ Structured Log │  │
│  │                 │ │                 │ │                │  │
│  │ port :3000      │ │ GET  /api/       │ │ JSON to stdout │  │
│  │ Real-time push  │ │   containers    │ │ Per-event      │  │
│  │ via SSE from    │ │ POST /api/       │ │                │  │
│  │ in-memory bus   │ │   update/:id    │ │                │  │
│  │ No UI polling   │ │ POST /api/       │ │                │  │
│  │                 │ │   rollback/:id  │ │                │  │
│  └─────────────────┘ │ GET  /api/events│ └────────────────┘  │
│                      │ POST /webhook/  │                      │
│                      │   push          │                      │
│                      │ POST /webhook/  │                      │
│                      │   dockerhub     │                      │
│                      └─────────────────┘                      │
└────────────────────────────────────────────────────────────────┘
```

### In-Memory Bus Topics

| Topic | Published by | Consumed by |
|---|---|---|
| `image.updated` | Watcher / Registry Poller | Executor |
| `container.unhealthy` | Watcher (health_status event), Health Monitor | Rollback Engine |
| `update.applied` | Executor | Health Monitor, Notifier |
| `update.skipped` | Executor | Notifier |
| `rollback.done` | Rollback Engine | Notifier |

### In-Memory State Store

Each container gets a `ContainerState` with a **ring buffer of the last 5 image digests**.  
The Rollback Engine reads `PreviousDigest()` from this buffer — no external state, no database.

```
Container: nginx
  digest[0] sha256:abc...  ← current (just updated)
  digest[1] sha256:def...  ← previous (rollback target)
  digest[2] sha256:ghi...  ← 2 versions ago
```

---

## Project Structure

```
dockwatch/
├── cmd/
│   └── dockwatch/
│       └── main.go          # entrypoint — wires all components
├── internal/
│   ├── bus/
│   │   ├── bus.go           # in-memory pub/sub (Go channels)
│   │   └── payloads.go      # typed event payload structs
│   ├── store/
│   │   └── store.go         # in-memory container state + digest ring buffer
│   ├── watcher/
│   │   └── watcher.go       # Docker event stream subscriber
│   ├── registry/
│   │   └── registry.go      # HEAD manifest check (no full pull)
│   ├── executor/
│   │   └── executor.go      # update executor + semver strategy
│   ├── healthmon/
│   │   └── healthmon.go     # post-update health watch window
│   ├── rollback/
│   │   └── rollback.go      # auto rollback on container.unhealthy
│   ├── notifier/
│   │   └── notifier.go      # SSE push + structured log dispatcher
│   └── api/
│       └── api.go           # REST API + SSE server
├── config/
│   └── config.go            # env-var config loader
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Quick Start

```bash
# Build
docker build -t dockwatch .

# Run
docker run -d \
  --name dockwatch \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -p 3000:3000 \
  dockwatch

# Or with docker compose
docker compose up -d
```

Open `http://localhost:3000` for the dashboard.  
SSE stream at `http://localhost:3000/api/events`.

---

## Inbound Webhooks

This is the preferred way for dockwatch to learn about new images — **zero polling**.  
Your CI/CD pipeline calls dockwatch right after `docker push` completes.

### Endpoint: POST /webhook/push

Generic endpoint for any CI/CD system.

**Payload:**
```json
{
  "image":  "yourrepo/myapp",
  "tag":    "1.2.3",
  "digest": "sha256:abc123...",
  "source": "github-actions"
}
```

`digest` is optional but recommended — if provided, dockwatch skips the registry
HEAD check entirely and uses it directly.

**Signature (HMAC-SHA256):**
```bash
# Generate a secret
SECRET=$(openssl rand -hex 32)

# Sign the payload
PAYLOAD='{"image":"yourrepo/myapp","tag":"1.2.3"}'
SIG="sha256=$(echo -n "$PAYLOAD" | openssl dgmac -sha256 -hmac "$SECRET" | tr -d ' \n')"

curl -X POST http://dockwatch:3000/webhook/push \
  -H "Content-Type: application/json" \
  -H "X-Dockwatch-Signature: $SIG" \
  -d "$PAYLOAD"
```

---

### Endpoint: POST /webhook/dockerhub

Accepts Docker Hub's native webhook format directly. Configure in Docker Hub under:  
**Repository → Webhooks → Add Webhook URL → `http://your-host:3000/webhook/dockerhub`**

No signature is sent by Docker Hub — protect this endpoint with network-level controls.

---

### CI/CD Integration Examples

**GitHub Actions** — call after `docker push`:
```yaml
- name: Notify dockwatch
  run: |
    PAYLOAD=$(jq -n \
      --arg image "${{ env.IMAGE_NAME }}" \
      --arg tag "${{ github.sha }}" \
      --arg digest "${{ steps.push.outputs.digest }}" \
      '{image: $image, tag: $tag, digest: $digest, source: "github-actions"}')

    SIG="sha256=$(echo -n "$PAYLOAD" | openssl dgmac -sha256 \
      -hmac "${{ secrets.DOCKWATCH_WEBHOOK_SECRET }}")"

    curl -sf -X POST https://your-dockwatch-host/webhook/push \
      -H "Content-Type: application/json" \
      -H "X-Dockwatch-Signature: $SIG" \
      -d "$PAYLOAD"
```

**GitLab CI** — in `.gitlab-ci.yml`:
```yaml
notify_dockwatch:
  stage: deploy
  script:
    - |
      PAYLOAD="{\"image\":\"$CI_REGISTRY_IMAGE\",\"tag\":\"$CI_COMMIT_SHA\",\"source\":\"gitlab-ci\"}"
      SIG="sha256=$(echo -n "$PAYLOAD" | openssl dgmac -sha256 -hmac "$DOCKWATCH_WEBHOOK_SECRET")"
      curl -sf -X POST https://your-dockwatch-host/webhook/push \
        -H "Content-Type: application/json" \
        -H "X-Dockwatch-Signature: $SIG" \
        -d "$PAYLOAD"
```

---

## Configuration

All configuration is via environment variables — no config file needed.

| Variable | Default | Description |
|---|---|---|
| `DOCKWATCH_ADDR` | `:3000` | HTTP listen address |
| `DOCKWATCH_LOG_LEVEL` | `info` | Log verbosity: debug/info/warn/error |
| `DOCKWATCH_REGISTRY_CRON` | `0 0 4 * * *` | Fallback registry poll schedule (6-field cron) |
| `DOCKWATCH_HEALTH_GRACE` | `30s` | How long to watch a container post-update |
| `DOCKWATCH_DRY_RUN` | `false` | Disable all destructive actions |
| `DOCKWATCH_WEBHOOK_SECRET` | `""` | HMAC-SHA256 secret for `/webhook/push` validation |

---

## Container Labels

Per-container behaviour is controlled via Docker labels:

| Label | Values | Description |
|---|---|---|
| `dockwatch.watch` | `true` / `false` | Include this container (default: all) |
| `dockwatch.update` | `auto` / `minor` / `patch` / `notify` | Update strategy |
| `dockwatch.health.grace` | duration e.g. `60s` | Override health grace window |

**Update strategy rules:**

- `auto` — always apply updates automatically (default)
- `minor` — auto-apply minor + patch; notify-only on major
- `patch` — auto-apply patch only; notify-only on minor + major
- `notify` — never auto-update; only send notifications

---

## Implementation Status

| Component | Status | Notes |
|---|---|---|
| In-memory event bus | ✅ Complete | `internal/bus` |
| In-memory state store | ✅ Complete | `internal/store` |
| Config loader | ✅ Complete | `config/config.go` |
| Inbound webhook receiver | ✅ Complete | `internal/webhook` — `/webhook/push` + `/webhook/dockerhub` |
| HMAC signature validation | ✅ Complete | `internal/webhook` — `X-Dockwatch-Signature` header |
| REST API + SSE | ✅ Complete | `internal/api` |
| Notifier (SSE + log) | ✅ Complete | `internal/notifier` |
| Docker event watcher | 🔧 Interface only | Wire real Docker SDK client via `watcher.DockerClient` |
| Registry HEAD check | 🔧 Stub | Implement token + HEAD request (fallback only) |
| Update executor | 🔧 Skeleton | Wire Docker SDK stop/pull/start |
| Health monitor | 🔧 Skeleton | Wire Docker inspect for health status |
| Rollback engine | 🔧 Skeleton | Wire Docker SDK image pull by digest |

---

