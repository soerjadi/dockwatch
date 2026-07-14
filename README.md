<p align="center">
  <h1 align="center">Dockwatch</h1>
  <p align="center">Event-driven, in-memory Docker container update manager.</p>
</p>

<p align="center">
  <a href="https://github.com/soerjadi/dockwatch/actions"><img src="https://img.shields.io/github/actions/workflow/status/soerjadi/dockwatch/build.yml?branch=main&label=build&logo=github&style=flat-square" alt="Build Status"></a>
  <a href="https://github.com/soerjadi/dockwatch/releases/latest"><img src="https://img.shields.io/github/release/soerjadi/dockwatch.svg?style=flat-square" alt="GitHub release"></a>
  <a href="https://hub.docker.com/r/soerjadi/dockwatch/"><img src="https://img.shields.io/docker/pulls/soerjadi/dockwatch.svg?style=flat-square&logo=docker" alt="Docker Pulls"></a>
  <a href="https://hub.docker.com/r/soerjadi/dockwatch/"><img src="https://img.shields.io/docker/stars/soerjadi/dockwatch.svg?style=flat-square&logo=docker" alt="Docker Stars"></a>
  <a href="https://goreportcard.com/report/github.com/soerjadi/dockwatch"><img src="https://goreportcard.com/badge/github.com/soerjadi/dockwatch?style=flat-square" alt="Go Report Card"></a>
</p>

---

## About

While tools like Watchtower or standard Docker Compose are great for starting containers, **Dockwatch** acts as an intelligent CI/CD automation pipeline that sits *on top* of them. 

Dockwatch bridges the gap between your CI/CD (like GitHub Actions) and your production servers, acting as an event-driven webhook receiver to instantly deploy updates with zero delay.

## Features

* **Automated Webhook Triggers:** Native webhooks (`/webhook/push`) instantly detect when your CI pushes a new image and trigger the deployment immediately, eliminating polling delays.
* **AST YAML Patching:** Automatically edits your `docker-compose.yml` file to bump the `image:` tag to the new version *while preserving all of your comments and formatting*.
* **Semver Strategy Controls:** Dockwatch reads image tags and can automatically block breaking changes (e.g., blocking a major version bump from `v1.2` to `v2.0`).
* **History & Audit Logging:** Records every single deployment into a SQLite database. It saves a backup of your `docker-compose.yml` before every change, and logs the old digest, new digest, and timestamps.
* **Multi-Host Orchestration:** If you have multiple servers, Dockwatch's WebSocket agent architecture allows a single webhook to instantly dispatch updates securely to all agents without opening inbound firewall ports on the agents.
* **Direct Docker API Rollback:** For users not using Docker Compose, Docker Engine has zero native rollback capabilities. Dockwatch's `healthmon` and rollback engine provide a safety net by monitoring the container for 30s and automatically reverting to the previous digest if it becomes unhealthy.


## Getting Started

### Quick Start (Docker Run)

```bash
docker run -d \
  --name dockwatch \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -p 3010:3010 \
  ghcr.io/soerjadi/dockwatch:latest
```

### Quick Start (Docker Compose)

```yaml
services:
  dockwatch:
    image: ghcr.io/soerjadi/dockwatch:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    ports:
      - "3010:3010"
    restart: unless-stopped
```

Open `http://localhost:3010` for the dashboard.  
SSE stream at `http://localhost:3010/api/events`.

## Container Labels

By default, all containers on the docker socket are watched. To explicitly target or ignore a container, use Docker labels:

```yaml
services:
  myapp:
    image: myrepo/myapp:latest
    labels:
      - "dockwatch.watch=true"
      - "dockwatch.update=auto"
```

| Label | Values | Description |
|---|---|---|
| `dockwatch.watch` | `true` / `false` | Include this container (default: all) |
| `dockwatch.update` | `auto` / `minor` / `patch` / `notify` | Update strategy |
| `dockwatch.health.grace` | duration e.g. `60s` | Override health grace window |
| `dockwatch.compose.update` | `auto` / `notify` | Enable compose-first update path (default: `notify`) |

## Webhook Integrations

This is the preferred way for dockwatch to learn about new images — **zero polling**.  
Your CI/CD pipeline calls dockwatch right after `docker push` completes.

### GitHub Actions

`IMAGE_NAME` must be the image name **without** tag (e.g. `ghcr.io/owner/app`).  
`DOCKER_TAG` must be a real Docker tag the registry actually has (e.g. `main`, `latest`).

```yaml
- name: Notify dockwatch
  env:
    IMAGE_NAME: ghcr.io/${{ github.repository_owner }}/your-app
    DOCKER_TAG: ${{ github.ref_name }}
  run: |
    PAYLOAD=$(jq -n \
      --arg image "$IMAGE_NAME" \
      --arg tag   "$DOCKER_TAG" \
      --arg digest "${{ steps.push.outputs.digest }}" \
      '{image: $image, tag: $tag, digest: $digest, source: "github-actions"}')

    SIG="sha256=$(echo -n "$PAYLOAD" | openssl dgst -sha256 \
      -hmac "${{ secrets.DOCKWATCH_WEBHOOK_SECRET }}" | awk '{print $NF}')"

    curl -sf -X POST https://your-dockwatch-host/webhook/push \
      -H "Content-Type: application/json" \
      -H "X-Dockwatch-Signature: $SIG" \
      -d "$PAYLOAD"
```

### Docker Hub Webhooks
Configure in Docker Hub under: **Repository → Webhooks → Add Webhook URL → `http://your-host:3010/webhook/dockerhub`**

## Configuration

All configuration is via environment variables — no config file needed.

| Variable | Default | Description |
|---|---|---|
| `DOCKWATCH_ADDR` | `:3010` | HTTP listen address |
| `DOCKWATCH_LOG_LEVEL` | `info` | Log verbosity: debug/info/warn/error |
| `DOCKWATCH_REGISTRY_CRON` | `0 0 4 * * *` | Fallback registry poll schedule (6-field cron) |
| `DOCKWATCH_HEALTH_GRACE` | `30s` | How long to watch a container post-update |
| `DOCKWATCH_DRY_RUN` | `false` | Disable all destructive actions |
| `DOCKWATCH_WEBHOOK_SECRET` | `""` | HMAC-SHA256 secret for `/webhook/push` validation |
| `DOCKWATCH_HISTORY_DB` | `/data/dockwatch.db` | SQLite database path for update history |
| `DOCKWATCH_HISTORY_DIR` | `/data/history` | Directory for compose file backups (rollback snapshots) |
| `DOCKWATCH_AGENT_TOKEN` | `""` | Shared token for authenticating remote agents (empty = no auth) |
| `GITHUB_TOKEN` | `""` | GitHub PAT for release note enrichment (raises rate limit) |

## Multi-Host Agent

For managing containers across multiple Docker hosts, dockwatch ships a headless `dockwatch-agent` binary that connects **outbound** to the controller over WebSocket — no inbound firewall ports needed on the agent host.

**Run the agent on a remote host:**

```bash
docker run -d \
  --name dockwatch-agent \
  -e DOCKWATCH_CONTROLLER_URL=wss://dockwatch.example.com/agent/connect \
  -e DOCKWATCH_AGENT_TOKEN=your-shared-secret \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /path/to/compose/projects:/projects \
  ghcr.io/soerjadi/dockwatch:latest /usr/local/bin/agent
```

**Dispatch an update to a specific host:**

```bash
curl -X POST http://dockwatch:3010/api/agents/my-remote-host/update \
  -H "Content-Type: application/json" \
  -d '{"service": "nginx", "image": "nginx:1.25"}'
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
