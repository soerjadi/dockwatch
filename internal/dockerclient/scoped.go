// Package dockerclient is dockwatch's single, deliberately narrow gateway to
// the Docker daemon.
//
// Security model
// --------------
// The Docker Engine API exposes ~150 endpoints, many of them dangerous
// (exec into containers, mutate the daemon, read arbitrary files via bind
// mounts, etc.). dockwatch only ever needs eight of them. Rather than mount
// the raw socket and trust every call site to behave — or stand up a
// separate socket-proxy that enforces an allow-list at the HTTP layer (which
// can be bypassed by anything that can still reach the real socket) — we wrap
// the SDK client in the Scoped interface below.
//
// The allow-list is enforced by the Go type system, at compile time:
//
//	Full Docker API (~150 endpoints)
//	         |
//	  Scoped (this file)  -- the only surface the rest of dockwatch can see
//	  +-----------------------------------------------------------+
//	  | ListContainers()    GET  /containers/json                 |
//	  | InspectContainer()  GET  /containers/{id}/json            |
//	  | StreamEvents()      GET  /events                          |
//	  | PullImage()         POST /images/create                   |
//	  | StopContainer()     POST /containers/{id}/stop            |
//	  | StartContainer()    POST /containers/{id}/start           |
//	  | RemoveContainer()   DELETE /containers/{id}               |
//	  | CreateContainer()   POST /containers/create               |
//	  +-----------------------------------------------------------+
//	  Everything else -> not on the interface -> unreachable
//
// No part of dockwatch holds a *client.Client; they hold a Scoped. There is
// no method to call exec, attach, commit, or read the daemon config — not
// because policy forbids it, but because the method does not exist. A
// compromised executor or rollback engine cannot widen its own authority
// without someone editing this file and recompiling.
//
// Stop+Start alone cannot change a container's image (they reuse the existing
// container's config), so Remove+Create are included to let the executor and
// rollback engine recreate a container against a new image digest. The
// Recreate helper composes those primitives; it adds no new authority.
package dockerclient

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

// InspectResult is the daemon's full view of a single container. It is an
// alias for the SDK type so consumers can name inspect results without each
// importing the SDK directly.
type InspectResult = types.ContainerJSON

// Event is a minimal, SDK-free projection of a Docker daemon event. Keeping
// our own type means consumers (e.g. the watcher) never import the SDK just to
// read an event, and the daemon's wire types can change without rippling out.
type Event struct {
	Type   string // "container" | "image" | ...
	Action string // "pull" | "die" | "start" | "destroy" | "health_status: unhealthy" | ...
	ID     string // container/image ID (Actor.ID)
	Name   string // container name   (Actor.Attributes["name"])
	Image  string // image reference  (Actor.Attributes["image"])
}

// CreateSpec carries everything CreateContainer needs to recreate a container
// faithfully. Config/HostConfig/Networking come straight from a prior
// InspectContainer call so the new container matches the old one except for
// the image.
type CreateSpec struct {
	Name       string
	Config     *container.Config
	HostConfig *container.HostConfig
	Networking *network.NetworkingConfig
}

// Scoped is the ONLY Docker surface the rest of dockwatch is allowed to touch.
// It lists exactly the eight operations dockwatch needs. Anything not on this
// interface is unreachable from application code — that is the whole point.
//
// Do not add methods here without a corresponding security review: every
// method added widens the blast radius of a compromise.
type Scoped interface {
	// ListContainers returns all containers (running and stopped) so the store
	// can be rebuilt on startup. GET /containers/json
	ListContainers(ctx context.Context) ([]types.Container, error)

	// InspectContainer returns the full config of a single container, used to
	// read health status and to recreate the container faithfully.
	// GET /containers/{id}/json
	InspectContainer(ctx context.Context, id string) (types.ContainerJSON, error)

	// StreamEvents subscribes to the daemon's real-time event stream.
	// GET /events
	StreamEvents(ctx context.Context) (<-chan Event, <-chan error)

	// PullImage pulls an image reference (tag or digest) and blocks until the
	// pull completes. POST /images/create
	PullImage(ctx context.Context, ref string) error

	// StopContainer stops a running container. POST /containers/{id}/stop
	StopContainer(ctx context.Context, id string) error

	// StartContainer starts a stopped container. POST /containers/{id}/start
	StartContainer(ctx context.Context, id string) error

	// RemoveContainer removes a (stopped) container. DELETE /containers/{id}
	RemoveContainer(ctx context.Context, id string) error

	// CreateContainer creates a new container from a spec and returns its ID.
	// POST /containers/create
	CreateContainer(ctx context.Context, spec CreateSpec) (string, error)

	// Close releases the underlying daemon connection.
	Close() error
}

// Client is the production implementation of Scoped, backed by the Docker SDK.
// It is the only thing in the codebase that holds a *client.Client.
type Client struct {
	cli    *client.Client
	dryRun bool
}

// compile-time assertion that *Client satisfies the narrow interface.
var _ Scoped = (*Client)(nil)

// New constructs a scoped client. host may be empty to honour the standard
// DOCKER_HOST / default socket resolution. When dryRun is true, the mutating
// operations (Pull, Stop, Start, Remove, Create) become no-ops while the read
// operations (List, Inspect, StreamEvents) stay live — so dockwatch can
// observe and report without touching anything.
func New(host string, dryRun bool) (*Client, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}
	if host != "" {
		opts = append(opts, client.WithHost(host))
	}
	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("dockerclient: init: %w", err)
	}
	return &Client{cli: cli, dryRun: dryRun}, nil
}

func (c *Client) ListContainers(ctx context.Context) ([]types.Container, error) {
	return c.cli.ContainerList(ctx, container.ListOptions{All: true})
}

func (c *Client) InspectContainer(ctx context.Context, id string) (types.ContainerJSON, error) {
	return c.cli.ContainerInspect(ctx, id)
}

func (c *Client) StreamEvents(ctx context.Context) (<-chan Event, <-chan error) {
	rawCh, errCh := c.cli.Events(ctx, events.ListOptions{})
	out := make(chan Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-rawCh:
				if !ok {
					return
				}
				select {
				case out <- Event{
					Type:   string(msg.Type),
					Action: string(msg.Action),
					ID:     msg.Actor.ID,
					Name:   msg.Actor.Attributes["name"],
					Image:  msg.Actor.Attributes["image"],
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, errCh
}

func (c *Client) PullImage(ctx context.Context, ref string) error {
	if c.dryRun {
		return nil
	}
	rc, err := c.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull %s: %w", ref, err)
	}
	defer rc.Close()
	// The pull is asynchronous on the wire; draining the stream to EOF is how
	// we block until it actually finishes.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return fmt.Errorf("pull %s: drain: %w", ref, err)
	}
	return nil
}

func (c *Client) StopContainer(ctx context.Context, id string) error {
	if c.dryRun {
		return nil
	}
	return c.cli.ContainerStop(ctx, id, container.StopOptions{})
}

func (c *Client) StartContainer(ctx context.Context, id string) error {
	if c.dryRun {
		return nil
	}
	return c.cli.ContainerStart(ctx, id, container.StartOptions{})
}

func (c *Client) RemoveContainer(ctx context.Context, id string) error {
	if c.dryRun {
		return nil
	}
	return c.cli.ContainerRemove(ctx, id, container.RemoveOptions{})
}

func (c *Client) CreateContainer(ctx context.Context, spec CreateSpec) (string, error) {
	if c.dryRun {
		return "dry-run-" + spec.Name, nil
	}
	resp, err := c.cli.ContainerCreate(ctx, spec.Config, spec.HostConfig, spec.Networking, nil, spec.Name)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", spec.Name, err)
	}
	return resp.ID, nil
}

func (c *Client) Close() error { return c.cli.Close() }

// Recreate applies a new image to an existing container by composing the
// primitive operations: inspect (to capture config) -> pull -> stop -> remove
// -> create (same config, new image) -> start. It returns the NEW container
// ID, which differs from the old one.
//
// Recreate takes a Scoped, not a *Client, so it works against the dry-run
// client and against test fakes — and so it provably cannot do anything the
// interface doesn't already allow.
func Recreate(ctx context.Context, c Scoped, containerID, newImageRef string) (string, error) {
	info, err := c.InspectContainer(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("recreate: inspect: %w", err)
	}
	if info.Config == nil {
		return "", fmt.Errorf("recreate: container %s has no config", short(containerID))
	}

	name := strings.TrimPrefix(info.Name, "/")
	newConfig := *info.Config
	newConfig.Image = newImageRef

	var net *network.NetworkingConfig
	if info.NetworkSettings != nil && len(info.NetworkSettings.Networks) > 0 {
		// Preserve the container's network attachments. Note: if the container
		// is attached to more than one network, only the first is applied at
		// create time by the daemon; additional ones would need a connect call
		// (out of scope by design).
		net = &network.NetworkingConfig{EndpointsConfig: info.NetworkSettings.Networks}
	}

	if err := c.PullImage(ctx, newImageRef); err != nil {
		return "", fmt.Errorf("recreate: %w", err)
	}
	if err := c.StopContainer(ctx, containerID); err != nil {
		return "", fmt.Errorf("recreate: stop: %w", err)
	}
	if err := c.RemoveContainer(ctx, containerID); err != nil {
		return "", fmt.Errorf("recreate: remove: %w", err)
	}
	newID, err := c.CreateContainer(ctx, CreateSpec{
		Name:       name,
		Config:     &newConfig,
		HostConfig: info.HostConfig,
		Networking: net,
	})
	if err != nil {
		return "", fmt.Errorf("recreate: %w", err)
	}
	if err := c.StartContainer(ctx, newID); err != nil {
		return newID, fmt.Errorf("recreate: start: %w", err)
	}
	return newID, nil
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
