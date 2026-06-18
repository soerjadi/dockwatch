// Package registry provides a thin client for querying image registries.
//
// Design: use HEAD on the /v2/<name>/manifests/<tag> endpoint to fetch only
// the digest — no full image pull required. This is far cheaper than
// Watchtower's pull-to-compare approach.
//
// Supported registries:
//
//   - Docker Hub (registry-1.docker.io, docker.io)
//   - GitHub Container Registry (ghcr.io)
//   - Quay (quay.io)
//   - AWS ECR (*.dkr.ecr.*.amazonaws.com) — requires DOCKWATCH_REGISTRY_USERNAME=AWS
//     and DOCKWATCH_REGISTRY_PASSWORD=$(aws ecr get-login-password)
//   - Google Artifact Registry / GCR — requires DOCKWATCH_REGISTRY_USERNAME=oauth2accesstoken
//     and DOCKWATCH_REGISTRY_PASSWORD=$(gcloud auth print-access-token)
//   - Azure ACR (*.azurecr.io)
//   - Any OCI-compliant private registry (Harbor, Nexus, etc.)
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	dockerHubAuthURL  = "https://auth.docker.io/token"
	dockerHubRegistry = "https://registry-1.docker.io"

	// manifestAccept covers Docker v2, OCI manifests, and manifest list/index
	// types so that multi-arch images and modern OCI images return a digest.
	manifestAccept = "application/vnd.docker.distribution.manifest.v2+json," +
		"application/vnd.oci.image.manifest.v1+json," +
		"application/vnd.docker.distribution.manifest.list.v2+json," +
		"application/vnd.oci.image.index.v1+json"
)

// Config holds per-registry credentials. All fields are optional;
// registries that allow anonymous pulls work without credentials.
type Config struct {
	// Docker Hub credentials — required only for private repositories.
	DockerHubUsername string
	DockerHubPassword string

	// GHCRToken authenticates against ghcr.io.
	// A GitHub PAT with read:packages scope works, as does GITHUB_TOKEN in
	// Actions contexts. Falls back to GITHUB_TOKEN if unset.
	GHCRToken string

	// RegistryUsername and RegistryPassword are used for every other registry:
	// Quay, Harbor, Nexus, generic private, and cloud registries.
	//
	// AWS ECR:  username="AWS"             password=$(aws ecr get-login-password --region <region>)
	// GCR/GAR:  username="oauth2accesstoken" password=$(gcloud auth print-access-token)
	// Azure ACR: username=<client-id>       password=<client-secret or token>
	RegistryUsername string
	RegistryPassword string
}

// credsFor returns the (username, password) to use when authenticating against
// the given registry host.
func (cfg Config) credsFor(host string) (username, password string) {
	switch host {
	case "registry-1.docker.io":
		return cfg.DockerHubUsername, cfg.DockerHubPassword
	case "ghcr.io":
		if cfg.GHCRToken != "" {
			// GHCR's token endpoint accepts any non-empty username.
			// "token" is the conventional placeholder used by most tooling.
			return "token", cfg.GHCRToken
		}
	}
	return cfg.RegistryUsername, cfg.RegistryPassword
}

// Client queries a container registry for image metadata.
type Client struct {
	http *http.Client
	cfg  Config
}

// New returns a registry Client. cfg may be zero-valued for anonymous access.
func New(cfg Config) *Client {
	return &Client{
		http: &http.Client{Timeout: 15 * time.Second},
		cfg:  cfg,
	}
}

// HeadDigest returns the content-addressable digest of the given image tag
// without pulling the full image layers.
//
// image may be in any standard form:
//
//	nginx                                          (Docker Hub official)
//	nginx:1.25                                     (Docker Hub official, pinned)
//	myuser/myapp:v1                                (Docker Hub user repo)
//	ghcr.io/owner/repo:sha-abc                     (GHCR)
//	quay.io/org/image:tag                          (Quay)
//	123456.dkr.ecr.us-east-1.amazonaws.com/r:v2   (AWS ECR)
//	registry.example.com:5000/org/app:v3           (generic private)
func (c *Client) HeadDigest(ctx context.Context, image string) (string, error) {
	host, name, tag := parseImageRef(image)

	if host == "registry-1.docker.io" {
		return c.headDockerHub(ctx, name, tag)
	}
	return c.headGeneric(ctx, host, name, tag)
}

// headDockerHub uses Docker Hub's dedicated token service. Kept separate
// because Docker Hub's auth URL differs from the WWW-Authenticate realm
// returned by most other registries.
func (c *Client) headDockerHub(ctx context.Context, name, tag string) (string, error) {
	token, err := c.fetchDockerHubToken(ctx, name)
	if err != nil {
		return "", fmt.Errorf("dockerhub auth: %w", err)
	}
	manifestURL := fmt.Sprintf("%s/v2/%s/manifests/%s", dockerHubRegistry, name, tag)
	return c.headWithBearer(ctx, manifestURL, token)
}

// headGeneric implements the standard OCI/Docker Registry v2 auth flow:
//  1. HEAD the manifest URL without credentials.
//  2. 200 → return digest (public image, no auth needed).
//  3. 401 → parse WWW-Authenticate, obtain a token, retry with credentials.
//
// Handles GHCR, Quay, Harbor, Nexus, ACR, ECR, and any OCI-compliant registry.
func (c *Client) headGeneric(ctx context.Context, host, name, tag string) (string, error) {
	manifestURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, name, tag)

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, manifestURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", manifestAccept)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("registry head: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return extractDigest(resp, manifestURL)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return "", fmt.Errorf("registry returned %d for %s/%s:%s", resp.StatusCode, host, name, tag)
	}

	wwwAuth := resp.Header.Get("Www-Authenticate")
	if wwwAuth == "" {
		return "", fmt.Errorf("registry returned 401 with no WWW-Authenticate header for %s/%s:%s", host, name, tag)
	}

	username, password := c.cfg.credsFor(host)
	return c.authenticatedHead(ctx, manifestURL, host, name, wwwAuth, username, password)
}

// authenticatedHead resolves credentials from the WWW-Authenticate challenge
// and retries the HEAD request.
func (c *Client) authenticatedHead(ctx context.Context, manifestURL, host, name, wwwAuth, username, password string) (string, error) {
	scheme, _, _ := strings.Cut(wwwAuth, " ")
	switch strings.ToLower(scheme) {
	case "bearer":
		token, err := c.wwwBearerToken(ctx, wwwAuth, username, password)
		if err != nil {
			return "", fmt.Errorf("bearer token for %s/%s: %w", host, name, err)
		}
		return c.headWithBearer(ctx, manifestURL, token)

	case "basic":
		if username == "" && password == "" {
			return "", fmt.Errorf(
				"registry at %s requires Basic auth but no credentials configured "+
					"(set DOCKWATCH_REGISTRY_USERNAME / DOCKWATCH_REGISTRY_PASSWORD)", host)
		}
		return c.headWithBasic(ctx, manifestURL, username, password)

	default:
		return "", fmt.Errorf("unsupported auth scheme %q from %s", scheme, host)
	}
}

// wwwBearerToken follows a Bearer WWW-Authenticate challenge: parses
// realm/service/scope, fetches a token (with optional Basic credentials),
// and returns the Bearer token string.
func (c *Client) wwwBearerToken(ctx context.Context, wwwAuth, username, password string) (string, error) {
	realm, params, err := parseBearerChallenge(wwwAuth)
	if err != nil {
		return "", err
	}

	tokenURL, err := url.Parse(realm)
	if err != nil {
		return "", fmt.Errorf("invalid realm URL %q: %w", realm, err)
	}
	q := tokenURL.Query()
	if svc := params["service"]; svc != "" {
		q.Set("service", svc)
	}
	if scope := params["scope"]; scope != "" {
		q.Set("scope", scope)
	}
	tokenURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return "", err
	}
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request to %s: %w", realm, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint returned %d", resp.StatusCode)
	}

	var result struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"` // alternate field used by some registries
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	token := result.Token
	if token == "" {
		token = result.AccessToken
	}
	if token == "" {
		return "", fmt.Errorf("empty token in response from %s", realm)
	}
	return token, nil
}

// fetchDockerHubToken obtains a pull token from Docker Hub's auth service.
// Credentials are included when configured, enabling private repository access.
func (c *Client) fetchDockerHubToken(ctx context.Context, name string) (string, error) {
	tokenURL := fmt.Sprintf(
		"%s?service=registry.docker.io&scope=repository:%s:pull",
		dockerHubAuthURL, name,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return "", err
	}
	if c.cfg.DockerHubUsername != "" || c.cfg.DockerHubPassword != "" {
		req.SetBasicAuth(c.cfg.DockerHubUsername, c.cfg.DockerHubPassword)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode token: %w", err)
	}
	if result.Token == "" {
		return "", fmt.Errorf("empty token in Docker Hub response")
	}
	return result.Token, nil
}

// headWithBearer issues a HEAD request authenticated with a Bearer token.
func (c *Client) headWithBearer(ctx context.Context, manifestURL, token string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, manifestURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", manifestAccept)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("manifest head: %w", err)
	}
	defer resp.Body.Close()
	return extractDigest(resp, manifestURL)
}

// headWithBasic issues a HEAD request authenticated with Basic credentials.
func (c *Client) headWithBasic(ctx context.Context, manifestURL, username, password string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, manifestURL, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Accept", manifestAccept)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("manifest head: %w", err)
	}
	defer resp.Body.Close()
	return extractDigest(resp, manifestURL)
}

// extractDigest returns the Docker-Content-Digest header from a 200 response.
func extractDigest(resp *http.Response, manifestURL string) (string, error) {
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry returned %d for %s", resp.StatusCode, manifestURL)
	}
	digest := resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		return "", fmt.Errorf("no Docker-Content-Digest header from %s", manifestURL)
	}
	return digest, nil
}

// parseBearerChallenge extracts the realm and parameters from a
// "Bearer realm=...,service=...,scope=..." WWW-Authenticate value.
func parseBearerChallenge(header string) (realm string, params map[string]string, err error) {
	after, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return "", nil, fmt.Errorf("expected Bearer challenge, got %q", header)
	}
	params = make(map[string]string)
	for _, part := range strings.Split(after, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		val = strings.Trim(val, `"`)
		if key == "realm" {
			realm = val
		} else {
			params[key] = val
		}
	}
	if realm == "" {
		return "", nil, fmt.Errorf("no realm in Bearer challenge: %q", header)
	}
	return realm, params, nil
}

// parseImageRef splits an image reference into (registryHost, name, tag).
//
// The first path component is treated as the registry host if it contains a
// dot or colon, or equals "localhost". Otherwise Docker Hub is assumed.
// Docker Hub official images (no slash in name) are namespaced under "library/".
//
// Examples:
//
//	nginx                                  → registry-1.docker.io, library/nginx, latest
//	nginx:1.25                             → registry-1.docker.io, library/nginx, 1.25
//	myuser/myapp:v1                        → registry-1.docker.io, myuser/myapp, v1
//	ghcr.io/owner/repo:sha-abc             → ghcr.io, owner/repo, sha-abc
//	123456.dkr.ecr.us-east-1.amazonaws.com/r:v2 → (ecr host), r, v2
//	registry.example.com:5000/org/app:v3   → registry.example.com:5000, org/app, v3
func parseImageRef(image string) (host, name, tag string) {
	ref := image

	// Strip digest (@sha256:...) — always resolve by tag.
	if idx := strings.Index(ref, "@"); idx != -1 {
		ref = ref[:idx]
	}

	// Separate tag: the last ":" that appears after the last "/".
	lastSlash := strings.LastIndex(ref, "/")
	if idx := strings.LastIndex(ref, ":"); idx > lastSlash {
		tag = ref[idx+1:]
		ref = ref[:idx]
	} else {
		tag = "latest"
	}

	// Check whether the first path component is a registry host.
	first, rest, hasSlash := strings.Cut(ref, "/")
	isHost := strings.ContainsAny(first, ".:") || first == "localhost"

	if hasSlash && isHost {
		host = first
		name = rest
		if host == "docker.io" {
			host = "registry-1.docker.io"
		}
	} else {
		host = "registry-1.docker.io"
		name = ref
	}

	// Docker Hub official images live under "library/".
	if host == "registry-1.docker.io" && !strings.Contains(name, "/") {
		name = "library/" + name
	}

	return
}
