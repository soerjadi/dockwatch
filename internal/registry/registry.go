// Package registry provides a thin client for querying image registries.
//
// Key design: we use HEAD requests on the /v2/<name>/manifests/<tag> endpoint
// to fetch only the digest — no full image pull required. This is the same
// approach WUD uses and is far cheaper than Watchtower's full pull-to-compare.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	dockerHubAuthURL = "https://auth.docker.io/token"
	dockerHubRegistry = "https://registry-1.docker.io"
)

// Client queries a container registry for image metadata.
type Client struct {
	http    *http.Client
}

// New returns a registry Client with sensible timeouts.
func New() *Client {
	return &Client{
		http: &http.Client{Timeout: 15 * time.Second},
	}
}

// HeadDigest returns the content-addressable digest of the given image tag
// without pulling the full image layers. Returns "" on error.
//
// image should be in the form "library/nginx:1.25" or "nginx:latest".
func (c *Client) HeadDigest(ctx context.Context, image string) (string, error) {
	name, tag := splitImageRef(image)
	token, err := c.fetchToken(ctx, name)
	if err != nil {
		return "", fmt.Errorf("auth: %w", err)
	}

	url := fmt.Sprintf("%s/v2/%s/manifests/%s", dockerHubRegistry, name, tag)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("head request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry returned %d for %s", resp.StatusCode, image)
	}

	digest := resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		return "", fmt.Errorf("no Docker-Content-Digest header in response")
	}
	return digest, nil
}

// fetchToken obtains a short-lived Bearer token from Docker Hub's auth service.
func (c *Client) fetchToken(ctx context.Context, image string) (string, error) {
	url := fmt.Sprintf(
		"%s?service=registry.docker.io&scope=repository:%s:pull",
		dockerHubAuthURL, image,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("empty token in registry response")
	}
	return result.Token, nil
}

// splitImageRef splits "nginx:1.25" → ("library/nginx", "1.25").
// Handles official images (no slash), user images, and full registry URLs.
func splitImageRef(image string) (name, tag string) {
	// strip registry host if present
	parts := strings.SplitN(image, "/", 2)
	if len(parts) == 2 && strings.Contains(parts[0], ".") {
		image = parts[1]
	}

	if idx := strings.LastIndex(image, ":"); idx != -1 {
		name = image[:idx]
		tag = image[idx+1:]
	} else {
		name = image
		tag = "latest"
	}

	// official images on Docker Hub live under "library/"
	if !strings.Contains(name, "/") {
		name = "library/" + name
	}
	return
}
