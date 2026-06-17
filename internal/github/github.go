// Package github provides optional GitHub Releases enrichment for breaking
// change detection. The breaking-change flag itself is derived purely from
// semver; this package adds human-readable release note context.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const apiBase = "https://api.github.com"

// Client queries the GitHub Releases API.
type Client struct {
	http  *http.Client
	token string // optional Bearer token — raises rate limit from 60 to 5000 req/hr
}

// New creates a Client. Pass an empty token to use unauthenticated requests.
func New(token string) *Client {
	return &Client{
		http:  &http.Client{Timeout: 10 * time.Second},
		token: token,
	}
}

// FetchReleaseNotes returns titles and brief bodies for releases between
// fromVersion and toVersion in the repository at repoURL
// (e.g. "https://github.com/owner/repo"). Returns nil on any error so callers
// can treat enrichment as best-effort.
func (c *Client) FetchReleaseNotes(ctx context.Context, repoURL, fromVersion, toVersion string) []string {
	owner, repo, ok := parseGitHubURL(repoURL)
	if !ok {
		return nil
	}

	url := fmt.Sprintf("%s/repos/%s/%s/releases", apiBase, owner, repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var releases []struct {
		TagName string `json:"tag_name"`
		Name    string `json:"name"`
		Body    string `json:"body"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return nil
	}

	var notes []string
	for _, r := range releases {
		if !betweenVersions(r.TagName, fromVersion, toVersion) {
			continue
		}
		title := r.Name
		if title == "" {
			title = r.TagName
		}
		body := r.Body
		if len(body) > 200 {
			body = body[:200] + "…"
		}
		if body != "" {
			notes = append(notes, fmt.Sprintf("%s: %s", title, strings.TrimSpace(body)))
		} else {
			notes = append(notes, title)
		}
	}
	return notes
}

// parseGitHubURL extracts owner and repo from https://github.com/owner/repo.
func parseGitHubURL(rawURL string) (owner, repo string, ok bool) {
	rawURL = strings.TrimSuffix(rawURL, ".git")
	parts := strings.Split(strings.TrimPrefix(rawURL, "https://github.com/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// betweenVersions returns true when tag is strictly after from and at-or-before to.
// Tags are compared by stripping leading "v" and comparing as strings; a full
// semver library is deliberately avoided to keep the dependency footprint small.
func betweenVersions(tag, from, to string) bool {
	t := strings.TrimPrefix(tag, "v")
	f := strings.TrimPrefix(from, "v")
	tt := strings.TrimPrefix(to, "v")
	return t > f && t <= tt
}
