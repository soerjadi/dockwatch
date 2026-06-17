package compose

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ApplyUpdate is the high-level entry point: backs up the compose file,
// patches the image tag, and runs docker compose up -d --no-deps.
// Returns the backup path so the caller can record it in update history.
// Returns ("", nil) in dry-run mode.
func ApplyUpdate(ctx context.Context, info *Info, newTag, histDir string, log *slog.Logger) (string, error) {
	if len(info.ConfigFiles) == 0 {
		return "", fmt.Errorf("compose apply: no config files in compose info")
	}
	configFile := info.ConfigFiles[0]

	backupPath, err := BackupFile(configFile, info.Service, histDir)
	if err != nil {
		return "", err
	}
	if err := UpdateServiceImage(configFile, info.Service, newTag); err != nil {
		return backupPath, err
	}
	if err := UpService(ctx, info.WorkingDir, configFile, info.Service, log); err != nil {
		return backupPath, err
	}
	return backupPath, nil
}

// BackupFile copies configFile to destDir/{service}_{timestamp}.yml before
// modification. Returns the backup path.
func BackupFile(configFile, service, destDir string) (string, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return "", fmt.Errorf("compose backup: read %s: %w", configFile, err)
	}
	name := fmt.Sprintf("%s_%d.yml", service, time.Now().Unix())
	dest := destDir + "/" + name
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		return "", fmt.Errorf("compose backup: write %s: %w", dest, err)
	}
	return dest, nil
}

// UpdateServiceImage rewrites the image tag for service in configFile in-place,
// preserving all comments and formatting using the yaml.v3 Node API.
// newTag is just the tag portion (e.g. "1.25" or "alpine"), not the full ref.
func UpdateServiceImage(configFile, service, newTag string) error {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("compose update: read %s: %w", configFile, err)
	}

	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("compose update: parse yaml: %w", err)
	}
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		if err := patchImageNode(root.Content[0], service, newTag); err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&root); err != nil {
		return fmt.Errorf("compose update: encode yaml: %w", err)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("compose update: close encoder: %w", err)
	}
	return os.WriteFile(configFile, buf.Bytes(), 0o644)
}

// patchImageNode walks the mapping node to find services.<service>.image and
// updates the tag portion of the value.
func patchImageNode(doc *yaml.Node, service, newTag string) error {
	// Find "services" key
	servicesNode := mappingValue(doc, "services")
	if servicesNode == nil {
		return fmt.Errorf("compose update: no 'services' key found")
	}
	// Find the target service block
	svcNode := mappingValue(servicesNode, service)
	if svcNode == nil {
		return fmt.Errorf("compose update: service %q not found", service)
	}
	// Find "image" key within the service block
	imgNode := mappingValue(svcNode, "image")
	if imgNode == nil {
		return fmt.Errorf("compose update: service %q has no 'image' key", service)
	}
	// Replace the tag: keep name, swap tag
	imgNode.Value = replaceTag(imgNode.Value, newTag)
	return nil
}

// mappingValue returns the value node for key in a mapping node, or nil.
func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

// replaceTag swaps the tag in an image reference, preserving the name and
// any digest. e.g. "nginx:alpine" with newTag "1.25" → "nginx:1.25".
func replaceTag(image, newTag string) string {
	// Strip digest suffix first
	digest := ""
	if idx := strings.Index(image, "@"); idx != -1 {
		digest = image[idx:]
		image = image[:idx]
	}
	// Replace tag
	if idx := strings.LastIndex(image, ":"); idx != -1 {
		image = image[:idx+1] + newTag
	} else {
		image = image + ":" + newTag
	}
	return image + digest
}
