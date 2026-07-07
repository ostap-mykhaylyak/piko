package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ostap-mykhaylyak/piko/internal/config"
)

// TestDefaultConfigValid parses the embedded template exactly as `piko
// --init` would write it, catching typos and unit-less durations before
// they reach a user's server.
func TestDefaultConfigValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, defaultConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err != nil {
		t.Fatalf("embedded default config does not load: %v", err)
	}
}

// TestServiceUnit checks the embedded systemd unit runs the installed binary
// and that --init can point ExecStart at a custom config path.
func TestServiceUnit(t *testing.T) {
	if !strings.Contains(serviceUnit, "ExecStart="+binaryPath) {
		t.Errorf("service unit does not ExecStart %s:\n%s", binaryPath, serviceUnit)
	}
	if !strings.Contains(serviceUnit, defaultConfigPath) {
		t.Fatalf("service unit lacks the default config path to substitute")
	}
	custom := strings.ReplaceAll(serviceUnit, defaultConfigPath, "/opt/piko/config.yaml")
	if strings.Contains(custom, defaultConfigPath) || !strings.Contains(custom, "/opt/piko/config.yaml") {
		t.Error("config path substitution failed")
	}
}
