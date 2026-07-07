package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValid(t *testing.T) {
	path := writeConfig(t, `
listen:
  address: "127.0.0.1:3307"
backend:
  address: "10.0.0.10:3306"
  username: "piko"
  password: "secret"
users:
  - username: "wordpress"
    password: "apppass"
pool:
  max_open: 50
  ping_interval: 10s
log:
  level: debug
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen.Address != "127.0.0.1:3307" {
		t.Errorf("listen address = %q", cfg.Listen.Address)
	}
	if cfg.Backend.Username != "piko" {
		t.Errorf("backend username = %q", cfg.Backend.Username)
	}
	if len(cfg.Users) != 1 || cfg.Users[0].Username != "wordpress" {
		t.Errorf("users = %+v", cfg.Users)
	}
	if cfg.Pool.MaxOpen != 50 {
		t.Errorf("pool.max_open = %d, want 50", cfg.Pool.MaxOpen)
	}
	if cfg.Pool.PingInterval != 10*time.Second {
		t.Errorf("pool.ping_interval = %v, want 10s", cfg.Pool.PingInterval)
	}
	// Defaults survive partial configs.
	if cfg.Pool.MaxIdle != 10 {
		t.Errorf("pool.max_idle default = %d, want 10", cfg.Pool.MaxIdle)
	}
	if cfg.Pool.AcquireTimeout != 5*time.Second {
		t.Errorf("pool.acquire_timeout default = %v, want 5s", cfg.Pool.AcquireTimeout)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("log format default = %q, want text", cfg.Log.Format)
	}
	if cfg.Log.Path != "/var/log/piko" {
		t.Errorf("log path default = %q, want /var/log/piko", cfg.Log.Path)
	}
}

// TestLoadUsersFallback: without a users section, clients authenticate
// with the backend credentials.
func TestLoadUsersFallback(t *testing.T) {
	path := writeConfig(t, `
backend:
  address: "10.0.0.10:3306"
  username: "wordpress"
  password: "secret"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []User{{Username: "wordpress", Password: "secret"}}
	if len(cfg.Users) != 1 || cfg.Users[0] != want[0] {
		t.Errorf("users = %+v, want %+v", cfg.Users, want)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `
backend:
  address: "10.0.0.10:3306"
  username: "piko"
users:
  - username: "wordpress"
tipo: "sconosciuto"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestLoadMissingBackend(t *testing.T) {
	path := writeConfig(t, `
listen:
  address: "0.0.0.0:3306"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for missing backend")
	}
}

// valid returns a minimal valid config to mutate in validation tests.
func valid() Config {
	cfg := Default()
	cfg.Backend.Address = "10.0.0.10:3306"
	cfg.Backend.Username = "piko"
	cfg.Users = []User{{Username: "wordpress", Password: "x"}}
	return cfg
}

func TestValidateBadValues(t *testing.T) {
	cases := map[string]func(*Config){
		"listen address":     func(c *Config) { c.Listen.Address = "no-port" },
		"backend address":    func(c *Config) { c.Backend.Address = "also-no-port" },
		"backend username":   func(c *Config) { c.Backend.Username = "" },
		"no users":           func(c *Config) { c.Users = nil },
		"empty user":         func(c *Config) { c.Users = []User{{Password: "x"}} },
		"max_open zero":      func(c *Config) { c.Pool.MaxOpen = 0 },
		"max_idle too big":   func(c *Config) { c.Pool.MaxIdle = c.Pool.MaxOpen + 1 },
		"ping_interval zero": func(c *Config) { c.Pool.PingInterval = 0 },
		"acquire zero":       func(c *Config) { c.Pool.AcquireTimeout = 0 },
		"log level":          func(c *Config) { c.Log.Level = "verbose" },
		"log path empty":     func(c *Config) { c.Log.Path = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
