// Package status exposes piko's runtime state over a local unix socket,
// consumed by the `piko status` command. Read-only, JSON, one snapshot per
// connection — no admin interface, in line with piko's config-file-only
// philosophy.
package status

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/proxy"
)

// Snapshot is the state returned to `piko status`.
type Snapshot struct {
	Version       string        `json:"version"`
	UptimeSeconds int64         `json:"uptime_seconds"`
	Clients       proxy.Stats   `json:"clients"`
	Pool          pool.Stats    `json:"pool"`
	Cache         *cache.Report `json:"cache,omitempty"`
}

// Collector assembles a Snapshot on demand.
type Collector func() Snapshot

// Serve answers status requests on a unix socket until ctx is cancelled.
// A stale socket file from a previous run is removed first.
func Serve(ctx context.Context, socket string, collect Collector, log *slog.Logger) error {
	_ = os.Remove(socket)

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "unix", socket)
	if err != nil {
		return err
	}
	log.Info("status socket up", "socket", socket)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = os.Remove(socket)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil // shutdown
		}
		go func() {
			defer conn.Close()
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := json.NewEncoder(conn).Encode(collect()); err != nil {
				log.Debug("status write failed", "error", err)
			}
		}()
	}
}

// Query connects to a running piko instance and fetches its Snapshot.
func Query(socket string) (*Snapshot, error) {
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var snap Snapshot
	if err := json.NewDecoder(conn).Decode(&snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
