package status

import (
	"context"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/proxy"
)

func TestServeAndQuery(t *testing.T) {
	// piko targets Linux; Windows rejects the long unix-socket paths under
	// the temp dir. The behavior is exercised on the Linux CI runner.
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets with long paths are unsupported on Windows")
	}
	socket := filepath.Join(t.TempDir(), "s.sock")

	collect := func() Snapshot {
		return Snapshot{
			Version:       "test",
			UptimeSeconds: 42,
			Clients:       proxy.Stats{Clients: 7, Pinned: 2},
			Pool:          pool.Stats{Open: 3, Idle: 1, MaxOpen: 100},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = Serve(ctx, socket, collect, slog.New(slog.DiscardHandler))
	}()

	var snap *Snapshot
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for {
		snap, err = Query(socket)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status query never succeeded: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if snap.Version != "test" || snap.UptimeSeconds != 42 ||
		snap.Clients.Clients != 7 || snap.Clients.Pinned != 2 ||
		snap.Pool.Open != 3 || snap.Pool.MaxOpen != 100 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if snap.Cache != nil {
		t.Fatal("cache should be omitted when nil")
	}
}
