package proxy

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
)

// fakeCounters tracks what the fake backend observed.
type fakeCounters struct {
	accepted atomic.Int64
	queries  atomic.Int64
}

// fakeHandler answers every SELECT with a single-value resultset, every
// other query with OK, and accepts COM_RESET_CONNECTION (via
// HandleOtherCommand returning nil = OK packet).
type fakeHandler struct{ counters *fakeCounters }

func (fakeHandler) UseDB(string) error { return nil }
func (h fakeHandler) HandleQuery(query string) (*mysql.Result, error) {
	h.counters.queries.Add(1)
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
		return &mysql.Result{AffectedRows: 1}, nil
	}
	rs, err := mysql.BuildSimpleTextResultset([]string{"v"}, [][]any{{int64(42)}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}
func (fakeHandler) HandleFieldList(string, string) ([]*mysql.Field, error) { return nil, nil }
func (fakeHandler) HandleStmtPrepare(string) (int, int, any, error)        { return 0, 0, nil, nil }
func (fakeHandler) HandleStmtExecute(any, string, []any) (*mysql.Result, error) {
	return nil, nil
}
func (fakeHandler) HandleStmtClose(any) error             { return nil }
func (fakeHandler) HandleOtherCommand(byte, []byte) error { return nil }

func startFakeMySQL(t *testing.T, user, password string) (string, *fakeCounters) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	srvConf := server.NewServer("8.0.36", mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser(user, password); err != nil {
		t.Fatal(err)
	}

	counters := &fakeCounters{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			counters.accepted.Add(1)
			go func() {
				conn, err := server.NewCustomizedConn(c, srvConf, auth, fakeHandler{counters: counters})
				if err != nil {
					return
				}
				for !conn.Closed() {
					if conn.HandleCommand() != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String(), counters
}

// pikoOpts tweaks the test stack; the zero value is a working default.
type pikoOpts struct {
	pingInterval time.Duration
	dialer       pool.Dialer
	cache        *cache.Cache
}

// startPiko runs the full stack (pool + proxy) against the fake backend.
func startPiko(t *testing.T, backendAddr string) string {
	return startPikoWith(t, backendAddr, pikoOpts{})
}

func startPikoWith(t *testing.T, backendAddr string, opts pikoOpts) string {
	t.Helper()

	if opts.pingInterval == 0 {
		opts.pingInterval = time.Second
	}
	log := slog.New(slog.DiscardHandler)
	poolCfg := config.Pool{
		MaxOpen:        4,
		MaxIdle:        4,
		PingInterval:   opts.pingInterval,
		AcquireTimeout: 2 * time.Second,
	}
	p := pool.New(config.Backend{
		Address: backendAddr, Username: "piko", Password: "backendpass",
	}, poolCfg, log, opts.dialer)
	t.Cleanup(p.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // free the port for the server

	users := []config.User{{Username: "wordpress", Password: "apppass"}}
	srv := New(addr, users, poolCfg, p, opts.cache, log)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx) }()

	// Wait for the listener to come up.
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return addr
		}
		if time.Now().After(deadline) {
			t.Fatalf("piko never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEndToEnd: a MySQL client authenticates against piko, runs a query and
// receives the backend's result; sequential sessions share one backend
// connection.
func TestEndToEnd(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPiko(t, backendAddr)

	for i := 0; i < 3; i++ {
		conn, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
		if err != nil {
			t.Fatalf("session %d: connect: %v", i, err)
		}
		r, err := conn.Execute("SELECT 42")
		if err != nil {
			t.Fatalf("session %d: query: %v", i, err)
		}
		v, err := r.Resultset.GetInt(0, 0)
		if err != nil || v != 42 {
			t.Fatalf("session %d: got %v (err %v), want 42", i, v, err)
		}
		conn.Close()
	}

	// Sessions were sequential: the pool must have reused one connection.
	if got := counters.accepted.Load(); got != 1 {
		t.Errorf("backend saw %d connections, want 1 (pooling broken)", got)
	}
}

// pingCounter wraps a net.Conn counting COM_PING packets sent to the backend.
type pingCounter struct {
	net.Conn
	pings *atomic.Int64
}

func (c pingCounter) Write(b []byte) (int, error) {
	if len(b) == 5 && b[4] == mysql.COM_PING {
		c.pings.Add(1)
	}
	return c.Conn.Write(b)
}

// TestIdleSessionKeepalive reproduces the "MySQL server has gone away"
// scenario: a client (e.g. PHP crunching a CSV) sits idle holding its
// session; piko must ping the attached backend connection in the meantime,
// and the next query must succeed.
func TestIdleSessionKeepalive(t *testing.T) {
	backendAddr, _ := startFakeMySQL(t, "piko", "backendpass")

	var pings atomic.Int64
	dialer := func(ctx context.Context, network, address string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return pingCounter{Conn: conn, pings: &pings}, nil
	}
	pikoAddr := startPikoWith(t, backendAddr, pikoOpts{pingInterval: 50 * time.Millisecond, dialer: dialer})

	// Connecting with a database attaches a backend connection eagerly.
	conn, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Simulate the long idle pause: no commands from the client while the
	// backend connection stays attached to this session.
	deadline := time.Now().Add(3 * time.Second)
	for pings.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected keepalive pings on the attached connection, got %d", pings.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// After the pause the session must still work.
	if _, err := conn.Execute("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("query after idle pause: %v", err)
	}
}

// TestAuthRejected: wrong client credentials must not reach the backend.
func TestAuthRejected(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPiko(t, backendAddr)

	if conn, err := client.Connect(pikoAddr, "wordpress", "wrongpass", ""); err == nil {
		conn.Close()
		t.Fatal("expected authentication to fail")
	}
	if got := counters.accepted.Load(); got != 0 {
		t.Errorf("backend saw %d connections, want 0", got)
	}
}

// TestQueryCache: cacheable reads are served from memory (the backend sees
// them once) and writes invalidate the affected entries.
func TestQueryCache(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")

	cacheCfg := config.Default().Cache
	rules := []cache.Rule{}
	qc := cache.New(cacheCfg, rules, slog.New(slog.DiscardHandler))
	pikoAddr := startPikoWith(t, backendAddr, pikoOpts{cache: qc})

	conn, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	autoload := "SELECT option_name, option_value FROM wp_options WHERE autoload IN ( 'yes', 'on', 'auto-on', 'auto' )"
	transient := "SELECT option_value FROM wp_options WHERE option_name = '_transient_wc_count' LIMIT 1"

	// Baseline: the USE at connect does not issue queries.
	base := counters.queries.Load()

	for i := 0; i < 3; i++ {
		if _, err := conn.Execute(autoload); err != nil {
			t.Fatalf("autoload query %d: %v", i, err)
		}
	}
	if got := counters.queries.Load() - base; got != 1 {
		t.Fatalf("backend saw %d autoload queries, want 1 (cache miss loop)", got)
	}

	for i := 0; i < 3; i++ {
		if _, err := conn.Execute(transient); err != nil {
			t.Fatalf("transient query %d: %v", i, err)
		}
	}
	if got := counters.queries.Load() - base; got != 2 {
		t.Fatalf("backend saw %d queries after transient reads, want 2", got)
	}

	// A write attributed to another option drops the alloptions snapshot
	// but not the transient entry.
	if _, err := conn.Execute("UPDATE wp_options SET option_value = 'x' WHERE option_name = 'blogname'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Execute(autoload); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Execute(transient); err != nil {
		t.Fatal(err)
	}
	// +1 write, +1 autoload refetch; the transient read stays cached.
	if got := counters.queries.Load() - base; got != 4 {
		t.Fatalf("backend saw %d queries after option write, want 4", got)
	}

	// A write on the transient itself drops its entry.
	if _, err := conn.Execute("UPDATE wp_options SET option_value = 'y' WHERE option_name = '_transient_wc_count'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Execute(transient); err != nil {
		t.Fatal(err)
	}
	if got := counters.queries.Load() - base; got != 6 {
		t.Fatalf("backend saw %d queries after transient write, want 6", got)
	}

	// Inside a transaction the cache is bypassed.
	if _, err := conn.Execute("START TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Execute(autoload); err != nil {
		t.Fatal(err)
	}
	if got := counters.queries.Load() - base; got != 8 {
		t.Fatalf("backend saw %d queries inside transaction, want 8", got)
	}
	if _, err := conn.Execute("COMMIT"); err != nil {
		t.Fatal(err)
	}
}
