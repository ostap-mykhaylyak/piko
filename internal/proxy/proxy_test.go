package proxy

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/firewall"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/rewrite"
)

// fakeCounters tracks what the fake backend observed.
type fakeCounters struct {
	accepted  atomic.Int64
	queries   atomic.Int64
	lastQuery atomic.Value // string

	mu       sync.Mutex
	queryLog []string
}

func (c *fakeCounters) record(query string) {
	c.mu.Lock()
	c.queryLog = append(c.queryLog, query)
	c.mu.Unlock()
}

func (c *fakeCounters) countQueries(substr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, q := range c.queryLog {
		if strings.Contains(q, substr) {
			n++
		}
	}
	return n
}

// fakeHandler answers every SELECT with a single-value resultset, every
// other query with OK, and accepts COM_RESET_CONNECTION (via
// HandleOtherCommand returning nil = OK packet).
type fakeHandler struct{ counters *fakeCounters }

func (fakeHandler) UseDB(string) error { return nil }
func (h fakeHandler) HandleQuery(query string) (*mysql.Result, error) {
	h.counters.queries.Add(1)
	h.counters.lastQuery.Store(query)
	h.counters.record(query)
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
				// Real MySQL advertises autocommit in its status flags.
				conn.SetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
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
	rewriter     *rewrite.Rewriter
	firewall     *firewall.Firewall
	maxClients   int
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
		PingInterval:   config.Duration(opts.pingInterval),
		AcquireTimeout: config.Duration(2 * time.Second),
		Multiplexing:   true,
	}
	p, err := pool.New(config.Backend{
		Address: backendAddr, Username: "piko", Password: "backendpass",
	}, poolCfg, log, opts.dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // free the port for the server

	users := []config.User{{Username: "wordpress", Password: "apppass"}}
	srv := New(Options{
		Listen:   config.Listen{Address: addr, MaxConnections: opts.maxClients},
		Users:    users,
		PoolCfg:  poolCfg,
		Pool:     p,
		Cache:    opts.cache,
		Rewriter: opts.rewriter,
		Firewall: opts.firewall,
		Log:      log,
	})
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

// TestMultiplexingSharesConnections: two concurrently open client sessions
// alternating clean queries must share one backend connection.
func TestMultiplexingSharesConnections(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPiko(t, backendAddr)

	a, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for i := 0; i < 5; i++ {
		if _, err := a.Execute("SELECT 42"); err != nil {
			t.Fatalf("client A round %d: %v", i, err)
		}
		if _, err := b.Execute("SELECT 42"); err != nil {
			t.Fatalf("client B round %d: %v", i, err)
		}
	}

	if got := counters.accepted.Load(); got != 1 {
		t.Errorf("backend saw %d connections for 2 multiplexed sessions, want 1", got)
	}
}

// TestTransactionPinsConnection: a session inside a transaction keeps its
// connection; other sessions get their own.
func TestTransactionPinsConnection(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPiko(t, backendAddr)

	a, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	if _, err := a.Execute("START TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}

	// B arrives while A holds its transaction: needs a second connection.
	b, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}
	if got := counters.accepted.Load(); got != 2 {
		t.Errorf("backend saw %d connections with a pinned transaction, want 2", got)
	}

	// After COMMIT the connection is shareable again.
	if _, err := a.Execute("COMMIT"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := a.Execute("SELECT 42"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Execute("SELECT 42"); err != nil {
			t.Fatal(err)
		}
	}
	if got := counters.accepted.Load(); got != 2 {
		t.Errorf("backend saw %d connections after commit, want still 2", got)
	}
}

// TestSetNamesReplay: tracked SET statements follow the session onto other
// connections instead of pinning it.
func TestSetNamesReplay(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPiko(t, backendAddr)

	a, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// A configures its charset, like wpdb does at connect.
	if _, err := a.Execute("SET NAMES utf8mb4"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}
	// B (no SETs) grabs the shared connection: piko resets it.
	if _, err := b.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}
	// A comes back: its SET NAMES must be replayed on the reused conn.
	if _, err := a.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}

	if got := counters.accepted.Load(); got != 1 {
		t.Errorf("backend saw %d connections, want 1 (SET NAMES must not pin)", got)
	}
	if got := counters.countQueries("SET NAMES utf8mb4"); got != 2 {
		t.Errorf("backend saw SET NAMES %d times, want 2 (original + replay)", got)
	}
}

// TestTempTablePins: CREATE TEMPORARY TABLE ties the session to its
// connection for good.
func TestTempTablePins(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPiko(t, backendAddr)

	a, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.Execute("CREATE TEMPORARY TABLE tmp_report (id INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}

	b, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.Execute("SELECT 42"); err != nil {
		t.Fatal(err)
	}

	if got := counters.accepted.Load(); got != 2 {
		t.Errorf("backend saw %d connections with a temp-table session, want 2", got)
	}
}

// TestFirewallBlock: a blocked query is rejected without reaching the
// backend.
func TestFirewallBlock(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")

	fw, err := firewall.New([]firewall.Rule{
		{Name: "no-bigtable", Match: `(?i)FROM\s+wp_bigtable`},
	})
	if err != nil {
		t.Fatal(err)
	}
	pikoAddr := startPikoWith(t, backendAddr, pikoOpts{firewall: fw})

	conn, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	before := counters.queries.Load()
	if _, err := conn.Execute("SELECT * FROM wp_bigtable"); err == nil {
		t.Fatal("expected the blocked query to error")
	}
	if got := counters.queries.Load(); got != before {
		t.Errorf("blocked query reached the backend (%d queries)", got-before)
	}

	// A non-blocked query still works on the same session.
	if _, err := conn.Execute("SELECT 42"); err != nil {
		t.Fatalf("allowed query failed: %v", err)
	}
}

// TestQueryRewrite: configured rewrites are applied before the query
// reaches the backend.
func TestQueryRewrite(t *testing.T) {
	backendAddr, counters := startFakeMySQL(t, "piko", "backendpass")

	rw, err := rewrite.New([]rewrite.Rule{
		{Name: "no-rand", Match: `(?i)\s*ORDER\s+BY\s+RAND\s*\(\s*\)`, Replace: ""},
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	pikoAddr := startPikoWith(t, backendAddr, pikoOpts{rewriter: rw})

	conn, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Execute("SELECT ID FROM wp_posts ORDER BY RAND() LIMIT 1"); err != nil {
		t.Fatal(err)
	}
	if got := counters.lastQuery.Load(); got != "SELECT ID FROM wp_posts LIMIT 1" {
		t.Fatalf("backend received %q, want the rewritten query", got)
	}
}

// connectRetry keeps trying to connect until the deadline; needed where a
// just-closed connection may still hold its client slot for a moment.
func connectRetry(t *testing.T, addr string) *client.Conn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := client.Connect(addr, "wordpress", "apppass", "wp")
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect never succeeded: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMaxClientConnections: connections beyond the cap are refused.
func TestMaxClientConnections(t *testing.T) {
	backendAddr, _ := startFakeMySQL(t, "piko", "backendpass")
	pikoAddr := startPikoWith(t, backendAddr, pikoOpts{maxClients: 1})

	// Retry: the listener probe inside startPikoWith may still hold the
	// only client slot for an instant.
	a := connectRetry(t, pikoAddr)
	defer a.Close()

	if b, err := client.Connect(pikoAddr, "wordpress", "apppass", "wp"); err == nil {
		b.Close()
		t.Fatal("expected the second connection to be rejected")
	}

	// Closing the first frees a slot.
	a.Close()
	connectRetry(t, pikoAddr).Close()
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
