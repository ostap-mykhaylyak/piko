package pool

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/ostap-mykhaylyak/piko/internal/config"
)

// fakeCounters tracks what the fake backend observed.
type fakeCounters struct {
	accepted atomic.Int64
	resets   atomic.Int64 // COM_RESET_CONNECTION commands received
}

// fakeHandler answers every query with a single-value resultset and accepts
// COM_RESET_CONNECTION (via HandleOtherCommand returning nil = OK packet).
type fakeHandler struct{ counters *fakeCounters }

func (fakeHandler) UseDB(string) error { return nil }
func (fakeHandler) HandleQuery(string) (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"v"}, [][]any{{int64(1)}})
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
func (fakeHandler) HandleStmtClose(any) error { return nil }
func (h fakeHandler) HandleOtherCommand(cmd byte, _ []byte) error {
	if cmd == mysql.COM_RESET_CONNECTION {
		h.counters.resets.Add(1)
	}
	return nil
}

// startFakeMySQL runs an in-process MySQL server and returns its address
// plus the observed counters.
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

func testPoolConfig() config.Pool {
	return config.Pool{
		MaxOpen:        4,
		MaxIdle:        4,
		PingInterval:   time.Second,
		IdleTimeout:    0,
		AcquireTimeout: 2 * time.Second,
	}
}

func newTestPool(t *testing.T, addr string, cfg config.Pool, dialer Dialer) *Pool {
	t.Helper()
	p := New(config.Backend{Address: addr, Username: "piko", Password: "secret"},
		cfg, slog.New(slog.DiscardHandler), dialer)
	t.Cleanup(p.Close)
	return p
}

// TestReuse: a released connection must be handed to the next Acquire
// instead of opening a new backend connection.
func TestReuse(t *testing.T) {
	addr, counters := startFakeMySQL(t, "piko", "secret")
	p := newTestPool(t, addr, testPoolConfig(), nil)

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := c1.Execute("SELECT 1"); err != nil {
		t.Fatalf("query on pooled conn: %v", err)
	}
	p.Release(c1)

	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer p.Release(c2)

	if c1 != c2 {
		t.Error("expected the pooled connection to be reused")
	}
	if got := counters.accepted.Load(); got != 1 {
		t.Errorf("backend saw %d connections, want 1", got)
	}
	if _, err := c2.Execute("SELECT 1"); err != nil {
		t.Fatalf("query on reused conn: %v", err)
	}
}

// TestReleaseClean: the multiplexing release path must skip the
// COM_RESET_CONNECTION roundtrip, while the regular release performs it.
func TestReleaseClean(t *testing.T) {
	addr, counters := startFakeMySQL(t, "piko", "secret")
	p := newTestPool(t, addr, testPoolConfig(), nil)

	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.ReleaseClean(c)
	if got := counters.resets.Load(); got != 0 {
		t.Errorf("ReleaseClean sent %d resets, want 0", got)
	}

	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c2 != c {
		t.Error("expected the cleanly released connection to be reused")
	}

	c2.VarSig = "some-session-vars"
	p.Release(c2)
	if got := counters.resets.Load(); got != 1 {
		t.Errorf("Release sent %d resets, want 1", got)
	}

	c3, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release(c3)
	if c3.VarSig != "" || c3.BoundDB != "" {
		t.Errorf("reset must clear bindings, got VarSig=%q BoundDB=%q", c3.VarSig, c3.BoundDB)
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

// TestKeepalive: idle pooled connections must receive periodic COM_PINGs
// and stay usable.
func TestKeepalive(t *testing.T) {
	addr, _ := startFakeMySQL(t, "piko", "secret")

	var pings atomic.Int64
	dialer := func(ctx context.Context, network, address string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return pingCounter{Conn: conn, pings: &pings}, nil
	}

	cfg := testPoolConfig()
	cfg.PingInterval = 50 * time.Millisecond
	p := newTestPool(t, addr, cfg, dialer)

	c, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Release(c)

	deadline := time.Now().Add(3 * time.Second)
	for pings.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected keepalive pings, got %d", pings.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The connection must still be usable after being kept alive.
	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release(c2)
	if _, err := c2.Execute("SELECT 1"); err != nil {
		t.Fatalf("query after keepalive: %v", err)
	}
}

// TestCircuitBreaker: consecutive dial failures open the circuit (fail
// fast), and the probe closes it when the backend comes back.
func TestCircuitBreaker(t *testing.T) {
	// Reserve an address, then close it: dials fail with refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := testPoolConfig()
	cfg.AcquireTimeout = 500 * time.Millisecond
	cfg.Breaker = config.Breaker{Failures: 3, ProbeInterval: 50 * time.Millisecond}
	p := newTestPool(t, addr, cfg, nil)

	// Enough failures to trip the breaker.
	for i := 0; i < 3; i++ {
		if _, err := p.Acquire(context.Background()); err == nil {
			t.Fatal("expected acquire to fail against a dead backend")
		}
	}

	// Now it fails fast without dialing.
	start := time.Now()
	_, err = p.Acquire(context.Background())
	if !errors.Is(err, ErrBackendDown) {
		t.Fatalf("expected ErrBackendDown, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("circuit-open acquire took %v, want immediate", elapsed)
	}

	// Backend comes back on the same address: the probe closes the circuit.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not rebind %s: %v", addr, err)
	}
	defer ln2.Close()
	startFakeMySQLOn(t, ln2, "piko", "secret")

	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := p.Acquire(context.Background())
		if err == nil {
			p.Release(c)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("circuit never closed after recovery: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// startFakeMySQLOn serves the fake MySQL protocol on an existing listener.
func startFakeMySQLOn(t *testing.T, ln net.Listener, user, password string) {
	t.Helper()

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
}

// TestMaxOpen: the pool must cap backend connections and time out waiters.
func TestMaxOpen(t *testing.T) {
	addr, counters := startFakeMySQL(t, "piko", "secret")

	cfg := testPoolConfig()
	cfg.MaxOpen = 1
	cfg.MaxIdle = 1
	cfg.AcquireTimeout = 100 * time.Millisecond
	p := newTestPool(t, addr, cfg, nil)

	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := p.Acquire(context.Background()); err == nil {
		t.Fatal("expected acquire to time out with the pool exhausted")
	}

	p.Release(c1)
	c2, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	p.Release(c2)

	if got := counters.accepted.Load(); got != 1 {
		t.Errorf("backend saw %d connections, want 1", got)
	}
}
