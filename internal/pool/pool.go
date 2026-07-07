// Package pool manages piko's connections to the MySQL backend.
//
// Backend connections are authenticated once with piko's own credentials
// and reused across client sessions: when a client disconnects, its backend
// connection is reset (COM_RESET_CONNECTION) and parked in the pool for the
// next client instead of being closed. A keepalive loop sends COM_PING to
// idle pooled connections so MySQL's wait_timeout never closes them.
package pool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ostap-mykhaylyak/piko/internal/config"
)

// Dialer opens the raw TCP connection to the backend; overridable in tests.
type Dialer = client.Dialer

// Conn is a pooled backend connection.
type Conn struct {
	*client.Conn

	// BoundDB is the database the connection is currently bound to and
	// VarSig identifies the session variables applied to it (see the proxy
	// package). Both are maintained by the session layer to skip redundant
	// COM_INIT_DB/SET roundtrips when connections hop between sessions.
	BoundDB string
	VarSig  string

	// ThreadID is MySQL's connection id, used by KILL QUERY.
	ThreadID uint32

	pooledAt time.Time // when it was released to the pool
	lastPing time.Time // last successful COM_PING or command
}

// Pool hands out backend connections up to a configured cap.
type Pool struct {
	backend config.Backend
	cfg     config.Pool
	dialer  Dialer
	tlsConf *tls.Config // nil = plaintext backend connections
	log     *slog.Logger

	// openSem holds one token per open backend connection (leased or idle).
	openSem chan struct{}
	// idle holds parked connections ready for reuse (newest first is not
	// guaranteed; it is a simple buffered channel).
	idle chan *Conn

	resetUnsupported atomic.Bool

	// Circuit breaker: after cfg.Breaker.Failures consecutive dial
	// failures Acquire fails fast until a background probe reaches the
	// backend again.
	breakerFails atomic.Int32
	breakerOpen  atomic.Bool

	stop   chan struct{}
	closed atomic.Bool
}

// ErrBackendDown is returned by Acquire while the circuit is open.
var ErrBackendDown = errors.New("backend unavailable (circuit breaker open)")

// New creates the pool and starts its keepalive loop.
// A nil dialer means plain TCP.
func New(backend config.Backend, cfg config.Pool, log *slog.Logger, dialer Dialer) (*Pool, error) {
	if dialer == nil {
		d := &net.Dialer{Timeout: 5 * time.Second}
		dialer = d.DialContext
	}
	tlsConf, err := backendTLS(backend)
	if err != nil {
		return nil, err
	}
	p := &Pool{
		backend: backend,
		cfg:     cfg,
		dialer:  dialer,
		tlsConf: tlsConf,
		log:     log,
		openSem: make(chan struct{}, cfg.MaxOpen),
		idle:    make(chan *Conn, cfg.MaxOpen),
		stop:    make(chan struct{}),
	}
	go p.keepalive()
	return p, nil
}

// backendTLS builds the TLS configuration for backend connections.
func backendTLS(backend config.Backend) (*tls.Config, error) {
	if !backend.TLS.Enabled {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(backend.Address)
	if err != nil {
		return nil, fmt.Errorf("backend.address: %w", err)
	}
	conf := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: backend.TLS.SkipVerify,
	}
	if backend.TLS.CA != "" {
		pem, err := os.ReadFile(backend.TLS.CA)
		if err != nil {
			return nil, fmt.Errorf("reading backend.tls.ca: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("backend.tls.ca %s: no certificates found", backend.TLS.CA)
		}
		conf.RootCAs = roots
	}
	return conf, nil
}

// Acquire returns a backend connection: a pooled idle one when available,
// otherwise a new one if the cap allows, otherwise it waits until a
// connection frees up or the acquire timeout expires.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	// Fail fast while the backend is down: without this, every PHP worker
	// would pile up waiting for its own timeout.
	if p.breakerOpen.Load() {
		return nil, ErrBackendDown
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.AcquireTimeout)
	defer cancel()

	for {
		// Fast path: reuse an idle connection.
		select {
		case c := <-p.idle:
			if p.revive(c) {
				return c, nil
			}
			continue // dead connection dropped, try again
		default:
		}

		select {
		case c := <-p.idle:
			if p.revive(c) {
				return c, nil
			}
			continue
		case p.openSem <- struct{}{}:
			c, err := p.dial(ctx)
			if err != nil {
				<-p.openSem
				return nil, err
			}
			return c, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for a backend connection: %w", ctx.Err())
		}
	}
}

// Release parks a healthy connection for reuse. The connection is reset
// first so no session state (transactions, variables, temp tables) leaks
// to the next client; if the reset fails the connection is closed instead.
func (p *Pool) Release(c *Conn) {
	if p.closed.Load() {
		p.Discard(c)
		return
	}
	if err := p.ResetConn(c); err != nil {
		p.log.Debug("closing backend connection, reset failed", "error", err)
		p.Discard(c)
		return
	}

	if len(p.idle) >= p.cfg.MaxIdle {
		p.Discard(c)
		return
	}
	p.park(c)
}

// ReleaseClean parks a connection whose session state is known clean (the
// multiplexing path releases between queries): no reset roundtrip, and no
// MaxIdle trimming so bursts of concurrently released connections are not
// closed just to be re-dialed a moment later — idle_timeout shrinks the
// pool over time instead.
func (p *Pool) ReleaseClean(c *Conn) {
	if p.closed.Load() {
		p.Discard(c)
		return
	}
	p.park(c)
}

// park puts the connection in the idle buffer.
func (p *Pool) park(c *Conn) {
	now := time.Now()
	c.pooledAt = now
	c.lastPing = now
	select {
	case p.idle <- c:
	default:
		p.Discard(c)
	}
}

// Discard closes a connection and frees its slot in the pool.
func (p *Pool) Discard(c *Conn) {
	_ = c.Conn.Close()
	<-p.openSem
}

// Close shuts down the keepalive loop and closes all idle connections.
// Leased connections are closed by their sessions.
func (p *Pool) Close() {
	if p.closed.Swap(true) {
		return
	}
	close(p.stop)
	for {
		select {
		case c := <-p.idle:
			_ = c.Conn.Close()
			<-p.openSem
		default:
			return
		}
	}
}

func (p *Pool) dial(ctx context.Context) (*Conn, error) {
	c, err := client.ConnectWithDialer(ctx, "tcp", p.backend.Address,
		p.backend.Username, p.backend.Password, "", p.dialer, p.dialOptions()...)
	if err != nil {
		p.recordDialFailure()
		return nil, fmt.Errorf("connecting to backend %s: %w", p.backend.Address, err)
	}
	p.breakerFails.Store(0)
	now := time.Now()
	return &Conn{Conn: c, ThreadID: c.GetConnectionID(), pooledAt: now, lastPing: now}, nil
}

func (p *Pool) dialOptions() []client.Option {
	if p.tlsConf == nil {
		return nil
	}
	return []client.Option{func(c *client.Conn) error {
		c.SetTLSConfig(p.tlsConf)
		return nil
	}}
}

// KillQuery interrupts a running backend query (KILL QUERY leaves the
// connection alive) using a dedicated throwaway connection, so it works
// even when the pool is saturated by stuck queries.
func (p *Pool) KillQuery(threadID uint32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := client.ConnectWithDialer(ctx, "tcp", p.backend.Address,
		p.backend.Username, p.backend.Password, "", p.dialer, p.dialOptions()...)
	if err != nil {
		return fmt.Errorf("connecting to kill query: %w", err)
	}
	defer c.Close()

	_, err = c.Execute(fmt.Sprintf("KILL QUERY %d", threadID))
	return err
}

// Stats is a snapshot of the pool state.
type Stats struct {
	Open        int  `json:"open"`
	Idle        int  `json:"idle"`
	MaxOpen     int  `json:"max_open"`
	BreakerOpen bool `json:"breaker_open"`
}

// Stat returns the current pool state.
func (p *Pool) Stat() Stats {
	return Stats{
		Open:        len(p.openSem),
		Idle:        len(p.idle),
		MaxOpen:     p.cfg.MaxOpen,
		BreakerOpen: p.breakerOpen.Load(),
	}
}

// recordDialFailure counts consecutive dial failures and opens the circuit
// when the configured threshold is reached.
func (p *Pool) recordDialFailure() {
	threshold := p.cfg.Breaker.Failures
	if threshold <= 0 {
		return
	}
	if p.breakerFails.Add(1) < int32(threshold) {
		return
	}
	if p.breakerOpen.Swap(true) {
		return // already open, probe running
	}
	p.log.Error("backend unreachable, circuit breaker open: failing fast",
		"backend", p.backend.Address, "failures", threshold)
	go p.probe()
}

// probe retries the backend until it answers, then closes the circuit.
func (p *Pool) probe() {
	ticker := time.NewTicker(p.cfg.Breaker.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}

		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Breaker.ProbeInterval)
		c, err := client.ConnectWithDialer(ctx, "tcp", p.backend.Address,
			p.backend.Username, p.backend.Password, "", p.dialer, p.dialOptions()...)
		cancel()
		if err != nil {
			p.log.Debug("backend probe failed", "error", err)
			continue
		}
		_ = c.Close()

		p.breakerFails.Store(0)
		p.breakerOpen.Store(false)
		p.log.Info("backend recovered, circuit breaker closed",
			"backend", p.backend.Address)
		return
	}
}

// revive validates a connection popped from the pool. Connections pinged
// recently by the keepalive loop are trusted; stale ones get a fresh ping.
func (p *Pool) revive(c *Conn) bool {
	if time.Since(c.lastPing) < 2*p.cfg.PingInterval {
		return true
	}
	if err := c.Ping(); err != nil {
		p.log.Debug("dropping dead pooled connection", "error", err)
		p.Discard(c)
		return false
	}
	c.lastPing = time.Now()
	return true
}

// ResetConn clears session state with COM_RESET_CONNECTION: transactions
// rolled back, locks and temp tables released, session variables back to
// defaults. Servers that do not support it (pre-5.7 MySQL) get a ROLLBACK
// as a best-effort fallback. The bindings are cleared conservatively.
func (p *Pool) ResetConn(c *Conn) error {
	if err := p.reset(c); err != nil {
		return err
	}
	// The ROLLBACK fallback cannot clear session variables: a connection
	// that had any applied is not safe to recycle.
	if p.resetUnsupported.Load() && c.VarSig != "" {
		return fmt.Errorf("backend lacks COM_RESET_CONNECTION, cannot clear session variables")
	}
	// Variables are gone for sure; the current database should survive a
	// reset, but treating it as unknown costs at most one COM_INIT_DB later
	// and removes any doubt.
	c.VarSig = ""
	c.BoundDB = ""
	return nil
}

// reset sends COM_RESET_CONNECTION (or the ROLLBACK fallback).
func (p *Pool) reset(c *Conn) error {
	if p.resetUnsupported.Load() {
		return c.Rollback()
	}

	c.ResetSequence()
	if err := c.WritePacket([]byte{0x01, 0x00, 0x00, 0x00, mysql.COM_RESET_CONNECTION}); err != nil {
		return err
	}
	if _, err := c.ReadOKPacket(); err != nil {
		var myErr *mysql.MyError
		if errors.As(err, &myErr) {
			// Server answered but does not know the command: remember it
			// and fall back to ROLLBACK from now on.
			p.resetUnsupported.Store(true)
			p.log.Warn("backend does not support COM_RESET_CONNECTION, falling back to ROLLBACK")
			return c.Rollback()
		}
		return err
	}
	return nil
}

// keepalive pings idle pooled connections so MySQL never closes them for
// inactivity, and closes connections idle beyond idle_timeout.
func (p *Pool) keepalive() {
	ticker := time.NewTicker(p.cfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}

		for n := len(p.idle); n > 0; n-- {
			var c *Conn
			select {
			case c = <-p.idle:
			default:
				n = 0
				continue
			}

			if p.cfg.IdleTimeout > 0 && time.Since(c.pooledAt) > p.cfg.IdleTimeout {
				p.log.Debug("closing pooled connection, idle timeout reached")
				p.Discard(c)
				continue
			}
			if time.Since(c.lastPing) >= p.cfg.PingInterval {
				if err := c.Ping(); err != nil {
					p.log.Warn("pooled connection lost, dropping it", "error", err)
					p.Discard(c)
					continue
				}
				c.lastPing = time.Now()
			}

			select {
			case p.idle <- c:
			default:
				p.Discard(c)
			}
		}
	}
}
