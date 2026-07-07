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
	"errors"
	"fmt"
	"log/slog"
	"net"
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

	pooledAt time.Time // when it was released to the pool
	lastPing time.Time // last successful COM_PING or command
}

// Pool hands out backend connections up to a configured cap.
type Pool struct {
	backend config.Backend
	cfg     config.Pool
	dialer  Dialer
	log     *slog.Logger

	// openSem holds one token per open backend connection (leased or idle).
	openSem chan struct{}
	// idle holds parked connections ready for reuse (newest first is not
	// guaranteed; it is a simple buffered channel).
	idle chan *Conn

	resetUnsupported atomic.Bool

	stop   chan struct{}
	closed atomic.Bool
}

// New creates the pool and starts its keepalive loop.
// A nil dialer means plain TCP.
func New(backend config.Backend, cfg config.Pool, log *slog.Logger, dialer Dialer) *Pool {
	if dialer == nil {
		d := &net.Dialer{Timeout: 5 * time.Second}
		dialer = d.DialContext
	}
	p := &Pool{
		backend: backend,
		cfg:     cfg,
		dialer:  dialer,
		log:     log,
		openSem: make(chan struct{}, cfg.MaxOpen),
		idle:    make(chan *Conn, cfg.MaxOpen),
		stop:    make(chan struct{}),
	}
	go p.keepalive()
	return p
}

// Acquire returns a backend connection: a pooled idle one when available,
// otherwise a new one if the cap allows, otherwise it waits until a
// connection frees up or the acquire timeout expires.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
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
	if err := p.reset(c); err != nil {
		p.log.Debug("closing backend connection, reset failed", "error", err)
		p.Discard(c)
		return
	}

	now := time.Now()
	c.pooledAt = now
	c.lastPing = now

	if len(p.idle) >= p.cfg.MaxIdle {
		p.Discard(c)
		return
	}
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
		p.backend.Username, p.backend.Password, "", p.dialer)
	if err != nil {
		return nil, fmt.Errorf("connecting to backend %s: %w", p.backend.Address, err)
	}
	now := time.Now()
	return &Conn{Conn: c, pooledAt: now, lastPing: now}, nil
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

// reset clears session state with COM_RESET_CONNECTION. Servers that do not
// support it (pre-5.7 MySQL) get a ROLLBACK as a best-effort fallback.
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
