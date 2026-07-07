// Package proxy implements piko's client-facing MySQL listener.
//
// piko terminates the MySQL protocol on both sides: clients authenticate
// against the users defined in config.yaml, while backend connections are
// owned by piko and shared through the pool. Each client command is executed
// on the session's backend connection and the result relayed back.
package proxy

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/profile"
	"github.com/ostap-mykhaylyak/piko/internal/rewrite"
)

// Options wires the Server's collaborators; Cache, Profiler and Rewriter
// are optional.
type Options struct {
	Listen   config.Listen
	Users    []config.User
	PoolCfg  config.Pool
	Pool     *pool.Pool
	Cache    *cache.Cache
	Profiler *profile.Profiler
	Rewriter *rewrite.Rewriter
	Log      *slog.Logger
}

// Server accepts client connections and serves the MySQL protocol.
type Server struct {
	addr       string
	maxClients int
	pool       *pool.Pool
	cfg        config.Pool
	cache      *cache.Cache      // nil when disabled
	prof       *profile.Profiler // nil when disabled
	rewriter   *rewrite.Rewriter // nil when disabled
	log        *slog.Logger

	srvConf *server.Server
	auth    *server.InMemoryAuthenticationHandler

	wg         sync.WaitGroup
	clients    sync.Map // net.Conn -> struct{}, open client sockets
	numClients atomic.Int64
}

// New creates a Server; call Run to start serving.
func New(o Options) *Server {
	// mysql_native_password keeps compatibility with every PHP/mysqli and
	// mysqlnd version WordPress runs on.
	srvConf := server.NewServer("8.0.36-piko", mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	for _, u := range o.Users {
		// AddUser only fails for unknown auth plugins, which is fixed here.
		_ = auth.AddUser(u.Username, u.Password)
	}

	return &Server{
		addr:       o.Listen.Address,
		maxClients: o.Listen.MaxConnections,
		pool:       o.Pool,
		cfg:        o.PoolCfg,
		cache:      o.Cache,
		prof:       o.Profiler,
		rewriter:   o.Rewriter,
		log:        o.Log,
		srvConf:    srvConf,
		auth:       auth,
	}
}

// Run listens on the configured address until ctx is cancelled, then closes
// client connections and waits for sessions to finish.
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}
	s.log.Info("listening", "address", s.addr)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
		s.clients.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // shutdown requested
			}
			s.log.Error("accept failed", "error", err)
			continue
		}

		if s.maxClients > 0 && s.numClients.Load() >= int64(s.maxClients) {
			s.log.Warn("client connection limit reached, rejecting",
				"client", conn.RemoteAddr(), "max_connections", s.maxClients)
			_ = conn.Close()
			continue
		}

		s.numClients.Add(1)
		s.clients.Store(conn, struct{}{})
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.numClients.Add(-1)
			defer s.clients.Delete(conn)
			s.handle(ctx, conn)
		}()
	}

	s.wg.Wait()
	return nil
}

func (s *Server) handle(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	sess := newSession(ctx, s, s.log.With("client", clientConn.RemoteAddr()))
	defer sess.close()

	// Handshake: authenticates the client and, when it selects a database,
	// triggers UseDB which eagerly attaches a backend connection.
	conn, err := server.NewCustomizedConn(clientConn, s.srvConf, s.auth, sess)
	if err != nil {
		s.log.Warn("client handshake failed",
			"client", clientConn.RemoteAddr(), "error", err)
		return
	}

	sess.log.Debug("session opened", "user", conn.GetUser())
	for !conn.Closed() {
		if err := conn.HandleCommand(); err != nil {
			sess.log.Debug("session ended", "reason", err)
			return
		}
	}
	sess.log.Debug("session closed")
}
