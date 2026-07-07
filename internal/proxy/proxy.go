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

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/profile"
)

// Server accepts client connections and serves the MySQL protocol.
type Server struct {
	addr  string
	pool  *pool.Pool
	cfg   config.Pool
	cache *cache.Cache      // nil when disabled
	prof  *profile.Profiler // nil when disabled
	log   *slog.Logger

	srvConf *server.Server
	auth    *server.InMemoryAuthenticationHandler

	wg      sync.WaitGroup
	clients sync.Map // net.Conn -> struct{}, open client sockets
}

// New creates a Server; call Run to start serving. qc and prof may be nil.
func New(addr string, users []config.User, poolCfg config.Pool, p *pool.Pool, qc *cache.Cache, prof *profile.Profiler, log *slog.Logger) *Server {
	// mysql_native_password keeps compatibility with every PHP/mysqli and
	// mysqlnd version WordPress runs on.
	srvConf := server.NewServer("8.0.36-piko", mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	for _, u := range users {
		// AddUser only fails for unknown auth plugins, which is fixed here.
		_ = auth.AddUser(u.Username, u.Password)
	}

	return &Server{
		addr:    addr,
		pool:    p,
		cfg:     poolCfg,
		cache:   qc,
		prof:    prof,
		log:     log,
		srvConf: srvConf,
		auth:    auth,
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

		s.clients.Store(conn, struct{}{})
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.clients.Delete(conn)
			s.handle(ctx, conn)
		}()
	}

	s.wg.Wait()
	return nil
}

func (s *Server) handle(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	sess := newSession(ctx, s.pool, s.cfg, s.cache, s.prof, s.log.With("client", clientConn.RemoteAddr()))
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
