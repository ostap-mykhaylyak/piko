package proxy

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/profile"
)

// maxTrackedTxWrites caps the write statements remembered inside one
// transaction for re-invalidation at COMMIT; beyond that the whole cache is
// flushed on commit instead.
const maxTrackedTxWrites = 128

// session implements server.Handler for one client connection.
//
// A backend connection is acquired lazily (or at handshake time when the
// client selects a database) and stays attached for the whole client session
// so transactions and session state behave exactly as with a direct
// connection. While the client is idle — e.g. PHP parsing a large CSV before
// an INSERT — a pinger goroutine keeps the attached backend connection alive
// with COM_PING so MySQL never drops it ("server has gone away"). If the
// backend connection is lost anyway, the next command transparently attaches
// a fresh one from the pool.
type session struct {
	ctx   context.Context
	pool  *pool.Pool
	cfg   config.Pool
	cache *cache.Cache      // nil when disabled
	prof  *profile.Profiler // nil when disabled
	log   *slog.Logger

	mu      sync.Mutex // guards conn, db, lastUse against the pinger
	conn    *pool.Conn
	db      string
	lastUse time.Time

	// Transaction and cache-safety state (guarded by mu as well).
	inTx        bool
	txWrites    []string // writes seen inside the open transaction
	txOverflow  bool
	cacheUnsafe bool // session did something piko cannot track (autocommit...)

	stopPing chan struct{}
	pingDone chan struct{}
}

func newSession(ctx context.Context, p *pool.Pool, cfg config.Pool, qc *cache.Cache, prof *profile.Profiler, log *slog.Logger) *session {
	s := &session{
		ctx:      ctx,
		pool:     p,
		cfg:      cfg,
		cache:    qc,
		prof:     prof,
		log:      log,
		lastUse:  time.Now(),
		stopPing: make(chan struct{}),
		pingDone: make(chan struct{}),
	}
	go s.pinger()
	return s
}

// close releases the session's backend connection back to the pool.
func (s *session) close() {
	close(s.stopPing)
	<-s.pingDone

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.pool.Release(s.conn)
		s.conn = nil
	}
}

// backend returns the attached connection, acquiring one if needed.
// Must be called with s.mu held.
func (s *session) backend() (*pool.Conn, error) {
	if s.conn != nil {
		return s.conn, nil
	}

	c, err := s.pool.Acquire(s.ctx)
	if err != nil {
		return nil, err
	}
	// Bind the connection to the client's database. Always done on a fresh
	// attach: a pooled connection may carry the database of a previous
	// session.
	if s.db != "" {
		if err := c.UseDB(s.db); err != nil {
			s.discardOrRelease(c, err)
			return nil, err
		}
	}
	s.conn = c
	return c, nil
}

// finish records activity and handles connection-level failures: on a
// network error the backend connection is dropped so the next command gets
// a fresh one. MySQL-level errors (bad query, duplicate key...) leave the
// connection attached.
// Must be called with s.mu held.
func (s *session) finish(err error) {
	s.lastUse = time.Now()
	if err == nil || s.conn == nil {
		return
	}
	if isConnError(err) {
		s.log.Warn("backend connection lost, will reattach on next command", "error", err)
		s.pool.Discard(s.conn)
		s.conn = nil
	}
}

func (s *session) discardOrRelease(c *pool.Conn, err error) {
	if isConnError(err) {
		s.pool.Discard(c)
	} else {
		s.pool.Release(c)
	}
}

// isConnError distinguishes broken connections from server-side errors:
// MySQL replied = the connection is fine.
func isConnError(err error) bool {
	var myErr *mysql.MyError
	return !errors.As(err, &myErr)
}

// pinger keeps the attached backend connection alive while the client is
// idle, sending COM_PING every ping_interval.
func (s *session) pinger() {
	defer close(s.pingDone)
	ticker := time.NewTicker(s.cfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopPing:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		if s.conn != nil && time.Since(s.lastUse) >= s.cfg.PingInterval {
			if err := s.conn.Ping(); err != nil {
				s.log.Warn("keepalive ping failed, dropping backend connection", "error", err)
				s.pool.Discard(s.conn)
				s.conn = nil
			} else {
				s.lastUse = time.Now()
			}
		}
		s.mu.Unlock()
	}
}

// --- server.Handler implementation ---

// UseDB handles COM_INIT_DB and the database selected during the handshake.
func (s *session) UseDB(dbName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.db = dbName
	if s.conn == nil {
		// backend() binds the database on a fresh attach.
		_, err := s.backend()
		return err
	}
	err := s.conn.UseDB(dbName)
	s.finish(err)
	return err
}

// HandleQuery handles COM_QUERY, serving cacheable reads from memory.
func (s *session) HandleQuery(query string) (*mysql.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kind := cache.KindOther
	if s.cacheActive() {
		kind = cache.Classify(query)
		// Inside a transaction reads must see the session's own writes,
		// so the cache is bypassed entirely.
		if kind == cache.KindSelect && !s.inTx {
			if r, ok := s.cache.Lookup(s.db, query); ok {
				s.lastUse = time.Now()
				s.profile(query, 0, r, true, nil)
				return r, nil
			}
		}
	}

	c, err := s.backend()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	r, err := c.Execute(query)
	s.finish(err)
	s.profile(query, time.Since(start), r, false, err)
	if err != nil {
		return r, err
	}

	if s.cacheActive() {
		s.observe(kind, query, r)
	}
	return r, nil
}

// profile forwards one execution to the profiler, when enabled.
// Must be called with s.mu held.
func (s *session) profile(query string, dur time.Duration, r *mysql.Result, cached bool, err error) {
	if s.prof == nil {
		return
	}
	var rows uint64
	if r != nil {
		if r.HasResultset() {
			rows = uint64(len(r.Values))
		} else {
			rows = r.AffectedRows
		}
	}
	s.prof.Observe(s.db, query, dur, rows, cached, err)
}

// cacheActive reports whether this session may use the query cache.
// Must be called with s.mu held.
func (s *session) cacheActive() bool {
	return s.cache != nil && !s.cacheUnsafe
}

// observe updates cache and transaction state after a successful statement.
// Must be called with s.mu held.
func (s *session) observe(kind cache.Kind, query string, r *mysql.Result) {
	switch kind {
	case cache.KindSelect:
		if !s.inTx {
			s.cache.Store(s.db, query, r)
		}
	case cache.KindWrite:
		// Invalidate immediately so other sessions stop reading soon-stale
		// entries; inside a transaction the write is also remembered and
		// re-invalidated at COMMIT, because entries may be re-populated
		// with pre-commit data in the meantime.
		s.cache.InvalidateWrite(s.db, query)
		if s.inTx {
			if len(s.txWrites) >= maxTrackedTxWrites {
				s.txOverflow = true
			} else {
				s.txWrites = append(s.txWrites, query)
			}
		}
	case cache.KindBegin:
		s.inTx = true
	case cache.KindCommit:
		s.endTx(true)
	case cache.KindRollback:
		s.endTx(false)
	case cache.KindUnsafe:
		s.log.Debug("query cache disabled for this session", "query", query)
		s.cacheUnsafe = true
	case cache.KindOther:
		// No cache impact.
	}
}

// endTx closes the transaction bookkeeping; on commit the recorded writes
// are re-invalidated. Must be called with s.mu held.
func (s *session) endTx(commit bool) {
	if commit {
		if s.txOverflow {
			s.cache.Flush("transaction with too many writes committed")
		} else {
			for _, q := range s.txWrites {
				s.cache.InvalidateWrite(s.db, q)
			}
		}
	}
	s.inTx = false
	s.txWrites = nil
	s.txOverflow = false
}

// HandleFieldList handles COM_FIELD_LIST (used by old clients).
func (s *session) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c, err := s.backend()
	if err != nil {
		return nil, err
	}
	fields, err := c.FieldList(table, fieldWildcard)
	s.finish(err)
	return fields, err
}

// HandleStmtPrepare handles COM_STMT_PREPARE by preparing on the backend.
func (s *session) HandleStmtPrepare(query string) (int, int, any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c, err := s.backend()
	if err != nil {
		return 0, 0, nil, err
	}
	stmt, err := c.Prepare(query)
	s.finish(err)
	if err != nil {
		return 0, 0, nil, err
	}
	return stmt.ParamNum(), stmt.ColumnNum(), stmt, nil
}

// HandleStmtExecute handles COM_STMT_EXECUTE.
func (s *session) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	stmt, ok := context.(*client.Stmt)
	if !ok {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_STMT_HANDLER, "unknown prepared statement")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	r, err := stmt.Execute(args...)
	s.finish(err)
	s.profile(query, time.Since(start), r, false, err)
	if err == nil && s.cacheActive() {
		// Prepared reads are never cached, but prepared writes still have
		// to invalidate what they touch.
		if kind := cache.Classify(query); kind != cache.KindSelect {
			s.observe(kind, query, r)
		}
	}
	return r, err
}

// HandleStmtClose handles COM_STMT_CLOSE.
func (s *session) HandleStmtClose(context any) error {
	stmt, ok := context.(*client.Stmt)
	if !ok {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	err := stmt.Close()
	s.finish(err)
	return err
}

// HandleOtherCommand rejects commands piko does not support yet.
func (s *session) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR,
		"command not supported by piko")
}
