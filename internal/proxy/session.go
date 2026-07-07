package proxy

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/firewall"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/profile"
	"github.com/ostap-mykhaylyak/piko/internal/rewrite"
)

// maxTrackedTxWrites caps the write statements remembered inside one
// transaction for re-invalidation at COMMIT; beyond that the whole cache is
// flushed on commit instead.
const maxTrackedTxWrites = 128

// maxTrackedSets caps the SET statements replayed on connection reuse;
// sessions setting more than this get pinned instead.
const maxTrackedSets = 20

// session implements server.Handler for one client connection.
//
// With multiplexing enabled (the default), the backend connection is
// returned to the pool after every statement that leaves no session state
// behind, so many client sessions share few backend connections. Sessions
// holding state — open transactions, temporary tables, locks, prepared
// statements, user variables — keep their connection attached (pinned)
// exactly like a direct connection would behave. Tracked SET statements
// (SET NAMES and friends) are replayed when the session lands on a
// different connection.
//
// While a client idles holding an attached connection — e.g. PHP parsing a
// large CSV in the middle of a transaction — a pinger goroutine keeps that
// connection alive with COM_PING so MySQL never drops it ("server has gone
// away"). If the backend connection is lost anyway, the next command
// transparently attaches a fresh one from the pool.
type session struct {
	ctx      context.Context
	srv      *Server
	pool     *pool.Pool
	cfg      config.Pool
	cache    *cache.Cache       // nil when disabled
	prof     *profile.Profiler  // nil when disabled
	rewriter *rewrite.Rewriter  // nil when disabled
	firewall *firewall.Firewall // nil when disabled
	log      *slog.Logger

	mu      sync.Mutex // guards conn, db, lastUse against the pinger
	conn    *pool.Conn
	db      string
	lastUse time.Time

	// Transaction and cache-safety state (guarded by mu as well).
	inTx        bool
	txWrites    []string // writes seen inside the open transaction
	txOverflow  bool
	cacheUnsafe bool // session did something piko cannot track (autocommit...)

	// Multiplexing state (guarded by mu as well).
	mux       bool     // per-query release enabled
	pinned    bool     // session permanently tied to its connection
	holdNext  bool     // keep the connection for one more statement
	openStmts int      // prepared statements alive on the connection
	setStmts  []string // tracked SETs replayed on connection reuse
	varSig    string   // signature of setStmts

	// SQL_CALC_FOUND_ROWS / FOUND_ROWS() pairing (guarded by mu).
	calcPending    string // search query executed on the backend, awaiting its FOUND_ROWS()
	foundRows      uint64 // count to serve for the next FOUND_ROWS() from cache
	foundRowsKnown bool   // whether foundRows is armed

	stopPing chan struct{}
	pingDone chan struct{}
}

func newSession(ctx context.Context, srv *Server, log *slog.Logger) *session {
	s := &session{
		ctx:      ctx,
		srv:      srv,
		pool:     srv.pool,
		cfg:      srv.cfg,
		cache:    srv.cache,
		prof:     srv.prof,
		rewriter: srv.rewriter,
		firewall: srv.firewall,
		log:      log,
		mux:      srv.cfg.Multiplexing,
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
	if s.pinned {
		s.srv.numPinned.Add(-1)
	}
}

// backend returns the attached connection, acquiring and preparing one if
// needed. Must be called with s.mu held.
func (s *session) backend() (*pool.Conn, error) {
	if s.conn != nil {
		return s.conn, nil
	}

	// One retry: a pooled connection can fail preparation for connection
	// reasons (died while parked); a freshly dialed one gets a second shot.
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		c, err := s.pool.Acquire(s.ctx)
		if err != nil {
			return nil, err
		}
		if err := s.prepareConn(c); err != nil {
			lastErr = err
			if isConnError(err) {
				s.pool.Discard(c)
				continue
			}
			// MySQL-level error (e.g. unknown database): the connection is
			// fine, the request is not.
			s.pool.Release(c)
			return nil, err
		}
		s.conn = c
		return c, nil
	}
	return nil, lastErr
}

// prepareConn aligns a pooled connection with this session's environment:
// clears foreign session variables, binds the database and replays the
// session's tracked SETs. In the steady state (every WordPress session
// issues the same SET NAMES) signatures match and no roundtrip happens.
// Must be called with s.mu held.
func (s *session) prepareConn(c *pool.Conn) error {
	if c.VarSig != s.varSig && c.VarSig != "" {
		// The connection carries another session's variables.
		if err := s.pool.ResetConn(c); err != nil {
			return err
		}
	}
	if s.db != "" && c.BoundDB != s.db {
		if err := c.UseDB(s.db); err != nil {
			return err
		}
		c.BoundDB = s.db
	}
	if c.VarSig != s.varSig {
		for _, stmt := range s.setStmts {
			if _, err := c.Execute(stmt); err != nil {
				return err
			}
		}
		c.VarSig = s.varSig
	}
	return nil
}

// maybeRelease returns the connection to the pool when the session state
// allows it. Must be called with s.mu held, after the statement's result
// has been fully read (results are buffered, so the client reply does not
// need the connection anymore).
func (s *session) maybeRelease() {
	if !s.mux || s.conn == nil {
		return
	}
	if s.pinned || s.inTx || s.openStmts > 0 {
		return
	}
	// Status flags from the last OK/EOF packet: catches implicit
	// transactions (autocommit=0) even if keyword tracking missed them.
	if s.conn.IsInTransaction() || !s.conn.IsAutoCommit() {
		return
	}
	if s.holdNext {
		s.holdNext = false
		return
	}
	c := s.conn
	s.conn = nil
	s.pool.ReleaseClean(c)
}

// pin ties the session to its connection for its whole lifetime.
// Must be called with s.mu held.
func (s *session) pin(reason string) {
	if s.pinned {
		return
	}
	s.pinned = true
	s.srv.numPinned.Add(1)
	s.log.Debug("session pinned to its backend connection", "reason", reason)
}

// trackSafety updates transaction/pinning state after every successful
// statement, independent of the cache. Must be called with s.mu held.
func (s *session) trackSafety(kind cache.Kind, query string, r *mysql.Result) {
	switch kind {
	case cache.KindBegin:
		s.inTx = true
	case cache.KindCommit, cache.KindRollback:
		s.inTx = false
	case cache.KindUnsafe:
		s.pin("untracked session command (autocommit/XA)")
	}
	if r != nil && r.Status&mysql.SERVER_STATUS_IN_TRANS != 0 {
		s.inTx = true
	}

	if pinDetectRe.MatchString(query) {
		s.pin("temporary table, lock or transaction setting")
	}
	// User variables persist on the connection with values piko cannot
	// reproduce. Checked on the fingerprint so literals ('a@b.com') do not
	// false-positive.
	if strings.Contains(query, "@") && userVarRe.MatchString(profile.Fingerprint(query)) {
		s.pin("user-defined variables")
	}

	// The companion statement (SELECT FOUND_ROWS(), SELECT
	// LAST_INSERT_ID()...) must run on this same connection.
	if holdDetectRe.MatchString(query) || (r != nil && r.InsertId > 0) {
		s.holdNext = true
	}
}

// trackSet handles a successful SET statement: replayable ones join the
// session environment, untrackable ones pin. Must be called with s.mu held.
func (s *session) trackSet(query string, act setAction) {
	switch act {
	case setTrack:
		for _, existing := range s.setStmts {
			if existing == query {
				return // repeated identical SET (wpdb re-sends SET NAMES)
			}
		}
		if len(s.setStmts) >= maxTrackedSets {
			s.pin("too many session settings to replay")
			return
		}
		s.setStmts = append(s.setStmts, query)
		s.varSig = varSignature(s.setStmts)
		if s.conn != nil {
			s.conn.VarSig = s.varSig
		}
	case setPin:
		s.pin("untrackable SET statement")
	case setNone, setIgnore:
	}
}

// handleSearch serves or caches a SQL_CALC_FOUND_ROWS listing query,
// keeping its rows and pagination count together. Must be called with mu.
func (s *session) handleSearch(query string) (*mysql.Result, error) {
	if r, foundRows, ok := s.cache.LookupSearch(s.db, query); ok {
		// Rows from cache; arm the paired FOUND_ROWS() answer so the
		// client's pagination stays correct without touching the backend.
		s.foundRows = foundRows
		s.foundRowsKnown = true
		s.calcPending = ""
		s.lastUse = time.Now()
		s.profile(query, 0, r, true, nil)
		return r, nil
	}

	c, err := s.backend()
	if err != nil {
		return nil, err
	}
	stopWatchdog := s.queryWatchdog(c, query)
	start := time.Now()
	r, err := c.Execute(query)
	stopWatchdog()
	s.finish(err)
	s.profile(query, time.Since(start), r, false, err)
	if err != nil {
		return r, err
	}
	s.cache.StoreSearch(s.db, query, r)
	// The next FOUND_ROWS() belongs to this query; keep the same backend
	// connection so its count is correct, and do not release here.
	s.calcPending = query
	s.foundRowsKnown = false
	return r, nil
}

// serveFoundRows answers SELECT FOUND_ROWS(): from the pairing cache when
// the preceding search was a cache hit, otherwise from the backend, in
// which case the count is captured to complete the pending search entry.
// Must be called with mu.
func (s *session) serveFoundRows(query string) (*mysql.Result, error) {
	if s.foundRowsKnown {
		r := foundRowsResult(s.foundRows)
		s.foundRowsKnown = false
		s.calcPending = ""
		s.lastUse = time.Now()
		s.profile(query, 0, r, true, nil)
		s.maybeRelease()
		return r, nil
	}

	c, err := s.backend()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	r, err := c.Execute(query)
	s.finish(err)
	s.profile(query, time.Since(start), r, false, err)
	if err == nil && s.calcPending != "" && r != nil && r.Resultset != nil && len(r.Values) > 0 {
		if n, e := r.GetUint(0, 0); e == nil {
			s.cache.PairFoundRows(s.db, s.calcPending, n)
		}
	}
	s.calcPending = ""
	s.maybeRelease()
	return r, err
}

// foundRowsResult builds a synthetic FOUND_ROWS() result set.
func foundRowsResult(n uint64) *mysql.Result {
	rs, err := mysql.BuildSimpleTextResultset([]string{"FOUND_ROWS()"}, [][]any{{int64(n)}})
	if err != nil {
		return &mysql.Result{}
	}
	return mysql.NewResult(rs)
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
	ticker := time.NewTicker(s.cfg.PingInterval.Std())
	defer ticker.Stop()

	for {
		select {
		case <-s.stopPing:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		if s.conn != nil && time.Since(s.lastUse) >= s.cfg.PingInterval.Std() {
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
		// backend() binds the database on a fresh attach; attaching eagerly
		// also validates the database name during the handshake.
		if _, err := s.backend(); err != nil {
			return err
		}
		s.maybeRelease()
		return nil
	}
	err := s.conn.UseDB(dbName)
	s.finish(err)
	if err == nil && s.conn != nil {
		s.conn.BoundDB = dbName
		s.maybeRelease()
	}
	return err
}

// HandleQuery handles COM_QUERY, serving cacheable reads from memory.
func (s *session) HandleQuery(query string) (*mysql.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rewriter != nil {
		if rewritten, applied := s.rewriter.Apply(query); len(applied) > 0 {
			s.log.Debug("query rewritten", "rules", strings.Join(applied, ","), "query", rewritten)
			query = rewritten
		}
	}

	if s.firewall != nil {
		if rule, blocked := s.firewall.Check(query); blocked {
			s.log.Warn("query blocked", "rule", rule, "query", query)
			return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR,
				"query blocked by piko firewall rule '"+rule+"'")
		}
	}

	// SQL_CALC_FOUND_ROWS / FOUND_ROWS() pairing: cache product listings
	// together with their pagination count. Only outside transactions,
	// where reads must see the session's own writes.
	if s.cacheActive() && !s.inTx {
		if cache.IsFoundRowsQuery(query) {
			return s.serveFoundRows(query)
		}
		if cache.HasCalcFoundRows(query) && s.cache.SearchCacheable(s.db, query) {
			return s.handleSearch(query)
		}
	}
	// Any other statement breaks a pending calc/FOUND_ROWS() sequence.
	s.calcPending = ""
	s.foundRowsKnown = false

	kind := cache.Classify(query)

	// Inside a transaction reads must see the session's own writes,
	// so the cache is bypassed entirely.
	if s.cacheActive() && kind == cache.KindSelect && !s.inTx {
		if r, ok := s.cache.Lookup(s.db, query); ok {
			s.lastUse = time.Now()
			s.profile(query, 0, r, true, nil)
			// Served from memory: an attached clean connection is not
			// needed for this session right now.
			s.maybeRelease()
			return r, nil
		}
	}

	c, err := s.backend()
	if err != nil {
		return nil, err
	}
	stopWatchdog := s.queryWatchdog(c, query)
	start := time.Now()
	r, err := c.Execute(query)
	stopWatchdog()
	s.finish(err)
	s.profile(query, time.Since(start), r, false, err)
	if err != nil {
		return r, err
	}

	s.trackSet(query, classifySet(query))
	s.trackSafety(kind, query, r)
	if s.cacheActive() {
		s.observe(kind, query, r)
	}
	s.maybeRelease()
	return r, nil
}

// queryWatchdog kills the backend query when it exceeds max_query_time.
// KILL QUERY interrupts the statement, not the connection: the client
// receives a MySQL error and the session survives.
func (s *session) queryWatchdog(c *pool.Conn, query string) func() {
	if s.cfg.MaxQueryTime <= 0 || c.ThreadID == 0 {
		return func() {}
	}
	threadID := c.ThreadID
	limit := s.cfg.MaxQueryTime.Std()
	timer := time.AfterFunc(limit, func() {
		s.log.Warn("query exceeded max_query_time, killing it",
			"max_query_time", limit, "query", query)
		if err := s.pool.KillQuery(threadID); err != nil {
			s.log.Error("failed to kill runaway query", "error", err)
		}
	})
	return func() { timer.Stop() }
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
	if err == nil {
		s.maybeRelease()
	}
	return fields, err
}

// HandleStmtPrepare handles COM_STMT_PREPARE by preparing on the backend.
// Statement handles live on one specific connection: the session keeps it
// attached until every statement is closed.
func (s *session) HandleStmtPrepare(query string) (int, int, any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.firewall != nil {
		if rule, blocked := s.firewall.Check(query); blocked {
			s.log.Warn("prepared statement blocked", "rule", rule, "query", query)
			return 0, 0, nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR,
				"query blocked by piko firewall rule '"+rule+"'")
		}
	}

	c, err := s.backend()
	if err != nil {
		return 0, 0, nil, err
	}
	stmt, err := c.Prepare(query)
	s.finish(err)
	if err != nil {
		return 0, 0, nil, err
	}
	s.openStmts++
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

	stopWatchdog := func() {}
	if s.conn != nil {
		stopWatchdog = s.queryWatchdog(s.conn, query)
	}
	start := time.Now()
	r, err := stmt.Execute(args...)
	stopWatchdog()
	s.finish(err)
	s.profile(query, time.Since(start), r, false, err)
	if err == nil {
		kind := cache.Classify(query)
		s.trackSafety(kind, query, r)
		if s.cacheActive() && kind != cache.KindSelect {
			// Prepared reads are never cached, but prepared writes still
			// have to invalidate what they touch.
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
	if s.openStmts > 0 {
		s.openStmts--
	}
	if err == nil {
		s.maybeRelease()
	}
	return err
}

// HandleOtherCommand rejects commands piko does not support yet.
func (s *session) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR,
		"command not supported by piko")
}
