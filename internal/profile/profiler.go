// Package profile implements piko's query profiling: per-query statistics,
// slow query logging, periodic reports and index suggestions, all emitted
// through the standard log so no extra tooling is needed.
package profile

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
)

// maxDigests bounds the aggregation map; queries beyond the cap are only
// counted in the "other" bucket of the report.
const maxDigests = 5000

// queryStat aggregates executions of one normalized query.
type queryStat struct {
	digest string
	db     string
	sample string // a real execution, literals included, used for EXPLAIN

	calls  uint64
	cached uint64
	errors uint64
	rows   uint64
	total  time.Duration
	max    time.Duration
}

// Profiler collects statistics from sessions and reports periodically.
type Profiler struct {
	cfg  config.Profiling
	pool *pool.Pool
	log  *slog.Logger

	mu       sync.Mutex
	stats    map[string]*queryStat
	dbs      map[string]struct{}
	overflow uint64

	advisor *advisor
	cache   *cache.Cache // optional, adds cache statistics to the report
}

// SetCache attaches the query cache so reports include its statistics.
func (p *Profiler) SetCache(c *cache.Cache) { p.cache = c }

// New creates the profiler; call Run to start the reporting loop.
func New(cfg config.Profiling, p *pool.Pool, log *slog.Logger) *Profiler {
	return &Profiler{
		cfg:     cfg,
		pool:    p,
		log:     log.With("component", "profiling"),
		stats:   make(map[string]*queryStat),
		dbs:     make(map[string]struct{}),
		advisor: newAdvisor(log.With("component", "profiling")),
	}
}

// Observe records one executed statement. cached marks queries served from
// piko's cache without touching MySQL.
func (p *Profiler) Observe(db, query string, dur time.Duration, rows uint64, cached bool, err error) {
	digest := Fingerprint(query)

	p.mu.Lock()
	st, ok := p.stats[digest]
	if !ok {
		if len(p.stats) >= maxDigests {
			p.overflow++
			p.mu.Unlock()
			return
		}
		st = &queryStat{digest: digest, db: db, sample: query}
		p.stats[digest] = st
	}
	st.calls++
	st.rows += rows
	st.total += dur
	if dur > st.max {
		st.max = dur
		st.sample = query // keep the worst execution for EXPLAIN
	}
	if cached {
		st.cached++
	}
	if err != nil {
		st.errors++
	}
	p.dbs[db] = struct{}{}
	p.mu.Unlock()

	if !cached && p.cfg.SlowQuery > 0 && dur >= p.cfg.SlowQuery {
		p.log.Warn("slow query",
			"duration", dur.Round(time.Millisecond),
			"db", db,
			"rows", rows,
			"query", query)
	}
}

// Run emits a report every report_interval until ctx is cancelled, plus a
// final one on shutdown.
func (p *Profiler) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.ReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.report(context.Background())
			return
		case <-ticker.C:
			p.report(ctx)
		}
	}
}

// report logs the interval's statistics and resets them; when enabled it
// also runs the index advisor on the heaviest queries.
func (p *Profiler) report(ctx context.Context) {
	p.mu.Lock()
	stats := make([]*queryStat, 0, len(p.stats))
	for _, st := range p.stats {
		stats = append(stats, st)
	}
	dbs := make([]string, 0, len(p.dbs))
	for db := range p.dbs {
		dbs = append(dbs, db)
	}
	overflow := p.overflow
	p.stats = make(map[string]*queryStat)
	p.overflow = 0
	p.mu.Unlock()

	if len(stats) == 0 {
		return
	}

	sort.Slice(stats, func(i, j int) bool { return stats[i].total > stats[j].total })

	var calls, cached, errors uint64
	var total time.Duration
	for _, st := range stats {
		calls += st.calls
		cached += st.cached
		errors += st.errors
		total += st.total
	}
	p.log.Info("query report",
		"interval", p.cfg.ReportInterval,
		"queries", calls,
		"distinct", len(stats),
		"errors", errors,
		"cache_hits", cached,
		"cache_hit_ratio", ratio(cached, calls),
		"db_time", total.Round(time.Millisecond),
		"untracked", overflow)

	top := stats
	if len(top) > p.cfg.TopQueries {
		top = top[:p.cfg.TopQueries]
	}
	for i, st := range top {
		p.log.Info("top query",
			"rank", i+1,
			"calls", st.calls,
			"total", st.total.Round(time.Millisecond),
			"avg", (st.total / time.Duration(max(st.calls, 1))).Round(time.Microsecond),
			"max", st.max.Round(time.Millisecond),
			"avg_rows", st.rows/max(st.calls, 1),
			"cache_hit_ratio", ratio(st.cached, st.calls),
			"db", st.db,
			"query", st.digest)
	}

	if p.cache != nil {
		rep := p.cache.ReportStats()
		p.log.Info("cache report",
			"hits", rep.Hits,
			"misses", rep.Misses,
			"hit_ratio", ratio(rep.Hits, rep.Hits+rep.Misses),
			"entries", rep.Entries,
			"memory_bytes", rep.Bytes)
		for name, src := range rep.Sources {
			p.log.Info("cache source",
				"source", name,
				"hits", src.Hits,
				"entries", src.Entries,
				"memory_bytes", src.Bytes)
		}
	}

	if p.cfg.SuggestRewrites {
		p.suggestRewrites(stats)
	}
	if p.cfg.SuggestIndexes {
		p.suggest(ctx, top, dbs)
	}
}

// suggest borrows a pooled connection to EXPLAIN the heaviest queries and
// inspect the schema for duplicate or unused indexes.
func (p *Profiler) suggest(ctx context.Context, top []*queryStat, dbs []string) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		p.log.Debug("index advisor skipped, no backend connection", "error", err)
		return
	}
	healthy := true
	defer func() {
		if healthy {
			p.pool.Release(conn)
		} else {
			p.pool.Discard(conn)
		}
	}()

	for _, st := range top {
		if !isExplainable(st.sample) || st.db == "" {
			continue
		}
		if err := p.advisor.explainQuery(conn, st); err != nil {
			p.log.Debug("EXPLAIN failed", "query", st.digest, "error", err)
			if isConnError(err) {
				healthy = false
				return
			}
		}
	}

	for _, db := range dbs {
		if db == "" {
			continue
		}
		if err := p.advisor.reviewSchema(conn, db); err != nil {
			p.log.Debug("schema review failed", "db", db, "error", err)
			if isConnError(err) {
				healthy = false
				return
			}
		}
	}
}

// isExplainable reports whether EXPLAIN can run on the statement without
// executing it (SELECT/UPDATE/DELETE are all plan-only in modern MySQL).
func isExplainable(query string) bool {
	q := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(q, "SELECT") ||
		strings.HasPrefix(q, "UPDATE") ||
		strings.HasPrefix(q, "DELETE")
}

func ratio(part, whole uint64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(int(float64(part)/float64(whole)*1000)) / 1000
}
