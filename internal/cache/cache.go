// Package cache implements piko's WordPress-aware in-memory query cache.
//
// Three families of reads are served from RAM:
//
//   - the autoloaded options query (wp_load_alloptions), the single hottest
//     query of every WordPress pageload;
//   - transient reads (_transient_*/_site_transient_* rows in wp_options),
//     WordPress's database-backed cache;
//   - any SELECT matching a rule from the conf.d drop-ins (e.g. the
//     WooCommerce profile).
//
// Correctness comes from write-driven invalidation, with a TTL as safety
// net: every write statement flowing through piko drops the affected
// entries. Writes on the options table are attributed to individual option
// names when the SQL allows it (wpdb's queries do), so a transient update
// does not evict the whole options cache. Unparseable writes flush
// everything: when in doubt, piko prefers a database roundtrip over a stale
// answer.
package cache

import (
	"container/list"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ostap-mykhaylyak/piko/internal/config"
)

// Cache is safe for concurrent use by all sessions.
type Cache struct {
	cfg config.Cache
	log *slog.Logger

	rulesMu sync.RWMutex
	rules   []Rule // swappable at runtime (hot reload)

	optionsTable string
	autoloadRe   *regexp.Regexp
	optionRe     *regexp.Regexp
	optionInRe   *regexp.Regexp

	mu      sync.Mutex
	entries map[string]*entry
	lru     *list.List // *entry, front = most recently used
	byTag   map[string]map[*entry]struct{}
	bytes   int
	sources map[string]*sourceStat // per-source hit/entry counters
	learned map[string]string      // db -> exact alloptions query seen

	// hits counts served-from-cache lookups; misses counts cacheable
	// queries that had to hit the backend. Non-cacheable traffic (most of
	// a WordPress workload) is excluded, so hits/(hits+misses) is a
	// meaningful cache hit ratio.
	hits, misses uint64

	// refetch, when set, is invoked (asynchronously by the warmer) after
	// the alloptions entry of db is invalidated, so it is re-populated
	// before the next pageload pays for it.
	refetch func(db, query string)
}

type entry struct {
	key     string
	result  *mysql.Result
	expires time.Time
	tags    []string
	source  string
	bytes   int
	elem    *list.Element

	// For SQL_CALC_FOUND_ROWS search entries: the paired FOUND_ROWS()
	// count. hasFoundRows is false until captured, and the entry is not
	// served until then (otherwise the client's FOUND_ROWS() would be
	// wrong).
	foundRows    uint64
	hasFoundRows bool
}

// sourceStat aggregates per-source cache statistics (alloptions,
// transients, each conf.d rule).
type sourceStat struct {
	Hits    uint64
	Entries int
	Bytes   int
}

// New builds the cache. rules come from LoadRuleDir and may be empty.
func New(cfg config.Cache, rules []Rule, log *slog.Logger) *Cache {
	prefix := regexp.QuoteMeta(cfg.TablePrefix)
	return &Cache{
		cfg:          cfg,
		rules:        rules,
		log:          log,
		optionsTable: cfg.TablePrefix + "options",
		autoloadRe: regexp.MustCompile(
			`(?i)^SELECT\s+option_name\s*,\s*option_value\s+FROM\s+` + prefix + `options\s+WHERE\s+autoload\b`),
		optionRe: regexp.MustCompile(
			`(?i)^SELECT\s+option_value\s+FROM\s+` + prefix + `options\s+WHERE\s+option_name\s*=\s*'([^'\\]+)'\s+LIMIT\s+1\s*$`),
		// The batch form WordPress actually uses for transients:
		//   SELECT option_name, option_value FROM {p}options
		//   WHERE option_name IN ('_transient_x','_transient_timeout_x')
		optionInRe: regexp.MustCompile(
			`(?i)^SELECT\s+option_name\s*,\s*option_value\s+FROM\s+` + prefix + `options\s+WHERE\s+option_name\s+IN\s*\(([^)]*)\)\s*$`),
		entries: make(map[string]*entry),
		lru:     list.New(),
		byTag:   make(map[string]map[*entry]struct{}),
		sources: make(map[string]*sourceStat),
		learned: make(map[string]string),
	}
}

// SetRules atomically replaces the conf.d rules (hot reload) and flushes
// the cache: existing entries may descend from rules that no longer exist.
func (c *Cache) SetRules(rules []Rule) {
	c.rulesMu.Lock()
	c.rules = rules
	c.rulesMu.Unlock()
	c.Flush("rules reloaded")
}

// SetRefetch installs the warm-up callback (see Warmer).
func (c *Cache) SetRefetch(fn func(db, query string)) {
	c.mu.Lock()
	c.refetch = fn
	c.mu.Unlock()
}

// Tag namespaces: table writes, single options, the alloptions entry.
func tagTable(db, table string) string { return "t\x00" + db + "\x00" + table }
func tagOption(db, name string) string { return "o\x00" + db + "\x00" + name }
func tagAlloptions(db string) string   { return "a\x00" + db }

// Lookup returns a cached result for the query, if any.
func (c *Cache) Lookup(db, query string) (*mysql.Result, bool) {
	key := db + "\x00" + query

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expires) {
		c.removeLocked(e)
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	c.hits++
	if st := c.sources[e.source]; st != nil {
		st.Hits++
	}
	c.log.Debug("cache hit", "query", query)
	return e.result, true
}

// Store caches a client query result. Reaching Store past a Lookup miss
// means the query was cacheable but not cached — a real (cacheable) miss —
// so the hit ratio reflects only cacheable traffic, not the whole workload.
func (c *Cache) Store(db, query string, r *mysql.Result) {
	c.store(db, query, r, true)
}

// Warm caches a result the warmer fetched proactively; it is neither a hit
// nor a miss, so it must not move the counters.
func (c *Cache) Warm(db, query string, r *mysql.Result) {
	c.store(db, query, r, false)
}

func (c *Cache) store(db, query string, r *mysql.Result, countMiss bool) {
	if r == nil || !r.HasResultset() {
		return
	}
	ttl, tags, source, ok := c.cacheable(db, query)
	if !ok {
		return
	}
	size := resultSize(r)
	if size > c.cfg.MaxResultBytes {
		return
	}
	key := db + "\x00" + query

	c.mu.Lock()
	defer c.mu.Unlock()

	if countMiss {
		c.misses++
	}
	c.insertLocked(key, r, ttl, tags, source, size)

	// Remember the exact alloptions query so the warmer can re-populate it
	// after invalidations.
	if source == "alloptions" {
		c.learned[db] = query
	}
}

// insertLocked adds (or replaces) an entry and returns it. Caller holds mu.
func (c *Cache) insertLocked(key string, r *mysql.Result, ttl time.Duration, tags []string, source string, size int) *entry {
	if old, exists := c.entries[key]; exists {
		c.removeLocked(old)
	}
	for c.lru.Len() >= c.cfg.MaxEntries {
		c.removeLocked(c.lru.Back().Value.(*entry))
	}

	e := &entry{
		key:     key,
		result:  r,
		expires: time.Now().Add(ttl),
		tags:    tags,
		source:  source,
		bytes:   size,
	}
	e.elem = c.lru.PushFront(e)
	c.entries[key] = e
	c.bytes += size
	st := c.sources[source]
	if st == nil {
		st = &sourceStat{}
		c.sources[source] = st
	}
	st.Entries++
	st.Bytes += size
	for _, tag := range tags {
		set := c.byTag[tag]
		if set == nil {
			set = make(map[*entry]struct{})
			c.byTag[tag] = set
		}
		set[e] = struct{}{}
	}
	return e
}

// searchKey namespaces search entries so they never collide with normal
// reads of the same text.
func searchKey(db, query string) string { return db + "\x00search\x00" + query }

// LookupSearch returns the cached rows of a SQL_CALC_FOUND_ROWS query and
// its paired FOUND_ROWS() count. ok is true only when both the rows and the
// count are cached — serving rows without the matching count would corrupt
// the client's pagination.
func (c *Cache) LookupSearch(db, query string) (*mysql.Result, uint64, bool) {
	key := searchKey(db, query)

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok || !e.hasFoundRows {
		return nil, 0, false
	}
	if time.Now().After(e.expires) {
		c.removeLocked(e)
		return nil, 0, false
	}
	c.lru.MoveToFront(e.elem)
	c.hits++
	if st := c.sources[e.source]; st != nil {
		st.Hits++
	}
	return e.result, e.foundRows, true
}

// StoreSearch caches the rows of a SQL_CALC_FOUND_ROWS query that matched a
// conf.d rule. The FOUND_ROWS() count is filled in later by PairFoundRows;
// until then the entry is not served.
func (c *Cache) StoreSearch(db, query string, r *mysql.Result) {
	if r == nil || !r.HasResultset() {
		return
	}
	ttl, tags, source, ok := c.cacheableSearch(db, query)
	if !ok {
		return
	}
	size := resultSize(r)
	if size > c.cfg.MaxResultBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.misses++
	c.insertLocked(searchKey(db, query), r, ttl, tags, source, size)
}

// PairFoundRows records the FOUND_ROWS() count for a previously stored
// search query, completing the entry so it can be served.
func (c *Cache) PairFoundRows(db, query string, foundRows uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[searchKey(db, query)]; ok {
		e.foundRows = foundRows
		e.hasFoundRows = true
	}
}

// SearchCacheable reports whether a SQL_CALC_FOUND_ROWS query matches a
// conf.d rule (so the session knows whether to run the pairing dance).
func (c *Cache) SearchCacheable(db, query string) bool {
	_, _, _, ok := c.cacheableSearch(db, query)
	return ok
}

// cacheableSearch decides whether a SQL_CALC_FOUND_ROWS query may be cached
// via a conf.d rule. The built-in options/transient patterns never apply
// here; only user rules do.
func (c *Cache) cacheableSearch(db, query string) (time.Duration, []string, string, bool) {
	if unsafeForSearch(query) {
		return 0, nil, "", false
	}
	c.rulesMu.RLock()
	defer c.rulesMu.RUnlock()
	for i := range c.rules {
		r := &c.rules[i]
		if !r.re.MatchString(query) {
			continue
		}
		ttl := r.TTL
		if ttl <= 0 {
			ttl = c.cfg.DefaultTTL.Std()
		}
		tags := make([]string, 0, len(r.InvalidateOn))
		for _, table := range r.InvalidateOn {
			tags = append(tags, tagTable(db, table))
		}
		return ttl, tags, "rule:" + r.Name, true
	}
	return 0, nil, "", false
}

// cacheable decides whether a SELECT may be cached, how, and under which
// statistics source.
func (c *Cache) cacheable(db, query string) (time.Duration, []string, string, bool) {
	if unsafeForCache(query) {
		return 0, nil, "", false
	}

	if c.cfg.AutoloadOptions && c.autoloadRe.MatchString(query) {
		return c.cfg.DefaultTTL.Std(), []string{
			tagAlloptions(db),
			tagTable(db, c.optionsTable),
		}, "alloptions", true
	}

	if c.cfg.Transients {
		if m := c.optionRe.FindStringSubmatch(query); m != nil {
			if isTransientName(m[1]) {
				return c.cfg.DefaultTTL.Std(), []string{
					tagOption(db, m[1]),
					tagTable(db, c.optionsTable),
				}, "transients", true
			}
			return 0, nil, "", false
		}
		// Batch read of a transient and its timeout companion (the form
		// WordPress actually emits). Cache only when every option is a
		// transient, tagging each so any write to them invalidates it.
		if m := c.optionInRe.FindStringSubmatch(query); m != nil {
			names := extractQuotedList(m[1])
			if len(names) > 0 {
				tags := make([]string, 0, len(names)+1)
				tags = append(tags, tagTable(db, c.optionsTable))
				allTransient := true
				for _, n := range names {
					if !isTransientName(n) {
						allTransient = false
						break
					}
					tags = append(tags, tagOption(db, n))
				}
				if allTransient {
					return c.cfg.DefaultTTL.Std(), tags, "transients", true
				}
			}
		}
	}

	c.rulesMu.RLock()
	defer c.rulesMu.RUnlock()
	for i := range c.rules {
		r := &c.rules[i]
		if !r.re.MatchString(query) {
			continue
		}
		ttl := r.TTL
		if ttl <= 0 {
			ttl = c.cfg.DefaultTTL.Std()
		}
		tags := make([]string, 0, len(r.InvalidateOn))
		for _, table := range r.InvalidateOn {
			tags = append(tags, tagTable(db, table))
		}
		return ttl, tags, "rule:" + r.Name, true
	}
	return 0, nil, "", false
}

// InvalidateWrite drops the entries a write statement may have affected.
func (c *Cache) InvalidateWrite(db, query string) {
	table, ok := extractTable(query)
	if !ok {
		c.Flush("unparseable write statement")
		return
	}

	c.mu.Lock()
	optionsWrite := table == c.optionsTable
	hitAutoload := false
	if optionsWrite {
		if names := extractOptionNames(query); len(names) > 0 {
			// Attributed options write: drop the single-option entries, and
			// the alloptions snapshot only if an autoloaded option can be
			// affected. Transient writes (autoload='off') leave it alone —
			// otherwise WooCommerce's constant transient churn would keep
			// evicting the single hottest cache entry.
			hitAutoload = writeHitsAutoload(names)
			if hitAutoload {
				c.dropTagLocked(tagAlloptions(db))
			}
			for _, name := range names {
				c.dropTagLocked(tagOption(db, name))
			}
		} else {
			// Unattributable options write: drop everything options-tagged,
			// alloptions included.
			hitAutoload = true
			c.dropTagLocked(tagTable(db, table))
		}
	} else {
		c.dropTagLocked(tagTable(db, table))
	}
	refetch := c.refetch
	warm, learned := c.learned[db]
	c.mu.Unlock()

	// Re-warm the alloptions snapshot only when this write actually dropped
	// it, so transient churn no longer triggers pointless refetches.
	if optionsWrite && hitAutoload && learned && refetch != nil {
		refetch(db, warm)
	}
}

// Flush empties the whole cache and asks the warmer to re-populate the
// alloptions snapshots it has learned.
func (c *Cache) Flush(reason string) {
	c.mu.Lock()
	if n := len(c.entries); n > 0 {
		c.log.Debug("cache flushed", "entries", n, "reason", reason)
	}
	c.entries = make(map[string]*entry)
	c.lru.Init()
	c.byTag = make(map[string]map[*entry]struct{})
	c.bytes = 0
	c.sources = make(map[string]*sourceStat)
	refetch := c.refetch
	warm := make(map[string]string, len(c.learned))
	for db, q := range c.learned {
		warm[db] = q
	}
	c.mu.Unlock()

	if refetch != nil {
		for db, q := range warm {
			refetch(db, q)
		}
	}
}

// Report is a snapshot of the cache state for the profiling report.
type Report struct {
	Hits    uint64
	Misses  uint64
	Entries int
	Bytes   int
	Sources map[string]sourceStat
}

// Stats returns hit/miss counters (for logging and future metrics).
func (c *Cache) Stats() (hits, misses uint64, entries int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, len(c.entries)
}

// ReportStats returns the full snapshot, per source included.
func (c *Cache) ReportStats() Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	rep := Report{
		Hits:    c.hits,
		Misses:  c.misses,
		Entries: len(c.entries),
		Bytes:   c.bytes,
		Sources: make(map[string]sourceStat, len(c.sources)),
	}
	for name, st := range c.sources {
		rep.Sources[name] = *st
	}
	return rep
}

// resultSize approximates the memory held by a cached result. Results read
// from the wire accumulate every packet in RawPkg; synthetic ones (tests,
// helpers) are sized from their parts.
func resultSize(r *mysql.Result) int {
	if n := len(r.RawPkg); n > 0 {
		return n
	}
	n := 64
	for _, rd := range r.RowDatas {
		n += len(rd)
	}
	for _, f := range r.Fields {
		if f != nil {
			n += len(f.Data)
		}
	}
	return n
}

func (c *Cache) dropTagLocked(tag string) {
	for e := range c.byTag[tag] {
		c.removeLocked(e)
	}
}

func (c *Cache) removeLocked(e *entry) {
	delete(c.entries, e.key)
	c.lru.Remove(e.elem)
	c.bytes -= e.bytes
	if st := c.sources[e.source]; st != nil {
		st.Entries--
		st.Bytes -= e.bytes
	}
	for _, tag := range e.tags {
		set := c.byTag[tag]
		delete(set, e)
		if len(set) == 0 {
			delete(c.byTag, tag)
		}
	}
}

// String implements fmt.Stringer for startup logging.
func (c *Cache) String() string {
	c.rulesMu.RLock()
	defer c.rulesMu.RUnlock()
	return fmt.Sprintf("cache{prefix=%s rules=%d}", c.cfg.TablePrefix, len(c.rules))
}
