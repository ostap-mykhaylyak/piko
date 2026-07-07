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
	cfg   config.Cache
	rules []Rule
	log   *slog.Logger

	optionsTable string
	autoloadRe   *regexp.Regexp
	optionRe     *regexp.Regexp

	mu      sync.Mutex
	entries map[string]*entry
	lru     *list.List // *entry, front = most recently used
	byTag   map[string]map[*entry]struct{}

	hits, misses uint64
}

type entry struct {
	key     string
	result  *mysql.Result
	expires time.Time
	tags    []string
	elem    *list.Element
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
		entries: make(map[string]*entry),
		lru:     list.New(),
		byTag:   make(map[string]map[*entry]struct{}),
	}
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
		c.misses++
		return nil, false
	}
	if time.Now().After(e.expires) {
		c.removeLocked(e)
		c.misses++
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	c.hits++
	c.log.Debug("cache hit", "query", query)
	return e.result, true
}

// Store caches the result when the query matches a cacheable pattern.
// The result is shared read-only between sessions afterwards.
func (c *Cache) Store(db, query string, r *mysql.Result) {
	if r == nil || !r.HasResultset() {
		return
	}
	ttl, tags, ok := c.cacheable(db, query)
	if !ok {
		return
	}
	// RawPkg accumulated every packet of this result while it was read.
	if len(r.RawPkg) > c.cfg.MaxResultBytes {
		return
	}
	key := db + "\x00" + query

	c.mu.Lock()
	defer c.mu.Unlock()

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
	}
	e.elem = c.lru.PushFront(e)
	c.entries[key] = e
	for _, tag := range tags {
		set := c.byTag[tag]
		if set == nil {
			set = make(map[*entry]struct{})
			c.byTag[tag] = set
		}
		set[e] = struct{}{}
	}
}

// cacheable decides whether a SELECT may be cached and how.
func (c *Cache) cacheable(db, query string) (time.Duration, []string, bool) {
	if unsafeSelectRe.MatchString(query) {
		return 0, nil, false
	}

	if c.cfg.AutoloadOptions && c.autoloadRe.MatchString(query) {
		return c.cfg.DefaultTTL, []string{
			tagAlloptions(db),
			tagTable(db, c.optionsTable),
		}, true
	}

	if c.cfg.Transients {
		if m := c.optionRe.FindStringSubmatch(query); m != nil {
			if isTransientName(m[1]) {
				return c.cfg.DefaultTTL, []string{
					tagOption(db, m[1]),
					tagTable(db, c.optionsTable),
				}, true
			}
			return 0, nil, false
		}
	}

	for i := range c.rules {
		r := &c.rules[i]
		if !r.re.MatchString(query) {
			continue
		}
		ttl := r.TTL
		if ttl <= 0 {
			ttl = c.cfg.DefaultTTL
		}
		tags := make([]string, 0, len(r.InvalidateOn))
		for _, table := range r.InvalidateOn {
			tags = append(tags, tagTable(db, table))
		}
		return ttl, tags, true
	}
	return 0, nil, false
}

// InvalidateWrite drops the entries a write statement may have affected.
func (c *Cache) InvalidateWrite(db, query string) {
	table, ok := extractTable(query)
	if !ok {
		c.Flush("unparseable write statement")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if table == c.optionsTable {
		if names := extractOptionNames(query); len(names) > 0 {
			// Attributed options write: the alloptions snapshot may contain
			// any of these options, single-option entries only their own.
			c.dropTagLocked(tagAlloptions(db))
			for _, name := range names {
				c.dropTagLocked(tagOption(db, name))
			}
			return
		}
	}
	c.dropTagLocked(tagTable(db, table))
}

// Flush empties the whole cache.
func (c *Cache) Flush(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.entries); n > 0 {
		c.log.Debug("cache flushed", "entries", n, "reason", reason)
	}
	c.entries = make(map[string]*entry)
	c.lru.Init()
	c.byTag = make(map[string]map[*entry]struct{})
}

// Stats returns hit/miss counters (for logging and future metrics).
func (c *Cache) Stats() (hits, misses uint64, entries int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, len(c.entries)
}

func (c *Cache) dropTagLocked(tag string) {
	for e := range c.byTag[tag] {
		c.removeLocked(e)
	}
}

func (c *Cache) removeLocked(e *entry) {
	delete(c.entries, e.key)
	c.lru.Remove(e.elem)
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
	return fmt.Sprintf("cache{prefix=%s rules=%d}", c.cfg.TablePrefix, len(c.rules))
}
