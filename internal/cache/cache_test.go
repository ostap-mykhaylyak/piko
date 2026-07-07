package cache

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ostap-mykhaylyak/piko/internal/config"
)

func testResult(t *testing.T) *mysql.Result {
	t.Helper()
	rs, err := mysql.BuildSimpleTextResultset([]string{"v"}, [][]any{{int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	return mysql.NewResult(rs)
}

func testCache(t *testing.T, rules []Rule) *Cache {
	t.Helper()
	cfg := config.Default().Cache
	return New(cfg, rules, slog.New(slog.DiscardHandler))
}

const (
	autoloadQ  = "SELECT option_name, option_value FROM wp_options WHERE autoload IN ( 'yes', 'on', 'auto-on', 'auto' )"
	transientQ = "SELECT option_value FROM wp_options WHERE option_name = '_transient_foo' LIMIT 1"
)

func TestAutoloadCache(t *testing.T) {
	c := testCache(t, nil)
	r := testResult(t)

	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("unexpected hit on empty cache")
	}
	c.Store("wp", autoloadQ, r)
	if got, ok := c.Lookup("wp", autoloadQ); !ok || got != r {
		t.Fatal("expected cached autoload result")
	}

	// Same query, different database: separate entry.
	if _, ok := c.Lookup("other", autoloadQ); ok {
		t.Fatal("cache leaked across databases")
	}

	// An attributed write on another option still drops alloptions.
	c.InvalidateWrite("wp", "UPDATE wp_options SET option_value = 'x' WHERE option_name = 'blogname'")
	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("alloptions should be invalidated by an options write")
	}
}

func TestTransientCache(t *testing.T) {
	c := testCache(t, nil)
	c.Store("wp", transientQ, testResult(t))

	// A write on a different option must not touch this transient.
	c.InvalidateWrite("wp", "UPDATE wp_options SET option_value = 'x' WHERE option_name = 'blogname'")
	if _, ok := c.Lookup("wp", transientQ); !ok {
		t.Fatal("transient entry lost to an unrelated option write")
	}

	// A write on the transient itself drops it.
	c.InvalidateWrite("wp", "UPDATE wp_options SET option_value = 'y' WHERE option_name = '_transient_foo'")
	if _, ok := c.Lookup("wp", transientQ); ok {
		t.Fatal("transient entry should be invalidated by its own write")
	}

	// Non-transient options are not cached by the transient path.
	plain := "SELECT option_value FROM wp_options WHERE option_name = 'siteurl' LIMIT 1"
	c.Store("wp", plain, testResult(t))
	if _, ok := c.Lookup("wp", plain); ok {
		t.Fatal("plain option reads must not be cached")
	}
}

// TestTransientBatchRead: the IN(...) form WordPress uses for transients is
// cacheable and tagged per option name.
func TestTransientBatchRead(t *testing.T) {
	c := testCache(t, nil)
	q := "SELECT option_name, option_value FROM wp_options WHERE option_name IN ('_transient_wc_x','_transient_timeout_wc_x')"

	c.Store("wp", q, testResult(t))
	if _, ok := c.Lookup("wp", q); !ok {
		t.Fatal("batch transient read should be cached")
	}

	// The INSERT ... ON DUPLICATE that WooCommerce emits invalidates it.
	c.InvalidateWrite("wp", "INSERT INTO `wp_options` (`option_name`, `option_value`, `autoload`) VALUES ('_transient_wc_x', 'v', 'off') ON DUPLICATE KEY UPDATE `option_value` = VALUES(`option_value`)")
	if _, ok := c.Lookup("wp", q); ok {
		t.Fatal("batch transient read should be invalidated by a write to one of its options")
	}

	// A batch that includes a non-transient option is not cached.
	mixed := "SELECT option_name, option_value FROM wp_options WHERE option_name IN ('_transient_wc_x','siteurl')"
	c.Store("wp", mixed, testResult(t))
	if _, ok := c.Lookup("wp", mixed); ok {
		t.Fatal("batch with a non-transient option must not be cached")
	}
}

// TestTransientWriteKeepsAlloptions: a transient write (autoload='off')
// must not evict the alloptions snapshot — the WooCommerce hot-path fix.
func TestTransientWriteKeepsAlloptions(t *testing.T) {
	c := testCache(t, nil)
	c.Store("wp", autoloadQ, testResult(t))

	// The exact INSERT WooCommerce emits for a transient with expiration.
	c.InvalidateWrite("wp", "INSERT INTO `wp_options` (`option_name`, `option_value`, `autoload`) VALUES ('_transient_timeout_wc_related_29319', '1783506003', 'off') ON DUPLICATE KEY UPDATE `option_name` = VALUES(`option_name`), `option_value` = VALUES(`option_value`), `autoload` = VALUES(`autoload`)")
	if _, ok := c.Lookup("wp", autoloadQ); !ok {
		t.Fatal("a transient write must not evict the alloptions snapshot")
	}

	// A write to a real (non-transient) option still evicts it.
	c.InvalidateWrite("wp", "UPDATE wp_options SET option_value = 'x' WHERE option_name = 'blogname'")
	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("a normal option write must still evict the alloptions snapshot")
	}
}

func TestUnattributedOptionsWrite(t *testing.T) {
	c := testCache(t, nil)
	c.Store("wp", autoloadQ, testResult(t))
	c.Store("wp", transientQ, testResult(t))

	// DELETE with LIKE cannot be attributed: everything options-tagged goes.
	c.InvalidateWrite("wp", "DELETE FROM wp_options WHERE option_name LIKE '_transient_%'")
	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("alloptions should be gone")
	}
	if _, ok := c.Lookup("wp", transientQ); ok {
		t.Fatal("transient should be gone")
	}
}

func TestRuleCache(t *testing.T) {
	rules := []Rule{{
		Name:         "postmeta",
		Match:        `(?i)^SELECT post_id, meta_key, meta_value FROM wp_postmeta WHERE post_id IN \([0-9,\s]+\) ORDER BY meta_id ASC$`,
		TTL:          time.Minute,
		InvalidateOn: []string{"wp_postmeta"},
	}}
	for i := range rules {
		rules[i].re = mustCompile(t, rules[i].Match)
	}
	c := testCache(t, rules)

	q := "SELECT post_id, meta_key, meta_value FROM wp_postmeta WHERE post_id IN (1,2,3) ORDER BY meta_id ASC"
	c.Store("wp", q, testResult(t))
	if _, ok := c.Lookup("wp", q); !ok {
		t.Fatal("expected rule-matched query to be cached")
	}

	// A write on an unrelated table leaves it alone.
	c.InvalidateWrite("wp", "UPDATE wp_posts SET post_status = 'publish' WHERE ID = 5")
	if _, ok := c.Lookup("wp", q); !ok {
		t.Fatal("entry lost to an unrelated table write")
	}

	// A write on the invalidation table drops it.
	c.InvalidateWrite("wp", "UPDATE wp_postmeta SET meta_value = '1' WHERE meta_id = 9")
	if _, ok := c.Lookup("wp", q); ok {
		t.Fatal("entry should be invalidated by a postmeta write")
	}
}

// TestSearchPairing: a search entry is not served until its FOUND_ROWS()
// count is paired, and a write to an invalidation table drops it.
func TestSearchPairing(t *testing.T) {
	rule := Rule{Name: "listing", Match: `(?is)^SELECT SQL_CALC_FOUND_ROWS.*FROM wp_posts`,
		TTL: time.Minute, InvalidateOn: []string{"wp_posts"}}
	rule.re = mustCompile(t, rule.Match)
	c := testCache(t, []Rule{rule})

	q := "SELECT SQL_CALC_FOUND_ROWS wp_posts.ID FROM wp_posts WHERE post_type = 'product' LIMIT 0, 10"

	// Not cacheable via the normal path (SQL_CALC_FOUND_ROWS is unsafe there).
	c.Store("wp", q, testResult(t))
	if _, _, ok := c.LookupSearch("wp", q); ok {
		t.Fatal("normal Store must not create a search entry")
	}

	// Stored via the search path, but not served until the count is paired.
	c.StoreSearch("wp", q, testResult(t))
	if _, _, ok := c.LookupSearch("wp", q); ok {
		t.Fatal("search entry must not be served before FOUND_ROWS() is paired")
	}

	c.PairFoundRows("wp", q, 137)
	r, n, ok := c.LookupSearch("wp", q)
	if !ok || n != 137 || r == nil {
		t.Fatalf("paired search entry: ok=%v n=%d", ok, n)
	}

	// A write to wp_posts invalidates it.
	c.InvalidateWrite("wp", "UPDATE wp_posts SET post_status = 'publish' WHERE ID = 5")
	if _, _, ok := c.LookupSearch("wp", q); ok {
		t.Fatal("search entry should be invalidated by a wp_posts write")
	}
}

func TestUnparseableWriteFlushesAll(t *testing.T) {
	c := testCache(t, nil)
	c.Store("wp", autoloadQ, testResult(t))

	c.InvalidateWrite("wp", "DELETE a, b FROM wp_a AS a INNER JOIN wp_b AS b ON a.id = b.id")
	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("multi-table write should flush the cache")
	}
}

func TestUnsafeSelectsNotCached(t *testing.T) {
	c := testCache(t, nil)
	q := autoloadQ + " FOR UPDATE"
	c.Store("wp", q, testResult(t))
	if _, ok := c.Lookup("wp", q); ok {
		t.Fatal("locking reads must not be cached")
	}
}

func TestMaxEntriesEviction(t *testing.T) {
	cfg := config.Default().Cache
	cfg.MaxEntries = 2
	c := New(cfg, nil, slog.New(slog.DiscardHandler))

	for i := 0; i < 3; i++ {
		q := fmt.Sprintf("SELECT option_value FROM wp_options WHERE option_name = '_transient_%d' LIMIT 1", i)
		c.Store("wp", q, testResult(t))
	}
	if _, _, entries := c.Stats(); entries != 2 {
		t.Fatalf("cache holds %d entries, want 2", entries)
	}
	// The oldest entry was evicted.
	if _, ok := c.Lookup("wp", "SELECT option_value FROM wp_options WHERE option_name = '_transient_0' LIMIT 1"); ok {
		t.Fatal("LRU entry should have been evicted")
	}
}

func TestClassify(t *testing.T) {
	cases := map[string]Kind{
		"SELECT * FROM wp_posts":    KindSelect,
		"  select 1":                KindSelect,
		"/* hint */ SELECT 1":       KindSelect,
		"INSERT INTO t VALUES (1)":  KindWrite,
		"UPDATE t SET a = 1":        KindWrite,
		"DELETE FROM t":             KindWrite,
		"TRUNCATE TABLE t":          KindWrite,
		"CALL some_proc()":          KindWrite,
		"BEGIN":                     KindBegin,
		"START TRANSACTION":         KindBegin,
		"COMMIT":                    KindCommit,
		"ROLLBACK":                  KindRollback,
		"ROLLBACK TO SAVEPOINT sp1": KindOther,
		"SET NAMES utf8mb4":         KindOther,
		"SET autocommit = 0":        KindUnsafe,
		"XA START 'x'":              KindUnsafe,
		"SHOW TABLES":               KindOther,
	}
	for q, want := range cases {
		if got := Classify(q); got != want {
			t.Errorf("Classify(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestLoadRuleDir(t *testing.T) {
	dir := t.TempDir()
	good := `
name: test
rules:
  - name: r1
    match: "^SELECT 1$"
    ttl: 30s
    invalidate_on: [wp_t]
`
	if err := os.WriteFile(filepath.Join(dir, "10-test.yaml"), []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}

	set, err := LoadRuleDir(dir, "wp_")
	if err != nil {
		t.Fatalf("LoadRuleDir: %v", err)
	}
	if len(set.Cache) != 1 || set.Cache[0].Name != "r1" || set.Cache[0].re == nil {
		t.Fatalf("rules = %+v", set.Cache)
	}
	if len(set.Rewrites) != 0 || len(set.Blocks) != 0 {
		t.Fatalf("rewrites/blocks = %+v/%+v, want none", set.Rewrites, set.Blocks)
	}

	// A file with rewrites and blocks loads them too.
	withExtras := `
rewrites:
  - name: no-rand
    match: "(?i)\\s*ORDER\\s+BY\\s+RAND\\(\\)"
    replace: ""
block:
  - name: no-report
    match: "^SELECT .* FROM {prefix}bigtable"
`
	if err := os.WriteFile(filepath.Join(dir, "15-rw.yaml"), []byte(withExtras), 0o644); err != nil {
		t.Fatal(err)
	}
	set, err = LoadRuleDir(dir, "wp_")
	if err != nil || len(set.Rewrites) != 1 || len(set.Blocks) != 1 {
		t.Fatalf("rewrites/blocks = %+v/%+v (err %v), want 1/1", set.Rewrites, set.Blocks, err)
	}
	if set.Blocks[0].Match != "^SELECT .* FROM wp_bigtable" {
		t.Fatalf("block match not prefix-expanded: %q", set.Blocks[0].Match)
	}

	// Missing directory is fine.
	if set, err := LoadRuleDir(filepath.Join(dir, "missing"), "wp_"); err != nil || set.Cache != nil {
		t.Fatalf("missing dir: rules=%v err=%v", set.Cache, err)
	}

	// Broken regex is an error.
	bad := "rules:\n  - name: broken\n    match: \"([\"\n    invalidate_on: [t]\n"
	if err := os.WriteFile(filepath.Join(dir, "20-bad.yaml"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuleDir(dir, "wp_"); err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

// TestPrefixPlaceholder: {prefix} expands to the configured table prefix in
// rule patterns, invalidation tables and rewrites.
func TestPrefixPlaceholder(t *testing.T) {
	dir := t.TempDir()
	content := `
rules:
  - name: meta
    match: "^SELECT \\* FROM {prefix}postmeta$"
    ttl: 30s
    invalidate_on: ["{prefix}postmeta"]
rewrites:
  - name: rw
    match: "FROM {prefix}big"
    replace: "FROM {prefix}small"
`
	if err := os.WriteFile(filepath.Join(dir, "p.yaml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	set, err := LoadRuleDir(dir, "shop_")
	if err != nil {
		t.Fatalf("LoadRuleDir: %v", err)
	}
	if set.Cache[0].Match != `^SELECT \* FROM shop_postmeta$` {
		t.Errorf("match = %q", set.Cache[0].Match)
	}
	if set.Cache[0].InvalidateOn[0] != "shop_postmeta" {
		t.Errorf("invalidate_on = %v", set.Cache[0].InvalidateOn)
	}
	if set.Rewrites[0].Match != "FROM shop_big" || set.Rewrites[0].Replace != "FROM shop_small" {
		t.Errorf("rewrite = %+v", set.Rewrites[0])
	}
}

// TestSetRulesReload: swapping rules at runtime flushes the cache and the
// new rules take effect.
func TestSetRulesReload(t *testing.T) {
	c := testCache(t, nil)
	c.Store("wp", autoloadQ, testResult(t))

	q := "SELECT post_id, meta_key, meta_value FROM wp_postmeta WHERE post_id IN (1) ORDER BY meta_id ASC"
	c.Store("wp", q, testResult(t))
	if _, ok := c.Lookup("wp", q); ok {
		t.Fatal("query cached without any matching rule")
	}

	rule := Rule{Name: "postmeta", Match: `(?i)^SELECT post_id, meta_key, meta_value FROM wp_postmeta`,
		TTL: time.Minute, InvalidateOn: []string{"wp_postmeta"}}
	rule.re = mustCompile(t, rule.Match)
	c.SetRules([]Rule{rule})

	// Reload flushed everything.
	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("expected the cache to be flushed on rules reload")
	}
	// The new rule is live.
	c.Store("wp", q, testResult(t))
	if _, ok := c.Lookup("wp", q); !ok {
		t.Fatal("expected the reloaded rule to cache the query")
	}
}

// TestReportStats: per-source counters follow stores, hits and evictions,
// and misses count only cacheable queries (not the whole workload).
func TestReportStats(t *testing.T) {
	c := testCache(t, nil)
	c.Store("wp", autoloadQ, testResult(t))  // cacheable miss #1
	c.Store("wp", transientQ, testResult(t)) // cacheable miss #2
	c.Lookup("wp", autoloadQ)
	c.Lookup("wp", autoloadQ)
	c.Lookup("wp", transientQ)
	// A non-cacheable query missing the cache must NOT count as a miss.
	c.Lookup("wp", "SELECT missing")
	// A proactive warm refresh must not count as a miss either.
	c.Warm("wp", autoloadQ, testResult(t))

	rep := c.ReportStats()
	if rep.Hits != 3 || rep.Misses != 2 || rep.Entries != 2 || rep.Bytes <= 0 {
		t.Fatalf("report = %+v", rep)
	}
	if src := rep.Sources["alloptions"]; src.Hits != 2 || src.Entries != 1 {
		t.Fatalf("alloptions source = %+v", src)
	}
	if src := rep.Sources["transients"]; src.Hits != 1 || src.Entries != 1 {
		t.Fatalf("transients source = %+v", src)
	}

	// Invalidation updates the per-source entry counters.
	c.InvalidateWrite("wp", "UPDATE wp_options SET option_value = 'x' WHERE option_name = 'blogname'")
	rep = c.ReportStats()
	if src := rep.Sources["alloptions"]; src.Entries != 0 {
		t.Fatalf("alloptions entries after invalidation = %d, want 0", src.Entries)
	}
}

func mustCompile(t *testing.T, pattern string) *regexp.Regexp {
	t.Helper()
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatal(err)
	}
	return re
}
