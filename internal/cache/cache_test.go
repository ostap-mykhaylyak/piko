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

	rules, err := LoadRuleDir(dir)
	if err != nil {
		t.Fatalf("LoadRuleDir: %v", err)
	}
	if len(rules) != 1 || rules[0].Name != "r1" || rules[0].re == nil {
		t.Fatalf("rules = %+v", rules)
	}

	// Missing directory is fine.
	if rules, err := LoadRuleDir(filepath.Join(dir, "missing")); err != nil || rules != nil {
		t.Fatalf("missing dir: rules=%v err=%v", rules, err)
	}

	// Broken regex is an error.
	bad := "rules:\n  - name: broken\n    match: \"([\"\n    invalidate_on: [t]\n"
	if err := os.WriteFile(filepath.Join(dir, "20-bad.yaml"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuleDir(dir); err == nil {
		t.Fatal("expected error for invalid regex")
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
