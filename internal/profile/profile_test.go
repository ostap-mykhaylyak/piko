package profile

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ostap-mykhaylyak/piko/internal/config"
)

func TestFingerprint(t *testing.T) {
	cases := map[string]string{
		"SELECT option_value FROM wp_options WHERE option_name = 'siteurl' LIMIT 1": "SELECT option_value FROM wp_options WHERE option_name = ? LIMIT ?",
		"SELECT * FROM wp_posts WHERE ID IN (1, 2, 3)":                              "SELECT * FROM wp_posts WHERE ID IN (?)",
		"SELECT  *\nFROM   t\tWHERE a = 10":                                         "SELECT * FROM t WHERE a = ?",
		"UPDATE wp_options2 SET v = 'it''s' WHERE id = 5":                           "UPDATE wp_options2 SET v = ? WHERE id = ?",
		"SELECT 'a\\'b', \"c\" FROM t":                                              "SELECT ?, ? FROM t",
	}
	for in, want := range cases {
		if got := Fingerprint(in); got != want {
			t.Errorf("Fingerprint(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSuggestColumns(t *testing.T) {
	q := "SELECT * FROM wp_postmeta WHERE meta_key = 'total_sales' AND post_id IN (1,2) ORDER BY meta_id ASC"
	got := suggestColumns(q, "wp_postmeta")
	want := []string{"meta_key", "post_id", "meta_id"}
	if len(got) != len(want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("columns = %v, want %v", got, want)
		}
	}

	// JOIN queries are never guessed.
	if cols := suggestColumns("SELECT * FROM a INNER JOIN b ON a.id = b.id WHERE a.x = 1", "a"); cols != nil {
		t.Fatalf("expected no suggestion for JOIN query, got %v", cols)
	}
}

// fakeExecutor replays canned results keyed by query prefix.
type fakeExecutor struct {
	results map[string]*mysql.Result
}

func (f *fakeExecutor) UseDB(string) error { return nil }
func (f *fakeExecutor) Execute(cmd string, _ ...any) (*mysql.Result, error) {
	for prefix, r := range f.results {
		if strings.HasPrefix(cmd, prefix) {
			return r, nil
		}
	}
	return &mysql.Result{}, nil
}

func resultOf(t *testing.T, names []string, rows [][]any) *mysql.Result {
	t.Helper()
	rs, err := mysql.BuildSimpleTextResultset(names, rows)
	if err != nil {
		t.Fatal(err)
	}
	// BuildSimpleTextResultset only fills Fields and RowDatas; results read
	// from the wire also carry the by-name index and parsed Values, which
	// the advisor relies on. Complete them like the client reader does.
	for i, name := range names {
		rs.FieldNames[name] = i
	}
	rs.Values = make([][]mysql.FieldValue, len(rs.RowDatas))
	for i, rd := range rs.RowDatas {
		var err error
		if rs.Values[i], err = rd.Parse(rs.Fields, false, nil); err != nil {
			t.Fatal(err)
		}
	}
	return mysql.NewResult(rs)
}

func testLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func TestExplainSuggestsIndex(t *testing.T) {
	log, buf := testLogger()
	a := newAdvisor(log)

	exec := &fakeExecutor{results: map[string]*mysql.Result{
		"EXPLAIN": resultOf(t,
			[]string{"id", "select_type", "table", "type", "possible_keys", "key", "rows", "Extra"},
			[][]any{{int64(1), "SIMPLE", "wp_postmeta", "ALL", "", "", int64(50000), "Using where"}}),
	}}

	st := &queryStat{
		db:     "wp",
		digest: "SELECT * FROM wp_postmeta WHERE meta_key = ?",
		sample: "SELECT * FROM wp_postmeta WHERE meta_key = 'total_sales'",
	}
	if err := a.explainQuery(exec, st); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "action=add") || !strings.Contains(out, "wp_postmeta") ||
		!strings.Contains(out, "meta_key") {
		t.Fatalf("expected add-index suggestion, log was:\n%s", out)
	}

	// Same finding again: suggested only once.
	buf.Reset()
	if err := a.explainQuery(exec, st); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "action=add") {
		t.Fatal("suggestion should be emitted only once")
	}
}

func TestExplainSmallTableIgnored(t *testing.T) {
	log, buf := testLogger()
	a := newAdvisor(log)

	exec := &fakeExecutor{results: map[string]*mysql.Result{
		"EXPLAIN": resultOf(t,
			[]string{"id", "select_type", "table", "type", "possible_keys", "key", "rows", "Extra"},
			[][]any{{int64(1), "SIMPLE", "tiny", "ALL", "", "", int64(12), ""}}),
	}}
	st := &queryStat{db: "wp", digest: "q", sample: "SELECT * FROM tiny WHERE a = 1"}
	if err := a.explainQuery(exec, st); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "index suggestion") {
		t.Fatalf("small tables must not trigger suggestions, log was:\n%s", buf.String())
	}
}

func TestSchemaReviewFindsDuplicatesAndUnused(t *testing.T) {
	log, buf := testLogger()
	a := newAdvisor(log)

	statsCols := []string{"TABLE_NAME", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "NON_UNIQUE"}
	exec := &fakeExecutor{results: map[string]*mysql.Result{
		"SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX": resultOf(t, statsCols, [][]any{
			// idx_meta_key is a prefix of idx_meta_key_value: redundant.
			{"wp_postmeta", "idx_meta_key", int64(1), "meta_key", int64(1)},
			{"wp_postmeta", "idx_meta_key_value", int64(1), "meta_key", int64(1)},
			{"wp_postmeta", "idx_meta_key_value", int64(2), "meta_value", int64(1)},
			// Unique index: never suggested for dropping.
			{"wp_users", "user_login_key", int64(1), "user_login", int64(0)},
			{"wp_users", "idx_ghost", int64(1), "display_name", int64(1)},
		}),
		"SELECT OBJECT_NAME, INDEX_NAME": resultOf(t,
			[]string{"OBJECT_NAME", "INDEX_NAME"},
			[][]any{
				{"wp_users", "idx_ghost"},
				{"wp_users", "user_login_key"}, // unique: filtered out
			}),
	}}

	if err := a.reviewSchema(exec, "wp"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "idx_meta_key") || !strings.Contains(out, "redundant") {
		t.Fatalf("expected duplicate-index suggestion, log was:\n%s", out)
	}
	if !strings.Contains(out, "idx_ghost") || !strings.Contains(out, "never used") {
		t.Fatalf("expected unused-index suggestion, log was:\n%s", out)
	}
	if strings.Contains(out, "user_login_key") {
		t.Fatalf("unique indexes must never be suggested for dropping, log was:\n%s", out)
	}
	if strings.Contains(out, "idx_meta_key_value\"") && strings.Contains(out, "action=drop index=idx_meta_key_value") {
		t.Fatalf("the covering index must not be dropped, log was:\n%s", out)
	}
}

func TestObserveAndSlowLog(t *testing.T) {
	log, buf := testLogger()
	cfg := config.Default().Profiling
	cfg.Enabled = true
	cfg.SlowQuery = 10 * time.Millisecond
	p := New(cfg, nil, log)

	p.Observe("wp", "SELECT * FROM t WHERE id = 1", time.Millisecond, 1, false, nil)
	if strings.Contains(buf.String(), "slow query") {
		t.Fatal("fast query logged as slow")
	}

	p.Observe("wp", "SELECT * FROM t WHERE id = 2", 50*time.Millisecond, 1, false, nil)
	if !strings.Contains(buf.String(), "slow query") {
		t.Fatal("slow query not logged")
	}

	// Both executions aggregate under one digest.
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.stats["SELECT * FROM t WHERE id = ?"]
	if !ok || st.calls != 2 {
		t.Fatalf("stats = %+v (ok=%v), want 2 calls under one digest", st, ok)
	}
	if st.max != 50*time.Millisecond || !strings.Contains(st.sample, "id = 2") {
		t.Fatalf("worst sample not kept: %+v", st)
	}
}
