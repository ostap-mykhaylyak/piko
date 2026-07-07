package rewrite

import (
	"log/slog"
	"testing"
)

func newRewriter(t *testing.T, rules []Rule) *Rewriter {
	t.Helper()
	rw, err := New(rules, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return rw
}

func TestApply(t *testing.T) {
	rw := newRewriter(t, []Rule{
		{Name: "no-rand", Match: `(?i)\s*ORDER\s+BY\s+RAND\s*\(\s*\)`, Replace: ""},
		{Name: "force-limit", Match: `(?i)^(SELECT \* FROM wp_big)$`, Replace: "$1 LIMIT 100"},
	})

	got, applied := rw.Apply("SELECT ID FROM wp_posts ORDER BY RAND() LIMIT 1")
	if got != "SELECT ID FROM wp_posts LIMIT 1" {
		t.Errorf("rewritten = %q", got)
	}
	if len(applied) != 1 || applied[0] != "no-rand" {
		t.Errorf("applied = %v", applied)
	}

	// Capture group references work.
	got, applied = rw.Apply("SELECT * FROM wp_big")
	if got != "SELECT * FROM wp_big LIMIT 100" || len(applied) != 1 {
		t.Errorf("rewritten = %q, applied = %v", got, applied)
	}

	// Untouched queries come back as-is.
	got, applied = rw.Apply("SELECT 1")
	if got != "SELECT 1" || applied != nil {
		t.Errorf("rewritten = %q, applied = %v", got, applied)
	}
}

func TestCompileErrors(t *testing.T) {
	if _, err := New([]Rule{{Name: "bad", Match: "(["}}, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("expected error for invalid regex")
	}
	if _, err := New([]Rule{{Name: "empty"}}, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("expected error for missing match")
	}
}
