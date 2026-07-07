package firewall

import "testing"

func TestCheck(t *testing.T) {
	f, err := New([]Rule{
		{Name: "no-report", Match: `(?i)^SELECT .* FROM wp_bigtable`},
	})
	if err != nil {
		t.Fatal(err)
	}

	if name, blocked := f.Check("SELECT * FROM wp_bigtable WHERE y = 2024"); !blocked || name != "no-report" {
		t.Fatalf("expected block by no-report, got %q %v", name, blocked)
	}
	if _, blocked := f.Check("SELECT * FROM wp_posts"); blocked {
		t.Fatal("unexpected block")
	}
}

func TestSetRulesAndErrors(t *testing.T) {
	f, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, blocked := f.Check("SELECT 1"); blocked {
		t.Fatal("empty firewall blocked a query")
	}

	if err := f.SetRules([]Rule{{Name: "x", Match: "^SELECT 1$"}}); err != nil {
		t.Fatal(err)
	}
	if _, blocked := f.Check("SELECT 1"); !blocked {
		t.Fatal("reloaded rule not applied")
	}

	// Invalid reload keeps the previous rules.
	if err := f.SetRules([]Rule{{Name: "bad", Match: "(["}}); err == nil {
		t.Fatal("expected error for invalid regex")
	}
	if _, blocked := f.Check("SELECT 1"); !blocked {
		t.Fatal("previous rules lost after failed reload")
	}
	if err := f.SetRules([]Rule{{Name: "empty"}}); err == nil {
		t.Fatal("expected error for missing match")
	}
}
