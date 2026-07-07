// Package rewrite applies configured regex rewrites to incoming queries,
// fixing known-bad SQL (ORDER BY RAND() and friends) without touching the
// application. Rules live in the conf.d drop-ins next to the cache rules.
package rewrite

import (
	"fmt"
	"log/slog"
	"regexp"
)

// Rule replaces every match of a regex in the query text. Replace supports
// capture group references ($1, $2...). An empty Replace deletes the match.
type Rule struct {
	Name    string `yaml:"name"`
	Match   string `yaml:"match"`
	Replace string `yaml:"replace"`

	re *regexp.Regexp
}

// Compile validates and compiles the rule's regex; idempotent.
func (r *Rule) Compile() error {
	if r.Match == "" {
		return fmt.Errorf("rewrite rule %q: match is required", r.Name)
	}
	if r.re != nil {
		return nil
	}
	re, err := regexp.Compile(r.Match)
	if err != nil {
		return fmt.Errorf("rewrite rule %q: invalid match regex: %w", r.Name, err)
	}
	r.re = re
	return nil
}

// Rewriter applies an ordered list of rules to query text.
type Rewriter struct {
	rules []Rule
	log   *slog.Logger
}

// New compiles any uncompiled rules and builds the rewriter.
func New(rules []Rule, log *slog.Logger) (*Rewriter, error) {
	for i := range rules {
		if err := rules[i].Compile(); err != nil {
			return nil, err
		}
	}
	return &Rewriter{rules: rules, log: log}, nil
}

// Apply runs every rule in order and returns the resulting query plus the
// names of the rules that fired.
func (rw *Rewriter) Apply(query string) (string, []string) {
	var applied []string
	for i := range rw.rules {
		r := &rw.rules[i]
		if !r.re.MatchString(query) {
			continue
		}
		query = r.re.ReplaceAllString(query, r.Replace)
		applied = append(applied, r.Name)
	}
	return query, applied
}

// Len returns the number of loaded rules.
func (rw *Rewriter) Len() int { return len(rw.rules) }
