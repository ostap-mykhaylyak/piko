// Package firewall rejects queries matching configured patterns before
// they reach the backend: the emergency brake for runaway plugin queries
// (combined with hot reload, a bad query can be blocked without restarting
// anything) and an extra layer against known-bad SQL.
package firewall

import (
	"fmt"
	"regexp"
	"sync/atomic"
)

// Rule blocks every query matching an RE2 regex.
type Rule struct {
	Name  string `yaml:"name"`
	Match string `yaml:"match"`

	re *regexp.Regexp
}

// Compile validates and compiles the rule's regex; idempotent.
func (r *Rule) Compile() error {
	if r.Match == "" {
		return fmt.Errorf("block rule %q: match is required", r.Name)
	}
	if r.re != nil {
		return nil
	}
	re, err := regexp.Compile(r.Match)
	if err != nil {
		return fmt.Errorf("block rule %q: invalid match regex: %w", r.Name, err)
	}
	r.re = re
	return nil
}

// Firewall checks queries against an ordered rule list. The rule set is
// swappable at runtime (hot reload).
type Firewall struct {
	rules atomic.Pointer[[]Rule]
}

// New compiles any uncompiled rules and builds the firewall.
func New(rules []Rule) (*Firewall, error) {
	f := &Firewall{}
	if err := f.SetRules(rules); err != nil {
		return nil, err
	}
	return f, nil
}

// SetRules atomically replaces the rule set; used by hot reload.
func (f *Firewall) SetRules(rules []Rule) error {
	for i := range rules {
		if err := rules[i].Compile(); err != nil {
			return err
		}
	}
	f.rules.Store(&rules)
	return nil
}

// Check returns the name of the first rule blocking the query, if any.
func (f *Firewall) Check(query string) (string, bool) {
	rules := *f.rules.Load()
	for i := range rules {
		if rules[i].re.MatchString(query) {
			return rules[i].Name, true
		}
	}
	return "", false
}

// Len returns the number of loaded rules.
func (f *Firewall) Len() int { return len(*f.rules.Load()) }
