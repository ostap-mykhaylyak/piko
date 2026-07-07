package cache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ostap-mykhaylyak/piko/internal/firewall"
	"github.com/ostap-mykhaylyak/piko/internal/rewrite"
)

// Rule caches SELECTs matching a regex until a write touches one of the
// invalidation tables (or the TTL expires). Rules are loaded from
// conf.d/*.yaml drop-in files, e.g. the WooCommerce profile.
type Rule struct {
	Name         string        `yaml:"name"`
	Match        string        `yaml:"match"`
	TTL          time.Duration `yaml:"ttl"`
	InvalidateOn []string      `yaml:"invalidate_on"`

	re *regexp.Regexp
}

// ruleFile is the schema of a conf.d drop-in: cache rules, query rewrites
// and firewall blocks.
type ruleFile struct {
	Name     string          `yaml:"name"`
	Rules    []Rule          `yaml:"rules"`
	Rewrites []rewrite.Rule  `yaml:"rewrites"`
	Block    []firewall.Rule `yaml:"block"`
}

// RuleSet is everything the conf.d drop-ins declare.
type RuleSet struct {
	Cache    []Rule
	Rewrites []rewrite.Rule
	Blocks   []firewall.Rule
}

// LoadRuleDir loads and compiles every *.yaml/*.yml file in dir, in
// lexical order. The literal placeholder {prefix} in patterns and table
// lists expands to tablePrefix, so shipped rules work with any WordPress
// $table_prefix. A missing directory is not an error: it just means no
// extra rules.
func LoadRuleDir(dir, tablePrefix string) (RuleSet, error) {
	var set RuleSet

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return set, nil
	}
	if err != nil {
		return set, fmt.Errorf("reading rules directory: %w", err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		switch filepath.Ext(e.Name()) {
		case ".yaml", ".yml":
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		path := filepath.Join(dir, name)
		f, err := loadRuleFile(path, tablePrefix)
		if err != nil {
			return set, err
		}
		set.Cache = append(set.Cache, f.Rules...)
		set.Rewrites = append(set.Rewrites, f.Rewrites...)
		set.Blocks = append(set.Blocks, f.Block...)
	}
	return set, nil
}

func loadRuleFile(path, tablePrefix string) (*ruleFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	var f ruleFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	expand := func(s string) string { return strings.ReplaceAll(s, "{prefix}", tablePrefix) }
	for i := range f.Rules {
		r := &f.Rules[i]
		r.Match = expand(r.Match)
		for j := range r.InvalidateOn {
			r.InvalidateOn[j] = expand(r.InvalidateOn[j])
		}
		if r.Match == "" {
			return nil, fmt.Errorf("%s: rules[%d] (%s): match is required", path, i, r.Name)
		}
		if len(r.InvalidateOn) == 0 && r.TTL <= 0 {
			return nil, fmt.Errorf("%s: rules[%d] (%s): needs invalidate_on tables or a ttl", path, i, r.Name)
		}
		re, err := regexp.Compile(r.Match)
		if err != nil {
			return nil, fmt.Errorf("%s: rules[%d] (%s): invalid match regex: %w", path, i, r.Name, err)
		}
		r.re = re
	}
	for i := range f.Rewrites {
		f.Rewrites[i].Match = expand(f.Rewrites[i].Match)
		f.Rewrites[i].Replace = expand(f.Rewrites[i].Replace)
		if err := f.Rewrites[i].Compile(); err != nil {
			return nil, fmt.Errorf("%s: rewrites[%d]: %w", path, i, err)
		}
	}
	for i := range f.Block {
		f.Block[i].Match = expand(f.Block[i].Match)
		if err := f.Block[i].Compile(); err != nil {
			return nil, fmt.Errorf("%s: block[%d]: %w", path, i, err)
		}
	}
	return &f, nil
}
