package cache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"gopkg.in/yaml.v3"

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

// ruleFile is the schema of a conf.d drop-in: cache rules plus query
// rewrites.
type ruleFile struct {
	Name     string         `yaml:"name"`
	Rules    []Rule         `yaml:"rules"`
	Rewrites []rewrite.Rule `yaml:"rewrites"`
}

// LoadRuleDir loads and compiles every *.yaml/*.yml file in dir, in
// lexical order. A missing directory is not an error: it just means no
// extra rules.
func LoadRuleDir(dir string) ([]Rule, []rewrite.Rule, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("reading rules directory: %w", err)
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

	var rules []Rule
	var rewrites []rewrite.Rule
	for _, name := range files {
		path := filepath.Join(dir, name)
		f, err := loadRuleFile(path)
		if err != nil {
			return nil, nil, err
		}
		rules = append(rules, f.Rules...)
		rewrites = append(rewrites, f.Rewrites...)
	}
	return rules, rewrites, nil
}

func loadRuleFile(path string) (*ruleFile, error) {
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

	for i := range f.Rules {
		r := &f.Rules[i]
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
		if err := f.Rewrites[i].Compile(); err != nil {
			return nil, fmt.Errorf("%s: rewrites[%d]: %w", path, i, err)
		}
	}
	return &f, nil
}
