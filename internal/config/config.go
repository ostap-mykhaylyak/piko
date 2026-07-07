// Package config loads and validates piko's YAML configuration.
package config

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root of config.yaml.
type Config struct {
	Listen    Listen    `yaml:"listen"`
	Backend   Backend   `yaml:"backend"`
	Users     []User    `yaml:"users"`
	Pool      Pool      `yaml:"pool"`
	Cache     Cache     `yaml:"cache"`
	Profiling Profiling `yaml:"profiling"`
	Log       Log       `yaml:"log"`
}

// Listen configures the client-facing listener.
type Listen struct {
	Address string `yaml:"address"`
	// MaxConnections caps concurrent client connections (0 = unlimited);
	// connections beyond the cap are closed immediately instead of piling
	// up behind a saturated backend.
	MaxConnections int `yaml:"max_connections"`
}

// Backend is the MySQL server piko forwards to. Username and password are
// piko's own credentials: backend connections belong to piko, not to clients.
type Backend struct {
	Address  string `yaml:"address"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// User is an account that clients (e.g. WordPress) use to authenticate
// against piko. When no users are configured, clients authenticate with
// the backend credentials.
type User struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// Pool controls the backend connection pool.
type Pool struct {
	// MaxOpen caps the total number of backend connections (leased + idle).
	MaxOpen int `yaml:"max_open"`
	// MaxIdle is how many idle connections are kept warm for reuse.
	MaxIdle int `yaml:"max_idle"`
	// PingInterval is how often idle connections (pooled or attached to an
	// inactive client) receive a COM_PING so MySQL's wait_timeout never
	// closes them.
	PingInterval time.Duration `yaml:"ping_interval"`
	// IdleTimeout closes pooled connections unused for this long (0 = never).
	IdleTimeout time.Duration `yaml:"idle_timeout"`
	// AcquireTimeout bounds how long a client waits for a connection when
	// the pool is exhausted.
	AcquireTimeout time.Duration `yaml:"acquire_timeout"`
	// Multiplexing releases the backend connection back to the pool between
	// queries whenever the session state allows it, so many client sessions
	// share few backend connections. Sessions holding state (transactions,
	// temp tables, locks, prepared statements, user variables...) keep
	// their connection automatically.
	Multiplexing bool `yaml:"multiplexing"`
	// Breaker protects against a dead or unreachable backend.
	Breaker Breaker `yaml:"breaker"`
}

// Breaker is the circuit breaker: after Failures consecutive connection
// failures piko fails fast instead of letting every PHP worker wait for
// its own timeout, and probes the backend until it recovers.
type Breaker struct {
	// Failures is how many consecutive dial failures open the circuit
	// (0 disables the breaker).
	Failures int `yaml:"failures"`
	// ProbeInterval is how often the backend is probed while the circuit
	// is open.
	ProbeInterval time.Duration `yaml:"probe_interval"`
}

// Cache controls the WordPress-aware in-memory query cache.
type Cache struct {
	Enabled bool `yaml:"enabled"`
	// TablePrefix is the WordPress table prefix ($table_prefix).
	TablePrefix string `yaml:"table_prefix"`
	// AutoloadOptions caches the autoloaded options query.
	AutoloadOptions bool `yaml:"autoload_options"`
	// Transients caches transient reads from the options table.
	Transients bool `yaml:"transients"`
	// DefaultTTL is the safety expiry for cached entries; write-driven
	// invalidation is the primary mechanism.
	DefaultTTL time.Duration `yaml:"default_ttl"`
	// MaxEntries bounds the number of cached result sets.
	MaxEntries int `yaml:"max_entries"`
	// MaxResultBytes skips caching results larger than this.
	MaxResultBytes int `yaml:"max_result_bytes"`
	// RulesDir holds extra cache rule drop-ins (conf.d). Empty means
	// "conf.d next to the config file".
	RulesDir string `yaml:"rules_dir"`
	// Warmup re-populates the alloptions snapshot in the background after
	// invalidations, so the next pageload finds it hot.
	Warmup bool `yaml:"warmup"`
}

// Profiling controls query statistics, slow query logging and index
// suggestions, all emitted through the log.
type Profiling struct {
	Enabled bool `yaml:"enabled"`
	// SlowQuery logs any statement slower than this immediately (0 = off).
	SlowQuery time.Duration `yaml:"slow_query"`
	// ReportInterval is how often the aggregated query report is logged.
	ReportInterval time.Duration `yaml:"report_interval"`
	// TopQueries is how many queries the report details, heaviest first.
	TopQueries int `yaml:"top_queries"`
	// SuggestIndexes runs EXPLAIN on the heaviest queries and inspects the
	// schema, logging missing/duplicate/unused index suggestions.
	SuggestIndexes bool `yaml:"suggest_indexes"`
	// SuggestRewrites scans queries for known antipatterns (ORDER BY
	// RAND()...) and logs the conf.d rewrite rule to configure.
	SuggestRewrites bool `yaml:"suggest_rewrites"`
}

// Log controls logging output.
type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	// Path is the directory where piko.log is written.
	// The special value "stdout" logs to standard output instead.
	Path string `yaml:"path"`
}

// Default returns a Config with sensible defaults applied.
func Default() Config {
	return Config{
		Listen: Listen{Address: "0.0.0.0:3306"},
		Pool: Pool{
			MaxOpen:        100,
			MaxIdle:        10,
			PingInterval:   30 * time.Second,
			IdleTimeout:    5 * time.Minute,
			AcquireTimeout: 5 * time.Second,
			Multiplexing:   true,
			Breaker: Breaker{
				Failures:      3,
				ProbeInterval: 2 * time.Second,
			},
		},
		Cache: Cache{
			Enabled:         true,
			TablePrefix:     "wp_",
			AutoloadOptions: true,
			Transients:      true,
			DefaultTTL:      5 * time.Minute,
			MaxEntries:      10000,
			MaxResultBytes:  1 << 20, // 1 MiB
			Warmup:          true,
		},
		Profiling: Profiling{
			SlowQuery:       500 * time.Millisecond,
			ReportInterval:  10 * time.Minute,
			TopQueries:      20,
			SuggestIndexes:  true,
			SuggestRewrites: true,
		},
		Log: Log{Level: "info", Format: "text", Path: "/var/log/piko"},
	}
}

// Load reads, parses and validates the YAML file at path.
func Load(path string) (Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("reading config: %w", err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("parsing %s: %w", path, err)
	}

	// No users defined: clients authenticate with the backend credentials,
	// so the common single-site setup needs them only once.
	if len(cfg.Users) == 0 {
		cfg.Users = []User{{Username: cfg.Backend.Username, Password: cfg.Backend.Password}}
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, nil
}

// Validate checks the configuration for consistency.
func (c *Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.Listen.Address); err != nil {
		return fmt.Errorf("listen.address %q: %w", c.Listen.Address, err)
	}
	if c.Backend.Address == "" {
		return fmt.Errorf("backend.address is required")
	}
	if _, _, err := net.SplitHostPort(c.Backend.Address); err != nil {
		return fmt.Errorf("backend.address %q: %w", c.Backend.Address, err)
	}
	if c.Backend.Username == "" {
		return fmt.Errorf("backend.username is required")
	}
	if len(c.Users) == 0 {
		return fmt.Errorf("at least one user is required (omit users entirely to reuse the backend credentials)")
	}
	for i, u := range c.Users {
		if u.Username == "" {
			return fmt.Errorf("users[%d].username is required", i)
		}
	}
	if c.Pool.MaxOpen < 1 {
		return fmt.Errorf("pool.max_open must be >= 1")
	}
	if c.Pool.MaxIdle < 0 || c.Pool.MaxIdle > c.Pool.MaxOpen {
		return fmt.Errorf("pool.max_idle must be between 0 and pool.max_open")
	}
	if c.Pool.PingInterval <= 0 {
		return fmt.Errorf("pool.ping_interval must be > 0")
	}
	if c.Pool.IdleTimeout < 0 {
		return fmt.Errorf("pool.idle_timeout must be >= 0 (0 disables it)")
	}
	if c.Pool.AcquireTimeout <= 0 {
		return fmt.Errorf("pool.acquire_timeout must be > 0")
	}
	if c.Pool.Breaker.Failures < 0 {
		return fmt.Errorf("pool.breaker.failures must be >= 0 (0 disables it)")
	}
	if c.Pool.Breaker.Failures > 0 && c.Pool.Breaker.ProbeInterval <= 0 {
		return fmt.Errorf("pool.breaker.probe_interval must be > 0")
	}
	if c.Listen.MaxConnections < 0 {
		return fmt.Errorf("listen.max_connections must be >= 0 (0 = unlimited)")
	}
	if c.Cache.Enabled {
		if c.Cache.TablePrefix == "" {
			return fmt.Errorf("cache.table_prefix is required when the cache is enabled")
		}
		if c.Cache.DefaultTTL <= 0 {
			return fmt.Errorf("cache.default_ttl must be > 0")
		}
		if c.Cache.MaxEntries < 1 {
			return fmt.Errorf("cache.max_entries must be >= 1")
		}
		if c.Cache.MaxResultBytes < 1 {
			return fmt.Errorf("cache.max_result_bytes must be >= 1")
		}
	}
	if c.Profiling.Enabled {
		if c.Profiling.ReportInterval <= 0 {
			return fmt.Errorf("profiling.report_interval must be > 0")
		}
		if c.Profiling.TopQueries < 1 {
			return fmt.Errorf("profiling.top_queries must be >= 1")
		}
		if c.Profiling.SlowQuery < 0 {
			return fmt.Errorf("profiling.slow_query must be >= 0 (0 disables it)")
		}
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level %q: must be debug, info, warn or error", c.Log.Level)
	}
	if c.Log.Path == "" {
		return fmt.Errorf(`log.path is required (use "stdout" for console output)`)
	}
	return nil
}
