// Command piko is a MySQL proxy for WordPress and WooCommerce.
package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/profile"
	"github.com/ostap-mykhaylyak/piko/internal/proxy"
	"github.com/ostap-mykhaylyak/piko/internal/rewrite"
)

const defaultConfigPath = "/etc/piko/config.yaml"

//go:embed config.default.yaml
var defaultConfig []byte

//go:embed woocommerce.default.yaml
var defaultWooCommerceRules []byte

// Set at build time via -ldflags (see .goreleaser.yaml / Makefile).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	configPath := flag.String("config", defaultConfigPath, "path to the YAML configuration file")
	initConfig := flag.Bool("init", false, "create a default configuration file at the -config path and exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("piko %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *initConfig {
		if err := writeDefaultConfig(*configPath); err != nil {
			fmt.Fprintln(os.Stderr, "piko:", err)
			os.Exit(1)
		}
		fmt.Println("configuration file created:", *configPath)
		return
	}

	if err := run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, "piko:", err)
		os.Exit(1)
	}
}

// writeDefaultConfig creates the default configuration at path plus the
// conf.d drop-in directory with the WooCommerce rules, refusing to
// overwrite existing files. The config is restricted to the owner because
// it contains credentials.
func writeDefaultConfig(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s already exists, not overwriting it", path)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("checking %s: %w", path, err)
	}

	confD := filepath.Join(filepath.Dir(path), "conf.d")
	if err := os.MkdirAll(confD, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", confD, err)
	}
	if err := os.WriteFile(path, defaultConfig, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}

	wooPath := filepath.Join(confD, "woocommerce.yaml")
	if _, err := os.Stat(wooPath); os.IsNotExist(err) {
		if err := os.WriteFile(wooPath, defaultWooCommerceRules, 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", wooPath, err)
		}
		fmt.Println("cache rules created:", wooPath)
	}
	return nil
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	logWriter, err := openLogWriter(cfg.Log)
	if err != nil {
		return err
	}
	log := newLogger(logWriter, cfg.Log)
	log.Info("starting piko", "version", version, "config", configPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// conf.d drop-ins carry cache rules and query rewrites; rewrites apply
	// even when the cache is disabled.
	rulesDir := cfg.Cache.RulesDir
	if rulesDir == "" {
		rulesDir = filepath.Join(filepath.Dir(configPath), "conf.d")
	}
	cacheRules, rewriteRules, err := cache.LoadRuleDir(rulesDir, cfg.Cache.TablePrefix)
	if err != nil {
		return err
	}

	var qc *cache.Cache
	if cfg.Cache.Enabled {
		qc = cache.New(cfg.Cache, cacheRules, log)
		log.Info("query cache enabled",
			"table_prefix", cfg.Cache.TablePrefix, "rules", len(cacheRules), "rules_dir", rulesDir)
	}

	// The rewriter always exists (possibly with zero rules) so a hot
	// reload can bring rules in later.
	rw, err := rewrite.New(rewriteRules, log)
	if err != nil {
		return err
	}
	if rw.Len() > 0 {
		log.Info("query rewriting enabled", "rules", rw.Len(), "rules_dir", rulesDir)
	}

	backendPool := pool.New(cfg.Backend, cfg.Pool, log, nil)
	defer backendPool.Close()

	if qc != nil && cfg.Cache.Warmup {
		warmer := cache.NewWarmer(qc, backendPool, log)
		go warmer.Run(ctx)
		qc.SetRefetch(warmer.Trigger)
	}

	var prof *profile.Profiler
	if cfg.Profiling.Enabled {
		prof = profile.New(cfg.Profiling, backendPool, log)
		if qc != nil {
			prof.SetCache(qc)
		}
		go prof.Run(ctx)
		log.Info("profiling enabled",
			"slow_query", cfg.Profiling.SlowQuery,
			"report_interval", cfg.Profiling.ReportInterval,
			"suggest_indexes", cfg.Profiling.SuggestIndexes)
	}

	// Hot reload: SIGHUP re-reads the conf.d drop-ins (cache rules and
	// rewrites) without touching client sessions.
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-reload:
			}
			newRules, newRewrites, err := cache.LoadRuleDir(rulesDir, cfg.Cache.TablePrefix)
			if err != nil {
				log.Error("rules reload failed, keeping the previous rules", "error", err)
				continue
			}
			if err := rw.SetRules(newRewrites); err != nil {
				log.Error("rewrites reload failed, keeping the previous rules", "error", err)
				continue
			}
			if qc != nil {
				qc.SetRules(newRules)
			}
			log.Info("rules reloaded",
				"cache_rules", len(newRules), "rewrites", len(newRewrites), "rules_dir", rulesDir)
		}
	}()

	srv := proxy.New(proxy.Options{
		Listen:   cfg.Listen,
		Users:    cfg.Users,
		PoolCfg:  cfg.Pool,
		Pool:     backendPool,
		Cache:    qc,
		Profiler: prof,
		Rewriter: rw,
		Log:      log,
	})
	if err := srv.Run(ctx); err != nil {
		return err
	}

	log.Info("shutdown complete")
	return nil
}

// openLogWriter returns the destination for logs: piko.log inside the
// configured directory (created if missing), or stdout.
func openLogWriter(cfg config.Log) (io.Writer, error) {
	if cfg.Path == "stdout" {
		return os.Stdout, nil
	}

	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return nil, fmt.Errorf("creating log directory %s: %w", cfg.Path, err)
	}
	logFile := filepath.Join(cfg.Path, "piko.log")
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("opening log file %s: %w", logFile, err)
	}
	return f, nil
}

func newLogger(w io.Writer, cfg config.Log) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}
	return slog.New(handler)
}
