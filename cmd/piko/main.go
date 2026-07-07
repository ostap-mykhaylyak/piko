// Command piko is a MySQL proxy for WordPress and WooCommerce.
package main

import (
	"context"
	"crypto/tls"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ostap-mykhaylyak/piko/internal/cache"
	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/firewall"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
	"github.com/ostap-mykhaylyak/piko/internal/profile"
	"github.com/ostap-mykhaylyak/piko/internal/proxy"
	"github.com/ostap-mykhaylyak/piko/internal/rewrite"
	"github.com/ostap-mykhaylyak/piko/internal/status"
)

const (
	defaultConfigPath = "/etc/piko/config.yaml"
	binaryPath        = "/sbin/piko"
	servicePath       = "/etc/systemd/system/piko.service"
	logrotatePath     = "/etc/logrotate.d/piko"
	logDir            = "/var/log/piko"
	systemUser        = "piko"
)

//go:embed config.default.yaml
var defaultConfig []byte

//go:embed woocommerce.default.yaml
var defaultWooCommerceRules []byte

//go:embed piko.service
var serviceUnit string

//go:embed piko.logrotate
var logrotateConf []byte

// Set at build time via -ldflags (see .goreleaser.yaml / Makefile).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	configPath := flag.String("config", defaultConfigPath, "path to the YAML configuration file")
	initConfig := flag.Bool("init", false, "install piko as a systemd service (binary, config, service, dirs) and exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("piko %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *initConfig {
		if err := runInit(*configPath); err != nil {
			fmt.Fprintln(os.Stderr, "piko:", err)
			os.Exit(1)
		}
		return
	}

	if flag.Arg(0) == "status" {
		if err := printStatus(*configPath); err != nil {
			fmt.Fprintln(os.Stderr, "piko:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, "piko:", err)
		os.Exit(1)
	}
}

// printStatus queries the running instance through its status socket.
func printStatus(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if cfg.Status.Socket == "" {
		return fmt.Errorf("status.socket is disabled in %s", configPath)
	}

	snap, err := status.Query(cfg.Status.Socket)
	if err != nil {
		return fmt.Errorf("is piko running? %w", err)
	}

	fmt.Printf("piko %s, up %s\n", snap.Version,
		(time.Duration(snap.UptimeSeconds) * time.Second).String())
	fmt.Printf("clients:  %d connected, %d pinned sessions\n",
		snap.Clients.Clients, snap.Clients.Pinned)
	breaker := "closed (backend healthy)"
	if snap.Pool.BreakerOpen {
		breaker = "OPEN (backend unreachable)"
	}
	fmt.Printf("backend:  %d/%d connections open, %d idle, breaker %s\n",
		snap.Pool.Open, snap.Pool.MaxOpen, snap.Pool.Idle, breaker)
	if snap.Cache != nil {
		total := snap.Cache.Hits + snap.Cache.Misses
		ratio := 0.0
		if total > 0 {
			ratio = float64(snap.Cache.Hits) / float64(total) * 100
		}
		fmt.Printf("cache:    %d entries, %.1f MiB, hit ratio %.1f%% (%d hits / %d misses)\n",
			snap.Cache.Entries, float64(snap.Cache.Bytes)/(1<<20), ratio,
			snap.Cache.Hits, snap.Cache.Misses)
		for name, src := range snap.Cache.Sources {
			fmt.Printf("  %-24s %d hits, %d entries, %.1f MiB\n",
				name, src.Hits, src.Entries, float64(src.Bytes)/(1<<20))
		}
	}
	return nil
}

// runInit installs piko as a systemd service, ready to start: it copies the
// running binary to /sbin/piko, creates the piko system user and the config
// and log directories, and writes the config, conf.d rules, systemd unit and
// logrotate config. Everything is overwritten on every run, so `piko --init`
// always resets to a known-good installation.
func runInit(configPath string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("--init installs a system service and must run as root (try: sudo piko --init)")
	}

	uid, gid, err := ensureUser(systemUser)
	if err != nil {
		return err
	}

	if err := installBinary(binaryPath); err != nil {
		return err
	}
	fmt.Println("installed binary:", binaryPath)

	// Directories: config (root-owned, readable), logs (piko-owned).
	confD := filepath.Join(filepath.Dir(configPath), "conf.d")
	if err := os.MkdirAll(confD, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", confD, err)
	}
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		return fmt.Errorf("creating %s: %w", logDir, err)
	}
	if err := os.Chown(logDir, uid, gid); err != nil {
		return fmt.Errorf("chown %s: %w", logDir, err)
	}

	// Config files, overwritten every run.
	if err := writeFile(configPath, defaultConfig, 0o640); err != nil {
		return err
	}
	if err := os.Chown(configPath, uid, gid); err != nil {
		return fmt.Errorf("chown %s: %w", configPath, err)
	}
	if err := writeFile(filepath.Join(confD, "woocommerce.yaml"), defaultWooCommerceRules, 0o644); err != nil {
		return err
	}
	fmt.Println("wrote configuration:", configPath)

	// systemd unit (ExecStart follows -config) and logrotate.
	unit := strings.ReplaceAll(serviceUnit, defaultConfigPath, configPath)
	if err := writeFile(servicePath, []byte(unit), 0o644); err != nil {
		return err
	}
	if err := writeFile(logrotatePath, logrotateConf, 0o644); err != nil {
		return err
	}
	fmt.Println("installed service:", servicePath)

	if err := reloadSystemd(); err != nil {
		fmt.Fprintln(os.Stderr, "warning:", err)
	}

	fmt.Println("\nEdit", configPath, "then start piko with:")
	fmt.Println("  systemctl enable --now piko")
	return nil
}

// writeFile writes data to path, replacing any existing file.
func writeFile(path string, data []byte, perm os.FileMode) error {
	if err := os.WriteFile(path, data, perm); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	// WriteFile keeps the old mode if the file already existed; enforce it.
	if err := os.Chmod(path, perm); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}

// installBinary copies the running executable to dst atomically (via a
// temp file + rename), so overwriting piko while it runs cannot fail with
// "text file busy".
func installBinary(dst string) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locating the running binary: %w", err)
	}
	if self, err = filepath.EvalSymlinks(self); err != nil {
		return fmt.Errorf("resolving the running binary: %w", err)
	}
	data, err := os.ReadFile(self)
	if err != nil {
		return fmt.Errorf("reading the running binary: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(dst), err)
	}
	tmp := dst + ".new"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("installing %s: %w", dst, err)
	}
	return nil
}

// ensureUser creates the system user if missing and returns its uid/gid.
func ensureUser(name string) (int, int, error) {
	u, err := user.Lookup(name)
	if err != nil {
		if _, ok := err.(user.UnknownUserError); !ok {
			return 0, 0, fmt.Errorf("looking up user %s: %w", name, err)
		}
		cmd := exec.Command("useradd", "--system", "--no-create-home",
			"--shell", "/usr/sbin/nologin", name)
		if out, err := cmd.CombinedOutput(); err != nil {
			return 0, 0, fmt.Errorf("creating user %s: %w: %s", name, err, out)
		}
		if u, err = user.Lookup(name); err != nil {
			return 0, 0, fmt.Errorf("looking up new user %s: %w", name, err)
		}
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing uid of %s: %w", name, err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing gid of %s: %w", name, err)
	}
	return uid, gid, nil
}

// reloadSystemd runs `systemctl daemon-reload`; a missing systemctl (e.g. in
// a container) is reported but not fatal.
func reloadSystemd() error {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return fmt.Errorf("systemctl not found, skipping daemon-reload (start piko manually)")
	}
	if out, err := exec.Command("systemctl", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload failed: %w: %s", err, out)
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
	rules, err := cache.LoadRuleDir(rulesDir, cfg.Cache.TablePrefix)
	if err != nil {
		return err
	}

	var qc *cache.Cache
	if cfg.Cache.Enabled {
		qc = cache.New(cfg.Cache, rules.Cache, log)
		log.Info("query cache enabled",
			"table_prefix", cfg.Cache.TablePrefix, "rules", len(rules.Cache), "rules_dir", rulesDir)
	}

	// Rewriter and firewall always exist (possibly with zero rules) so a
	// hot reload can bring rules in later.
	rw, err := rewrite.New(rules.Rewrites, log)
	if err != nil {
		return err
	}
	if rw.Len() > 0 {
		log.Info("query rewriting enabled", "rules", rw.Len(), "rules_dir", rulesDir)
	}
	fw, err := firewall.New(rules.Blocks)
	if err != nil {
		return err
	}
	if fw.Len() > 0 {
		log.Info("query firewall enabled", "rules", fw.Len(), "rules_dir", rulesDir)
	}

	backendPool, err := pool.New(cfg.Backend, cfg.Pool, log, nil)
	if err != nil {
		return err
	}
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
			newRules, err := cache.LoadRuleDir(rulesDir, cfg.Cache.TablePrefix)
			if err != nil {
				log.Error("rules reload failed, keeping the previous rules", "error", err)
				continue
			}
			if err := rw.SetRules(newRules.Rewrites); err != nil {
				log.Error("rewrites reload failed, keeping the previous rules", "error", err)
				continue
			}
			if err := fw.SetRules(newRules.Blocks); err != nil {
				log.Error("firewall reload failed, keeping the previous rules", "error", err)
				continue
			}
			if qc != nil {
				qc.SetRules(newRules.Cache)
			}
			log.Info("rules reloaded", "cache_rules", len(newRules.Cache),
				"rewrites", len(newRules.Rewrites), "blocks", len(newRules.Blocks),
				"rules_dir", rulesDir)
		}
	}()

	var listenTLS *tls.Config
	if cfg.Listen.TLS.Enabled() {
		cert, err := tls.LoadX509KeyPair(cfg.Listen.TLS.Cert, cfg.Listen.TLS.Key)
		if err != nil {
			return fmt.Errorf("loading listen.tls certificate: %w", err)
		}
		listenTLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		log.Info("client TLS enabled", "cert", cfg.Listen.TLS.Cert)
	}

	srv := proxy.New(proxy.Options{
		Listen:   cfg.Listen,
		Users:    cfg.Users,
		PoolCfg:  cfg.Pool,
		Pool:     backendPool,
		Cache:    qc,
		Profiler: prof,
		Rewriter: rw,
		Firewall: fw,
		TLS:      listenTLS,
		Log:      log,
	})

	if cfg.Status.Socket != "" {
		start := time.Now()
		collect := func() status.Snapshot {
			snap := status.Snapshot{
				Version:       version,
				UptimeSeconds: int64(time.Since(start).Seconds()),
				Clients:       srv.Stat(),
				Pool:          backendPool.Stat(),
			}
			if qc != nil {
				rep := qc.ReportStats()
				snap.Cache = &rep
			}
			return snap
		}
		go func() {
			if err := status.Serve(ctx, cfg.Status.Socket, collect, log); err != nil {
				log.Warn("status socket unavailable", "socket", cfg.Status.Socket, "error", err)
			}
		}()
	}

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
