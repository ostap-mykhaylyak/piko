# piko

**piko** is a MySQL proxy written in Go, aimed at **WordPress** and **WooCommerce**,
configured with a single `config.yaml`.

> ⚠️ Early development. Features land incrementally; the interface may change.

## Features

- **Connection pooling** — backend connections belong to piko and are opened
  with its own credentials. When a client disconnects, its connection is reset
  (`COM_RESET_CONNECTION`) and parked for the next client instead of being
  closed: hundreds of short-lived PHP requests share a handful of real MySQL
  connections.
- **Per-query multiplexing** — with `pool.multiplexing: true` (default) the
  backend connection is returned to the pool **between queries**, not just
  between sessions: hundreds of concurrent PHP-FPM workers share a few dozen
  real MySQL connections. Safety is automatic — sessions holding state are
  pinned to their connection: open transactions (tracked by keyword *and* by
  the server status flags, so `autocommit=0` is caught too), temporary
  tables, `LOCK TABLES`/`GET_LOCK()`, prepared statements, user-defined
  variables. Session settings like `SET NAMES` don't pin: piko tracks them
  and replays them when the session lands on a different connection (in the
  steady state every WordPress session issues the same `SET NAMES`, so no
  extra roundtrip happens). After `SQL_CALC_FOUND_ROWS`, `LAST_INSERT_ID()`
  or an INSERT, the connection is held for the companion statement.
- **Keepalive pings** — idle connections receive periodic `COM_PING`s, both
  while parked in the pool and while attached to an inactive client. A PHP
  worker that holds its connection during a long computation (importing a CSV,
  calling an external API...) no longer gets *"MySQL server has gone away"*
  on the next query: piko kept the session alive in the meantime. If the
  backend connection drops anyway, the next command transparently attaches a
  fresh one from the pool.
- **Client authentication** — by default clients authenticate against piko
  with the backend credentials, so the simple setup needs them only once.
  Optionally, a `users` list defines separate proxy-only accounts: MySQL never
  sees those passwords, and the backend credentials never leave piko.
- **WordPress-aware query cache** — the autoloaded options query (the
  hottest query of every WordPress pageload) and transient reads are served
  from RAM. Invalidation is write-driven: every write flowing through piko
  drops exactly the entries it affects (per option name on `wp_options`, per
  table elsewhere), with a TTL as safety net. Reads inside transactions
  always bypass the cache, and unparseable writes flush it entirely — when
  in doubt piko prefers a database roundtrip over a stale answer.
- **Rule-based caching for WooCommerce** — drop-in files in
  `/etc/piko/conf.d/` add cache rules as regex + TTL + invalidation tables.
  `piko --init` installs a WooCommerce profile covering the product-data
  queries that hammer shops (postmeta lookups, attribute taxonomies, term
  lookups, product listings). The `{prefix}` placeholder expands to
  `cache.table_prefix`, so rules work with any `$table_prefix`. Carts and
  customer sessions are deliberately never cached.
- **Paginated listing caching** — WooCommerce product listings use
  `SQL_CALC_FOUND_ROWS` + `FOUND_ROWS()` for page counts. piko caches the
  rows **and** the pagination count together and replays both, so the shop
  and category pages (identical for every visitor) are served from memory
  with correct "page X of Y" counts. Serving a listing without its matching
  count is impossible by construction — the entry is withheld until the
  count is paired.
- **Hot reload** — `SIGHUP` (or `systemctl reload piko`) re-reads the
  conf.d drop-ins (cache rules and rewrites) without dropping a single
  client connection. A failed reload keeps the previous rules.
- **Cache warm-up** — every option write invalidates the autoloaded options
  snapshot; with `cache.warmup` enabled piko re-fetches it in the background
  immediately, so the next visitor never pays the query.
- **Circuit breaker** — when MySQL is down, waiting for per-request timeouts
  melts PHP-FPM. After `pool.breaker.failures` consecutive connection
  failures piko fails fast with a clean error and probes the backend until
  it recovers. `listen.max_connections` additionally caps concurrent client
  connections, and `pool.max_query_time` kills (KILL QUERY) any statement
  running longer than the limit so one runaway query can't hold a pool
  connection hostage.
- **Query firewall** — a `block` list in the conf.d drop-ins rejects
  matching queries with a clean MySQL error before they reach the backend.
  Combined with hot reload it's the emergency brake for a runaway plugin
  query: add the rule and `systemctl reload piko`, no restart.
- **TLS** — `listen.tls` (cert + key) encrypts client connections;
  `backend.tls` encrypts the connection toward a remote MySQL, with an
  optional custom CA. Both are off by default (localhost deployments need
  no encryption).
- **`piko status`** — a read-only unix socket (`status.socket`) exposes
  live state; `piko status` prints connected clients, pinned sessions,
  pool/breaker state and per-source cache hit ratios without waiting for
  the periodic log report.

## Quick start

Download the Linux binary from the Releases page and let `--init` install
everything (it must run as root):

```sh
sudo ./piko --init            # copies itself to /sbin/piko, creates the piko
                              # user, /etc/piko (config + conf.d), /var/log/piko,
                              # the systemd unit and logrotate config
sudo $EDITOR /etc/piko/config.yaml
sudo systemctl enable --now piko
sudo systemctl reload piko    # SIGHUP: hot-reloads conf.d rules
```

`piko --init` **overwrites** everything it manages on every run, so it always
resets to a known-good install; re-run it to upgrade after downloading a new
binary. `-config <path>` changes where the config lives (the generated unit
follows it).

Check a running instance:

```sh
piko status                   # clients, pool, breaker, cache stats
```

Point WordPress at piko in `wp-config.php`:

```php
define( 'DB_HOST', '127.0.0.1:3306' ); // piko's listen address
define( 'DB_USER', 'wordpress' );      // a user from piko's config.yaml
```

## Configuration

See [config.default.yaml](cmd/piko/config.default.yaml) (the `--init`
template) for the full commented list:

```yaml
listen:
  address: "0.0.0.0:3306"

backend:
  address: "10.0.0.10:3306"
  username: "wordpress"    # the MySQL user you already use today
  password: "change-me"

pool:
  max_open: 100
  max_idle: 10
  ping_interval: 30s
  idle_timeout: 5m
  acquire_timeout: 5s
  multiplexing: true

cache:
  enabled: true
  table_prefix: "wp_"
  autoload_options: true
  transients: true
  default_ttl: 5m
  max_entries: 10000
  max_result_bytes: 1048576

log:
  level: info           # debug | info | warn | error
  format: text          # text | json
  path: /var/log/piko   # directory for piko.log ("stdout" for console output)
```

To keep the real database credentials out of `wp-config.php`, add proxy-only
accounts — clients then authenticate against piko with these, while piko
still connects to MySQL with the `backend` credentials:

```yaml
users:
  - username: "wordpress"
    password: "proxy-only-password"
```

### Profiling and index suggestions

With `profiling.enabled: true`, piko turns its privileged position — it sees
every query — into optimization guidance, written to the standard log:

- **slow query log**: statements slower than `slow_query` are logged
  immediately with their duration and full text;
- **periodic report** (`report_interval`): per-query statistics aggregated by
  normalized query (literals replaced by `?`): calls, total/avg/max time,
  rows, cache hit ratio, heaviest queries first;
- **index suggestions** (`suggest_indexes`): piko runs `EXPLAIN` on the
  heaviest queries and inspects the schema, logging each finding once with
  the `ALTER TABLE` statement ready to run:
  - *add*: a query scans a large table without using any index;
  - *drop (redundant)*: an index is a left prefix of another one;
  - *drop (unused)*: an index with zero reads since the MySQL server started
    (requires `performance_schema`); verify over a full business cycle
    before dropping.
  - *fulltext*: a `LIKE '%term%'` search scans a large table and no B-tree
    index can help — piko suggests a `FULLTEXT` index (or a search plugin).
    This is the classic WooCommerce product-search bottleneck.

```yaml
profiling:
  enabled: true
  slow_query: 500ms
  report_interval: 10m
  top_queries: 20
  suggest_indexes: true
```

Unique indexes and primary keys are never suggested for removal, and
suggestions on JOIN queries are limited to flagging the scan — piko does not
guess composite indexes across tables.

### Query rewriting

conf.d drop-ins can also declare `rewrites:` — regex replacements applied to
every query before execution, useful to fix known-bad SQL coming from
plugins you cannot change:

```yaml
rewrites:
  - name: remove-order-by-rand
    match: "(?i)\\s*ORDER\\s+BY\\s+RAND\\s*\\(\\s*\\)"
    replace: ""
```

`replace` supports capture references (`$1`); an empty string deletes the
match. Rewrites change query semantics, so none ship enabled: with profiling
on, piko detects the known antipatterns — `ORDER BY RAND()`,
`SQL_CALC_FOUND_ROWS`, leading-wildcard `LIKE`, huge `OFFSET` pagination —
and logs a `rewrite suggestion` entry with the exact conf.d rule to enable
(or advice when no automatic fix is safe):

```
WARN rewrite suggestion pattern=order-by-rand calls=1250
     reason="ORDER BY RAND() reads and sorts the whole table on every execution; ..."
     query="SELECT ID FROM wp_posts ORDER BY RAND() LIMIT ?"
     conf_d="rewrites: [{name: remove-order-by-rand, match: '(?i)\s*ORDER\s+BY\s+RAND\s*\(\s*\)', replace: ''}]"
```

Prepared statements are never rewritten. The WooCommerce drop-in ships the
two most common rewrites commented out, ready to enable.

### Custom cache rules (conf.d)

Every `*.yaml` file in the conf.d directory adds cache rules. A rule caches
SELECTs matching an RE2 regex and drops them when a write touches one of the
`invalidate_on` tables:

```yaml
name: my-rules
rules:
  - name: attribute-taxonomies
    match: "(?i)^SELECT \\* FROM wp_woocommerce_attribute_taxonomies"
    ttl: 10m
    invalidate_on: [wp_woocommerce_attribute_taxonomies]
```

Only add rules for queries whose results are the same for every visitor —
never cache per-user data (carts, sessions, anything keyed to a customer).

## Current limitations

- Result sets are buffered in memory while being relayed; very large `SELECT`s
  (e.g. full-table dumps) are better run directly against MySQL.
- Multi-statement queries, `LOAD DATA LOCAL INFILE` and the binlog/replication
  commands are not supported.
- Clients should always select a database (WordPress does): sessions without
  one may observe the database left by a previous session on a reused
  connection.

## Development

```sh
make build   # binary in bin/piko
make test    # unit tests
```

Every push to `main` runs the tests and builds Linux binaries (amd64 and
arm64), downloadable as artifacts from the Actions run. Pushing a `v*` tag
publishes the binaries on the Releases page via
[GoReleaser](https://goreleaser.com/), with checksums. Install a downloaded
binary with `sudo ./piko --init` (see Quick start); the systemd unit and
logrotate config are embedded in the binary.

## License

[MIT](LICENSE)
