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
  lookups). Carts and customer sessions are deliberately never cached.

## Quick start

```sh
sudo ./piko --init                    # creates /etc/piko/config.yaml
                                      # and /etc/piko/conf.d/woocommerce.yaml
sudo $EDITOR /etc/piko/config.yaml
sudo ./piko                           # logs to /var/log/piko/piko.log
```

`-config <path>` overrides the configuration path, both at startup and
with `--init`.

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
publishes them on the Releases page via
[GoReleaser](https://goreleaser.com/), with checksums.

## License

[MIT](LICENSE)
