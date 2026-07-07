package profile

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// fullScanRows is the EXPLAIN row estimate above which a scan without an
// index is worth a suggestion; smaller tables are fine without one.
const fullScanRows = 1000

// dbExecutor is the slice of *pool.Conn the advisor needs; tests provide
// fakes.
type dbExecutor interface {
	UseDB(dbName string) error
	Execute(command string, args ...any) (*mysql.Result, error)
}

// advisor turns EXPLAIN output and schema metadata into log suggestions.
// Each suggestion is emitted once per process lifetime.
type advisor struct {
	log *slog.Logger

	mu   sync.Mutex
	seen map[string]struct{}
}

func newAdvisor(log *slog.Logger) *advisor {
	return &advisor{log: log, seen: make(map[string]struct{})}
}

// once reports whether a suggestion key is new, marking it as emitted.
func (a *advisor) once(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[key]; ok {
		return false
	}
	a.seen[key] = struct{}{}
	return true
}

// explainQuery runs EXPLAIN on the query's worst execution and suggests an
// index when MySQL scans a large table without using any key.
func (a *advisor) explainQuery(conn dbExecutor, st *queryStat) error {
	if err := conn.UseDB(st.db); err != nil {
		return err
	}
	r, err := conn.Execute("EXPLAIN " + st.sample)
	if err != nil {
		return err
	}
	if r.Resultset == nil {
		return nil
	}

	for row := range r.Values {
		table, _ := r.GetStringByName(row, "table")
		accessType, _ := r.GetStringByName(row, "type")
		key, _ := r.GetStringByName(row, "key")
		rows, _ := r.GetUintByName(row, "rows")

		fullScan := key == "" || accessType == "ALL"
		if !fullScan || rows < fullScanRows || table == "" || strings.HasPrefix(table, "<") {
			continue
		}

		columns := suggestColumns(st.sample, table)
		if len(columns) == 0 {
			if a.once("scan|" + st.db + "|" + st.digest) {
				a.log.Warn("index suggestion",
					"action", "review",
					"db", st.db,
					"table", table,
					"reason", fmt.Sprintf("scans ~%d rows without using an index", rows),
					"query", st.digest)
			}
			continue
		}

		idxName := "idx_piko_" + strings.Join(columns, "_")
		if a.once("add|" + st.db + "|" + table + "|" + strings.Join(columns, ",")) {
			a.log.Warn("index suggestion",
				"action", "add",
				"db", st.db,
				"table", table,
				"columns", strings.Join(columns, ", "),
				"reason", fmt.Sprintf("scans ~%d rows without using an index", rows),
				"query", st.digest,
				"sql", fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (`%s`);",
					table, idxName, strings.Join(columns, "`, `")))
		}
	}
	return nil
}

// Column extraction from the query text: equality and IN predicates first,
// then the first ORDER BY column. Good enough for wpdb-shaped SQL.
var (
	wherePredicateRe = regexp.MustCompile(`(?i)(?:WHERE|AND|OR)[\s(]+\x60?([A-Za-z_][\w$]*)\x60?\s*(?:=|IN\s*\()`)
	orderByRe        = regexp.MustCompile(`(?i)ORDER\s+BY\s+\x60?([A-Za-z_][\w$]*)\x60?`)
	fromSingleRe     = regexp.MustCompile(`(?i)\b(?:FROM|UPDATE)\s+\x60?([A-Za-z_][\w$]*)\x60?\s*(?:WHERE|SET|ORDER|GROUP|LIMIT|$)`)
)

// suggestColumns proposes index columns for a single-table query on table.
// Multi-table queries return nil: guessing across JOINs is not safe.
func suggestColumns(query, table string) []string {
	m := fromSingleRe.FindStringSubmatch(query)
	if m == nil || !strings.EqualFold(m[1], table) {
		return nil
	}

	var columns []string
	seen := map[string]struct{}{}
	add := func(col string) {
		col = strings.ToLower(col)
		if _, dup := seen[col]; dup || len(columns) >= 3 {
			return
		}
		seen[col] = struct{}{}
		columns = append(columns, col)
	}

	for _, match := range wherePredicateRe.FindAllStringSubmatch(query, -1) {
		add(match[1])
	}
	if m := orderByRe.FindStringSubmatch(query); m != nil {
		add(m[1])
	}
	return columns
}

// reviewSchema looks for duplicate indexes (via information_schema) and
// unused ones (via performance_schema) in db.
func (a *advisor) reviewSchema(conn dbExecutor, db string) error {
	if err := conn.UseDB(db); err != nil {
		return err
	}

	indexes, err := loadIndexes(conn, db)
	if err != nil {
		return err
	}
	a.suggestDuplicates(db, indexes)

	// performance_schema may be disabled or not granted: not an error.
	if err := a.suggestUnused(conn, db, indexes); err != nil {
		a.log.Debug("unused index check skipped", "db", db, "error", err)
	}
	return nil
}

// tableIndex is one index with its ordered columns.
type tableIndex struct {
	table     string
	name      string
	columns   []string
	nonUnique bool
}

func loadIndexes(conn dbExecutor, db string) ([]tableIndex, error) {
	r, err := conn.Execute(
		"SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE "+
			"FROM information_schema.statistics WHERE TABLE_SCHEMA = ? "+
			"ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX", db)
	if err != nil {
		return nil, err
	}
	if r.Resultset == nil {
		return nil, nil
	}

	var indexes []tableIndex
	byKey := map[string]int{}
	for row := range r.Values {
		table, _ := r.GetStringByName(row, "TABLE_NAME")
		name, _ := r.GetStringByName(row, "INDEX_NAME")
		column, _ := r.GetStringByName(row, "COLUMN_NAME")
		nonUnique, _ := r.GetUintByName(row, "NON_UNIQUE")

		key := table + "." + name
		i, ok := byKey[key]
		if !ok {
			i = len(indexes)
			byKey[key] = i
			indexes = append(indexes, tableIndex{table: table, name: name, nonUnique: nonUnique == 1})
		}
		indexes[i].columns = append(indexes[i].columns, column)
	}
	return indexes, nil
}

// suggestDuplicates flags non-unique indexes whose columns are a left
// prefix of (or identical to) another index on the same table.
func (a *advisor) suggestDuplicates(db string, indexes []tableIndex) {
	for i := range indexes {
		dup := &indexes[i]
		if dup.name == "PRIMARY" || !dup.nonUnique {
			continue
		}
		for j := range indexes {
			other := &indexes[j]
			if i == j || dup.table != other.table || !isPrefix(dup.columns, other.columns) {
				continue
			}
			// Identical column lists: keep only one of the pair.
			if len(dup.columns) == len(other.columns) &&
				other.nonUnique && other.name != "PRIMARY" && dup.name < other.name {
				continue
			}

			if a.once("drop|" + db + "|" + dup.table + "|" + dup.name) {
				a.log.Warn("index suggestion",
					"action", "drop",
					"db", db,
					"table", dup.table,
					"index", dup.name,
					"reason", fmt.Sprintf("redundant: %s (%s) already covers it",
						other.name, strings.Join(other.columns, ", ")),
					"sql", fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;", dup.table, dup.name))
			}
			break
		}
	}
}

// suggestUnused flags non-unique indexes with zero reads since the MySQL
// server started, according to performance_schema.
func (a *advisor) suggestUnused(conn dbExecutor, db string, indexes []tableIndex) error {
	r, err := conn.Execute(
		"SELECT OBJECT_NAME, INDEX_NAME "+
			"FROM performance_schema.table_io_waits_summary_by_index_usage "+
			"WHERE OBJECT_SCHEMA = ? AND INDEX_NAME IS NOT NULL "+
			"AND INDEX_NAME <> 'PRIMARY' AND COUNT_STAR = 0", db)
	if err != nil {
		return err
	}
	if r.Resultset == nil {
		return nil
	}

	droppable := map[string]bool{}
	for _, idx := range indexes {
		if idx.nonUnique && idx.name != "PRIMARY" {
			droppable[idx.table+"."+idx.name] = true
		}
	}

	for row := range r.Values {
		table, _ := r.GetStringByName(row, "OBJECT_NAME")
		name, _ := r.GetStringByName(row, "INDEX_NAME")
		if !droppable[table+"."+name] {
			continue // unique or constraint index: never suggest dropping
		}
		if a.once("unused|" + db + "|" + table + "|" + name) {
			a.log.Warn("index suggestion",
				"action", "drop",
				"db", db,
				"table", table,
				"index", name,
				"reason", "never used since the MySQL server started (performance_schema); verify over a full business cycle before dropping",
				"sql", fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;", table, name))
		}
	}
	return nil
}

// isPrefix reports whether a is a left prefix of b (or equal to it).
func isPrefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

// isConnError distinguishes broken connections from server-side errors.
func isConnError(err error) bool {
	var myErr *mysql.MyError
	return !errors.As(err, &myErr)
}
