package cache

import (
	"regexp"
	"strings"
)

// Kind classifies a statement for caching and invalidation purposes.
type Kind int

const (
	// KindOther is anything piko neither caches nor tracks (SHOW, SET...).
	KindOther Kind = iota
	// KindSelect is a plain read.
	KindSelect
	// KindWrite is any statement that may change data (DML, DDL, CALL...).
	KindWrite
	// KindBegin starts a transaction.
	KindBegin
	// KindCommit commits one.
	KindCommit
	// KindRollback rolls one back.
	KindRollback
	// KindUnsafe changes session semantics piko does not track (autocommit,
	// XA): caching is disabled for the rest of the session.
	KindUnsafe
)

var autocommitRe = regexp.MustCompile(`(?i)\bautocommit\b`)

// Classify inspects the first keyword of a statement.
func Classify(query string) Kind {
	q := stripLeading(query)
	word, rest := firstWord(q)

	switch strings.ToUpper(word) {
	case "SELECT":
		return KindSelect
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "TRUNCATE",
		"ALTER", "DROP", "CREATE", "RENAME", "LOAD", "CALL":
		return KindWrite
	case "BEGIN":
		return KindBegin
	case "START":
		if next, _ := firstWord(rest); strings.EqualFold(next, "TRANSACTION") {
			return KindBegin
		}
		return KindOther
	case "COMMIT":
		return KindCommit
	case "ROLLBACK":
		// ROLLBACK TO SAVEPOINT keeps the transaction open.
		if next, _ := firstWord(rest); strings.EqualFold(next, "TO") {
			return KindOther
		}
		return KindRollback
	case "SET":
		if autocommitRe.MatchString(rest) {
			return KindUnsafe
		}
		return KindOther
	case "XA":
		return KindUnsafe
	default:
		return KindOther
	}
}

// stripLeading removes whitespace and /* ... */ comments from the front.
func stripLeading(q string) string {
	for {
		q = strings.TrimLeft(q, " \t\r\n")
		if strings.HasPrefix(q, "/*") {
			end := strings.Index(q, "*/")
			if end < 0 {
				return ""
			}
			q = q[end+2:]
			continue
		}
		return q
	}
}

// firstWord returns the leading identifier (skipping whitespace) and
// everything after it.
func firstWord(q string) (string, string) {
	q = strings.TrimLeft(q, " \t\r\n")
	i := 0
	for i < len(q) {
		ch := q[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' {
			i++
			continue
		}
		break
	}
	return q[:i], q[i:]
}

// Write-statement table extraction. Only plain single-table statements are
// recognized; anything else (JOINs, qualified names, CALL, LOAD DATA...)
// makes the caller fall back to flushing the whole cache.
var (
	insertTableRe = regexp.MustCompile(`(?i)^(?:INSERT|REPLACE)(?:\s+(?:LOW_PRIORITY|DELAYED|HIGH_PRIORITY|IGNORE))*\s+INTO\s+\x60?([\w$]+)\x60?`)
	updateTableRe = regexp.MustCompile(`(?i)^UPDATE\s+(?:LOW_PRIORITY\s+|IGNORE\s+)*\x60?([\w$]+)\x60?\s+SET\b`)
	deleteTableRe = regexp.MustCompile(`(?i)^DELETE\s+FROM\s+\x60?([\w$]+)\x60?(?:\s+WHERE\b|\s*$)`)
	ddlTableRe    = regexp.MustCompile(`(?i)^(?:TRUNCATE(?:\s+TABLE)?|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+\x60?([\w$]+)\x60?`)
)

// extractTable returns the single table touched by a write statement.
// ok is false when the target cannot be determined safely.
func extractTable(query string) (string, bool) {
	q := stripLeading(query)
	for _, re := range []*regexp.Regexp{insertTableRe, updateTableRe, deleteTableRe, ddlTableRe} {
		if m := re.FindStringSubmatch(q); m != nil {
			return m[1], true
		}
	}
	return "", false
}

// Option-name extraction for writes on the options table, matching the SQL
// wpdb generates for update_option/set_transient/delete_option.
var (
	optionNameEqRe     = regexp.MustCompile(`(?i)option_name\s*=\s*'([^'\\]*)'`)
	insertOptionNameRe = regexp.MustCompile(`(?i)\(\s*\x60?option_name\x60?\s*,[^)]*\)\s*VALUES\s*\(\s*'([^'\\]*)'`)
)

// extractOptionNames returns the option names referenced by a write on the
// options table; empty means "could not tell".
func extractOptionNames(query string) []string {
	var names []string
	for _, m := range optionNameEqRe.FindAllStringSubmatch(query, -1) {
		names = append(names, m[1])
	}
	if m := insertOptionNameRe.FindStringSubmatch(query); m != nil {
		names = append(names, m[1])
	}
	return names
}

var quotedItemRe = regexp.MustCompile(`'([^'\\]*)'`)

// extractQuotedList returns the single-quoted items of an IN (...) list.
func extractQuotedList(list string) []string {
	var names []string
	for _, m := range quotedItemRe.FindAllStringSubmatch(list, -1) {
		names = append(names, m[1])
	}
	return names
}

// isTransientName reports whether an option holds a WordPress transient.
// This also covers the "_transient_timeout_*" / "_site_transient_timeout_*"
// companion rows, since they share the "_transient_"/"_site_transient_"
// prefixes.
func isTransientName(name string) bool {
	return strings.HasPrefix(name, "_transient_") ||
		strings.HasPrefix(name, "_site_transient_")
}

// writeHitsAutoload reports whether a write to the options table can affect
// an autoloaded option (and thus the alloptions snapshot). Writes that only
// touch transients are treated as non-autoload: WordPress stores transients
// that have an expiration with autoload='off' (see the INSERT ... VALUES
// (..., 'off') that WooCommerce emits constantly), so invalidating the
// alloptions snapshot for them is both unnecessary and ruinous for the hit
// rate. A write piko cannot attribute (empty names) is treated as hitting
// autoload, to stay safe.
func writeHitsAutoload(names []string) bool {
	if len(names) == 0 {
		return true
	}
	for _, n := range names {
		if !isTransientName(n) {
			return true
		}
	}
	return false
}

// volatileSelectRe matches reads that must never be served from cache:
// locking reads and session/time dependent functions.
var volatileSelectRe = regexp.MustCompile(`(?i)\b(?:FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE|LAST_INSERT_ID\s*\(|FOUND_ROWS\s*\(|ROW_COUNT\s*\(|CONNECTION_ID\s*\(|RAND\s*\(|UUID\w*\s*\(|NOW\s*\(|SYSDATE\s*\(|CURDATE\s*\(|CURTIME\s*\(|CURRENT_\w+|GET_LOCK\s*\()`)

var (
	calcFoundRowsRe  = regexp.MustCompile(`(?i)\bSQL_CALC_FOUND_ROWS\b`)
	foundRowsQueryRe = regexp.MustCompile(`(?i)^\s*SELECT\s+FOUND_ROWS\s*\(\s*\)\s*$`)
)

// unsafeForCache reports whether a query is uncacheable through the normal
// path. SQL_CALC_FOUND_ROWS queries are excluded here: they can only be
// cached via the search path, which also caches the paired FOUND_ROWS().
func unsafeForCache(query string) bool {
	return volatileSelectRe.MatchString(query) || calcFoundRowsRe.MatchString(query)
}

// unsafeForSearch reports whether a SQL_CALC_FOUND_ROWS query is uncacheable
// even on the search path (it carries other volatile constructs).
func unsafeForSearch(query string) bool {
	return volatileSelectRe.MatchString(query)
}

// HasCalcFoundRows reports whether the query uses SQL_CALC_FOUND_ROWS.
func HasCalcFoundRows(query string) bool { return calcFoundRowsRe.MatchString(query) }

// IsFoundRowsQuery reports whether the query is exactly SELECT FOUND_ROWS().
func IsFoundRowsQuery(query string) bool { return foundRowsQueryRe.MatchString(query) }
