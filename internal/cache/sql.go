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

// isTransientName reports whether an option holds a WordPress transient.
func isTransientName(name string) bool {
	return strings.HasPrefix(name, "_transient_") ||
		strings.HasPrefix(name, "_site_transient_")
}

// unsafeSelectRe matches reads that must never be served from cache:
// locking reads and session/time dependent functions.
var unsafeSelectRe = regexp.MustCompile(`(?i)\b(?:FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE|LAST_INSERT_ID\s*\(|FOUND_ROWS\s*\(|ROW_COUNT\s*\(|CONNECTION_ID\s*\(|RAND\s*\(|UUID\w*\s*\(|NOW\s*\(|SYSDATE\s*\(|CURDATE\s*\(|CURTIME\s*\(|CURRENT_\w+|SQL_CALC_FOUND_ROWS|GET_LOCK\s*\()`)
