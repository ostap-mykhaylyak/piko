package profile

import (
	"regexp"
	"strings"
)

var inListRe = regexp.MustCompile(`(?i)IN\s*\(\s*\?(?:\s*,\s*\?)*\s*\)`)

// Fingerprint normalizes a query so different literal values map to the
// same digest: string and numeric literals become ?, IN lists collapse to a
// single placeholder, whitespace is squeezed.
func Fingerprint(query string) string {
	var b strings.Builder
	b.Grow(len(query))

	var prev byte
	i := 0
	for i < len(query) {
		ch := query[i]
		switch {
		case ch == '\'' || ch == '"':
			i = skipString(query, i)
			b.WriteByte('?')
			prev = '?'
		case ch >= '0' && ch <= '9' && !isIdentChar(prev):
			for i < len(query) && (query[i] >= '0' && query[i] <= '9' || query[i] == '.') {
				i++
			}
			b.WriteByte('?')
			prev = '?'
		case ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n':
			for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\r' || query[i] == '\n') {
				i++
			}
			if prev != ' ' && prev != 0 {
				b.WriteByte(' ')
				prev = ' '
			}
		default:
			b.WriteByte(ch)
			prev = ch
			i++
		}
	}

	out := strings.TrimRight(b.String(), " ")
	return inListRe.ReplaceAllString(out, "IN (?)")
}

// skipString returns the position after a quoted literal starting at start,
// honoring backslash escapes and doubled quotes.
func skipString(q string, start int) int {
	quote := q[start]
	i := start + 1
	for i < len(q) {
		switch q[i] {
		case '\\':
			i += 2
		case quote:
			if i+1 < len(q) && q[i+1] == quote {
				i += 2 // doubled quote inside the literal
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return i
}

func isIdentChar(ch byte) bool {
	return ch == '_' || ch == '$' ||
		(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9')
}
