package proxy

import (
	"regexp"
	"strings"
)

// Multiplexing safety: a backend connection can be returned to the pool
// between queries only when the session left no state on it. This file
// classifies statements into what they do to that state.
//
// The rules err on the side of pinning: a false positive costs one
// dedicated connection (the pre-multiplexing behavior), a false negative
// corrupts sessions.

// pinDetectRe matches constructs that tie the session to its connection for
// good: temporary tables, table/user locks, session-scoped transaction
// settings.
var pinDetectRe = regexp.MustCompile(`(?i)\b(?:CREATE\s+TEMPORARY\s+TABLE|LOCK\s+TABLES|GET_LOCK\s*\(|SET\s+TRANSACTION\b)`)

// holdDetectRe matches constructs whose companion query must run on the
// same connection (typically the very next statement): the connection is
// kept attached for one more round.
var holdDetectRe = regexp.MustCompile(`(?i)\b(?:SQL_CALC_FOUND_ROWS|FOUND_ROWS\s*\(|LAST_INSERT_ID\s*\(|ROW_COUNT\s*\()`)

// userVarRe matches user-defined variables (@x) outside of @@system_vars.
// It is applied to the fingerprinted query, where string literals are
// already collapsed, so e-mail addresses in values cannot false-positive.
var userVarRe = regexp.MustCompile(`(^|[^@\w])@[A-Za-z0-9_$.]`)

// Trackable SET statements: session-scoped settings piko can replay on
// another connection to reproduce the session environment. Everything else
// SET-shaped pins.
var (
	setRe          = regexp.MustCompile(`(?i)^\s*SET\b`)
	trackableSetRe = regexp.MustCompile(`(?i)^\s*SET\s+(?:NAMES\b|(?:SESSION\s+|@@SESSION\.)?\s*(?:character_set_\w+|collation_\w+|sql_mode|time_zone|wait_timeout|group_concat_max_len|sql_big_selects|net_read_timeout|net_write_timeout)\s*=)`)
	setGlobalRe    = regexp.MustCompile(`(?i)^\s*SET\s+(?:GLOBAL\b|@@GLOBAL\.)`)
)

// setAction describes how a SET statement affects multiplexing.
type setAction int

const (
	setNone   setAction = iota // not a SET statement
	setTrack                   // session setting piko replays on reuse
	setIgnore                  // server-wide, leaves no session state
	setPin                     // untrackable: pin the session
)

func classifySet(query string) setAction {
	if !setRe.MatchString(query) {
		return setNone
	}
	if trackableSetRe.MatchString(query) {
		return setTrack
	}
	if setGlobalRe.MatchString(query) {
		return setIgnore
	}
	return setPin
}

// varSignature identifies a replayable session environment: the ordered,
// deduplicated list of tracked SET statements.
func varSignature(statements []string) string {
	return strings.Join(statements, "\x00")
}
