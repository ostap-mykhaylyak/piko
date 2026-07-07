package profile

import "regexp"

// antipattern is a known-bad SQL construct the profiler flags in the logs.
// When a safe(ish) automatic fix exists, confD carries the ready-to-paste
// conf.d rewrite rule; otherwise the suggestion is advice only.
type antipattern struct {
	name   string
	detect *regexp.Regexp
	reason string
	confD  string // empty = nothing to configure, review the application
}

var antipatterns = []antipattern{
	{
		name:   "order-by-rand",
		detect: regexp.MustCompile(`(?i)ORDER\s+BY\s+RAND\s*\(\s*\)`),
		reason: "ORDER BY RAND() reads and sorts the whole table on every execution; " +
			"the rewrite removes it (row order becomes undefined — verify the application tolerates that)",
		confD: `rewrites: [{name: remove-order-by-rand, match: '(?i)\s*ORDER\s+BY\s+RAND\s*\(\s*\)', replace: ''}]`,
	},
	{
		name:   "sql-calc-found-rows",
		detect: regexp.MustCompile(`(?i)\bSQL_CALC_FOUND_ROWS\b`),
		reason: "SQL_CALC_FOUND_ROWS forces MySQL to count all matching rows past the LIMIT; " +
			"in WordPress prefer 'no_found_rows' => true in WP_Query; apply the rewrite ONLY if FOUND_ROWS() is never read afterwards",
		confD: `rewrites: [{name: remove-sql-calc-found-rows, match: '(?i)SQL_CALC_FOUND_ROWS\s+', replace: ''}]`,
	},
	{
		name:   "leading-wildcard-like",
		detect: regexp.MustCompile(`(?i)LIKE\s+'%`),
		reason: "LIKE with a leading wildcard cannot use an index and scans every row; " +
			"consider a FULLTEXT index or a search plugin instead",
	},
	{
		name:   "large-offset",
		detect: regexp.MustCompile(`(?i)LIMIT\s+\d{5,}\s*,|\bOFFSET\s+\d{5,}\b`),
		reason: "pagination with a large OFFSET reads and discards all preceding rows; " +
			"use keyset pagination (WHERE id > last_seen ORDER BY id LIMIT n)",
	},
}

// suggestRewrites scans the interval's queries for antipatterns and logs
// the suggested rewrite rule (or advice), once per query digest.
func (p *Profiler) suggestRewrites(stats []*queryStat) {
	for _, st := range stats {
		for i := range antipatterns {
			ap := &antipatterns[i]
			if !ap.detect.MatchString(st.sample) {
				continue
			}
			if !p.advisor.once("rewrite|" + ap.name + "|" + st.digest) {
				continue
			}
			if ap.confD != "" {
				p.log.Warn("rewrite suggestion",
					"pattern", ap.name,
					"calls", st.calls,
					"reason", ap.reason,
					"query", st.digest,
					"conf_d", ap.confD)
			} else {
				p.log.Warn("rewrite suggestion",
					"pattern", ap.name,
					"calls", st.calls,
					"reason", ap.reason,
					"query", st.digest)
			}
		}
	}
}
