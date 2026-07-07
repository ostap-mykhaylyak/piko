package proxy

import (
	"testing"

	"github.com/ostap-mykhaylyak/piko/internal/profile"
)

func TestClassifySet(t *testing.T) {
	cases := map[string]setAction{
		"SET NAMES utf8mb4": setTrack,
		"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_520_ci'":   setTrack,
		"SET SESSION sql_mode = 'TRADITIONAL'":                   setTrack,
		"SET sql_mode = ''":                                      setTrack,
		"SET character_set_results = utf8mb4":                    setTrack,
		"SET time_zone = '+00:00'":                               setTrack,
		"SET SESSION group_concat_max_len = 1048576":             setTrack,
		"SET GLOBAL max_connections = 500":                       setIgnore,
		"SET @@GLOBAL.sort_buffer_size = 1000000":                setIgnore,
		"SET @user_var = 1":                                      setPin,
		"SET autocommit = 0":                                     setPin,
		"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED": setPin,
		"SELECT 1":                            setNone,
		"UPDATE t SET a = 1":                  setNone,
		"INSERT INTO offset_t (a) VALUES (1)": setNone,
	}
	for q, want := range cases {
		if got := classifySet(q); got != want {
			t.Errorf("classifySet(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestPinDetection(t *testing.T) {
	pins := []string{
		"CREATE TEMPORARY TABLE tmp (id INT)",
		"create temporary table tmp AS SELECT 1",
		"LOCK TABLES wp_posts WRITE",
		"SELECT GET_LOCK('sync', 10)",
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
	}
	for _, q := range pins {
		if !pinDetectRe.MatchString(q) {
			t.Errorf("pinDetectRe should match %q", q)
		}
	}

	clean := []string{
		"SELECT * FROM wp_posts",
		"CREATE TABLE real_table (id INT)",
		"UPDATE wp_options SET option_value = 'locked' WHERE option_name = 'x'",
	}
	for _, q := range clean {
		if pinDetectRe.MatchString(q) {
			t.Errorf("pinDetectRe should not match %q", q)
		}
	}
}

func TestHoldDetection(t *testing.T) {
	holds := []string{
		"SELECT SQL_CALC_FOUND_ROWS ID FROM wp_posts LIMIT 10",
		"SELECT FOUND_ROWS()",
		"SELECT LAST_INSERT_ID()",
		"SELECT ROW_COUNT()",
	}
	for _, q := range holds {
		if !holdDetectRe.MatchString(q) {
			t.Errorf("holdDetectRe should match %q", q)
		}
	}
	if holdDetectRe.MatchString("SELECT * FROM wp_posts") {
		t.Error("holdDetectRe should not match a plain SELECT")
	}
}

func TestUserVarDetection(t *testing.T) {
	// Applied to the fingerprint, like trackSafety does.
	matches := []string{
		"SELECT @rank := 1 FROM t",
		"SELECT * FROM t WHERE id = @last_id",
	}
	for _, q := range matches {
		if !userVarRe.MatchString(profile.Fingerprint(q)) {
			t.Errorf("userVarRe should match fingerprint of %q", q)
		}
	}

	// E-mail addresses and @@system variables must not pin.
	noMatches := []string{
		"SELECT * FROM wp_users WHERE user_email = 'a@b.com'",
		"SELECT @@version",
		"SELECT @@session.sql_mode",
	}
	for _, q := range noMatches {
		if userVarRe.MatchString(profile.Fingerprint(q)) {
			t.Errorf("userVarRe should not match fingerprint of %q (fingerprint: %q)",
				q, profile.Fingerprint(q))
		}
	}
}
