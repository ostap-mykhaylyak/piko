package cache

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/ostap-mykhaylyak/piko/internal/config"
	"github.com/ostap-mykhaylyak/piko/internal/pool"
)

type warmFakeHandler struct{}

func (warmFakeHandler) UseDB(string) error { return nil }
func (warmFakeHandler) HandleQuery(string) (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"option_name", "option_value"},
		[][]any{{"siteurl", "https://example.com"}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}
func (warmFakeHandler) HandleFieldList(string, string) ([]*mysql.Field, error) { return nil, nil }
func (warmFakeHandler) HandleStmtPrepare(string) (int, int, any, error)        { return 0, 0, nil, nil }
func (warmFakeHandler) HandleStmtExecute(any, string, []any) (*mysql.Result, error) {
	return nil, nil
}
func (warmFakeHandler) HandleStmtClose(any) error             { return nil }
func (warmFakeHandler) HandleOtherCommand(byte, []byte) error { return nil }

// TestWarmer: after an options write invalidates the alloptions snapshot,
// the warmer re-populates it from the backend without client involvement.
func TestWarmer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srvConf := server.NewServer("8.0.36", mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser("piko", "secret"); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				conn, err := server.NewCustomizedConn(c, srvConf, auth, warmFakeHandler{})
				if err != nil {
					return
				}
				for !conn.Closed() {
					if conn.HandleCommand() != nil {
						return
					}
				}
			}()
		}
	}()

	log := slog.New(slog.DiscardHandler)
	p := pool.New(config.Backend{Address: ln.Addr().String(), Username: "piko", Password: "secret"},
		config.Pool{MaxOpen: 2, MaxIdle: 2, PingInterval: time.Second, AcquireTimeout: 2 * time.Second},
		log, nil)
	t.Cleanup(p.Close)

	c := testCache(t, nil)
	w := NewWarmer(c, p, log)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go w.Run(ctx)
	c.SetRefetch(w.Trigger)

	// The cache learns the alloptions query from normal traffic.
	c.Store("wp", autoloadQ, testResult(t))

	// An options write invalidates it...
	c.InvalidateWrite("wp", "UPDATE wp_options SET option_value = 'x' WHERE option_name = 'blogname'")
	if _, ok := c.Lookup("wp", autoloadQ); ok {
		t.Fatal("alloptions should be invalidated")
	}

	// ...and the warmer brings it back on its own.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := c.Lookup("wp", autoloadQ); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("warmer never re-populated the alloptions entry")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
