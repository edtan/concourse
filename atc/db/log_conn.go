package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/Masterminds/squirrel"
)

func Log(logger lager.Logger, conn Conn) Conn {
	return &logConn{
		Conn:    conn,
		logger:  logger,
		session: logger.SessionName(),
	}
}

type logConn struct {
	Conn

	logger  lager.Logger
	session string
}

// https://stackoverflow.com/a/45766707
func (c *logConn) elapsed(action, query string) func() {
	start := time.Now()
	return func() {
		c.logger.Debug(fmt.Sprintf("%s.%s", c.session, action), lager.Data{"duration": fmt.Sprintf("%v", time.Since(start)), "query": query})
	}
}

func (c *logConn) Query(query string, args ...interface{}) (*sql.Rows, error) {
	defer c.elapsed("query", c.strip(query))()
	return c.Conn.Query(query, args...)
}

func (c *logConn) QueryRow(query string, args ...interface{}) squirrel.RowScanner {
	defer c.elapsed("query-row", c.strip(query))()
	return c.Conn.QueryRow(query, args...)
}

func (c *logConn) Exec(query string, args ...interface{}) (sql.Result, error) {
	defer c.elapsed("exec", c.strip(query))()
	return c.Conn.Exec(query, args...)
}

func (c *logConn) SetSession(s string) {
	c.session = s
}

func (c *logConn) strip(query string) string {
	return strings.Join(strings.Fields(query), " ")
}
