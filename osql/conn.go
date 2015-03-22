package osql

import (
	_ "database/sql"
	"database/sql/driver"

	"github.com/quux00/ogonori/obinary"
)

//
// Implements Go sql/driver.Conn interface
//
type ogonoriConn struct {
	dbc *obinary.DBClient
}

func (c *ogonoriConn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (c *ogonoriConn) Begin() (driver.Tx, error) {
	return nil, nil
}

func (c *ogonoriConn) Close() error {
	// TODO: ensure that Close() is idempotent
	return nil
}
