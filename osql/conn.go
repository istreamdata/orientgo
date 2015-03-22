package osql

import (
	_ "database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/quux00/ogonori/obinary"
)

//
// Implements:
// sql/driver.Conn interface
// sql/driver.Execer interface
// sql/driver.Queryer interface
//
type ogonoriConn struct {
	dbc *obinary.DBClient
}

func (c *ogonoriConn) Prepare(query string) (driver.Stmt, error) {
	fmt.Println("** ogoConn.Prepare")
	return nil, nil
}

func (c *ogonoriConn) Begin() (driver.Tx, error) {
	fmt.Println("** ogoConn.Begin")
	return nil, nil
}

func (c *ogonoriConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	fmt.Println("** ogoConn.Exec")
	err := obinary.SQLCommand(c.dbc, query)
	return ogonoriResult{1, -1}, err
}

func (c *ogonoriConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	fmt.Println("** ogoConn.Query")
	return nil, nil
}

func (c *ogonoriConn) Close() error {
	fmt.Printf("%v\n", "OGONORICONN#Close() called")
	if c.dbc != nil {
		return obinary.CloseDatabase(c.dbc)
	}
	// Close() must be idempotent
	return nil
}
