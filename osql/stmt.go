package osql

import (
	_ "database/sql"
	"database/sql/driver"

	"github.com/quux00/ogonori/obinary"
)

//
// ogonoriStmt implements the Go sql/driver.Stmt interface.
//
type ogonoriStmt struct {
	dbc        *obinary.DBClient // TODO: review this - this is how the mysql driver does it
	paramCount int               // TODO: can we know this in OrientDB w/o parsing the SQL?
}

//
// NumInput returns the number of placeholder parameters.
//
func (st *ogonoriStmt) NumInput() int {
	return -1 // -1 means sql package will not "sanity check" arg counts
}

//
// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
func (st *ogonoriStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

//
// Query executes a query that may return rows, such as a SELECT.
//
func (st *ogonoriStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

//
// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
func (st *ogonoriStmt) Close() error {
	return nil
}
