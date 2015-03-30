package osql

import (
	_ "database/sql"
	"database/sql/driver"

	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
)

//
// ogonoriStmt implements the Go sql/driver.Stmt interface.
//
type ogonoriStmt struct {
	// dbc        *obinary.DBClient // TODO: review this - this is how the mysql driver does it
	conn  *ogonoriConn
	query string // the SQL query/cmd specified by the user
	// paramCount int    // TODO: can we know this in OrientDB w/o parsing the SQL?
}

//
// NumInput returns the number of placeholder parameters.
//
func (st *ogonoriStmt) NumInput() int {
	ogl.Debugln("** ogonoriStmt.NumInput")
	return -1 // -1 means sql package will not "sanity check" arg counts
}

//
// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
func (st *ogonoriStmt) Exec(args []driver.Value) (driver.Result, error) {
	ogl.Debugln("** ogonoriStmt.Exec")
	if st.conn == nil || st.conn.dbc == nil {
		return nil, oerror.ErrInvalidConn{"obinary.DBClient not initialized in ogonoriStmt#Exec"}
	}

	return doExec(st.conn.dbc, st.query, args)
}

//
// Query executes a query that may return rows, such as a SELECT.
//
func (st *ogonoriStmt) Query(args []driver.Value) (driver.Rows, error) {
	ogl.Debugln("** ogonoriStmt.Query")
	if st.conn == nil || st.conn.dbc == nil {
		return nil, oerror.ErrInvalidConn{"obinary.DBClient not initialized in ogonoriStmt#Query"}
	}

	return doQuery(st.conn.dbc, st.query, args)
}

//
// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use by any queries.
//
func (st *ogonoriStmt) Close() error {
	ogl.Debugln("** ogonoriStmt.Close")
	// nothing to do here since there is no special statement handle in OrientDB
	// that is referenced by a client driver
	return nil
}
