package osql

import (
	_ "database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"github.com/quux00/ogonori/obinary"
	"github.com/quux00/ogonori/ogl"
)

//
// ogonoriConn implements:
//  - sql/driver.Conn interface
//  - sql/driver.Execer interface
//  - sql/driver.Queryer interface
//
type ogonoriConn struct {
	dbc *obinary.DBClient
}

func (c *ogonoriConn) Prepare(query string) (driver.Stmt, error) {
	fmt.Println("** ogoConn.Prepare")
	return nil, nil
}

func (c *ogonoriConn) Begin() (driver.Tx, error) {
	ogl.Println("** ogoConn.Begin")
	return nil, nil
}

func (c *ogonoriConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	ogl.Println("** ogoConn.Exec")
	nrows, docs, err := obinary.SQLCommand(c.dbc, query)
	if err != nil {
		return ogonoriResult{-1, -1}, err
	}
	if docs == nil {
		return ogonoriResult{nrows, -1}, err
	}

	lastDoc := docs[len(docs)-1]
	sepIdx := strings.Index(lastDoc.Rid, ":")
	if sepIdx < 0 {
		return ogonoriResult{nrows, -1}, fmt.Errorf("RID of returned doc not of expected format: %v", lastDoc.Rid)
	}
	lastId, err := strconv.ParseInt(lastDoc.Rid[sepIdx+1:], 10, 64)
	if err != nil {
		return ogonoriResult{nrows, -1}, fmt.Errorf("Couldn't parse ID from doc RID: %v: %v", lastDoc.Rid, err)
	}
	return ogonoriResult{nrows, lastId}, err
}

func (c *ogonoriConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	ogl.Println("** ogoConn.Query")
	return nil, nil
}

func (c *ogonoriConn) Close() error {
	ogl.Println("** ogoConn.Close")
	// Close() must be idempotent
	if c.dbc != nil {
		return obinary.CloseDatabase(c.dbc)
		c.dbc = nil
	}
	return nil
}
