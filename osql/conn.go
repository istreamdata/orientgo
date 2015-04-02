package osql

import (
	_ "database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/quux00/ogonori/obinary"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
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

	return &ogonoriStmt{conn: c, query: query}, nil
}

func (c *ogonoriConn) Begin() (driver.Tx, error) {
	ogl.Println("** ogoConn.Begin")
	return nil, nil
}

func (c *ogonoriConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	ogl.Println("** ogoConn.Exec")

	if c.dbc == nil {
		return nil, oerror.ErrInvalidConn{"obinary.DBClient not initialized in ogonoriConn#Exec"}
	}
	return doExec(c.dbc, query, args)
}

func doExec(dbc *obinary.DBClient, cmd string, args []driver.Value) (driver.Result, error) {
	strargs := valuesToStrings(args)

	retval, docs, err := obinary.SQLCommand(dbc, cmd, strargs...)
	fmt.Printf("exec1: %T: %v\n", retval, retval)
	if err != nil {
		return ogonoriResult{-1, -1}, err
	}

	if docs == nil {
		fmt.Println("exec2")
		nrows, err := strconv.ParseInt(retval, 10, 64)
		if err != nil {
			fmt.Printf("exec3: %T: %v\n", err, err)
			nrows = -1
		}
		return ogonoriResult{nrows, -1}, err
	}

	lastDoc := docs[len(docs)-1]
	sepIdx := strings.Index(lastDoc.Rid, ":")
	if sepIdx < 0 {
		return ogonoriResult{len64(docs), -1}, fmt.Errorf("RID of returned doc not of expected format: %v", lastDoc.Rid)
	}
	lastId, err := strconv.ParseInt(lastDoc.Rid[sepIdx+1:], 10, 64)
	if err != nil {
		return ogonoriResult{len64(docs), -1}, fmt.Errorf("Couldn't parse ID from doc RID: %v: %v", lastDoc.Rid, err)
	}
	return ogonoriResult{len64(docs), lastId}, err
}

func len64(docs []*oschema.ODocument) int64 {
	return int64(len(docs))
}

func (c *ogonoriConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	ogl.Println("** ogoConn.Query")

	if c.dbc == nil {
		return nil, oerror.ErrInvalidConn{"obinary.DBClient not initialized in ogonoriConn#Exec"}
	}
	return doQuery(c.dbc, query, args)
}

func doQuery(dbc *obinary.DBClient, query string, args []driver.Value) (driver.Rows, error) {
	var (
		docs []*oschema.ODocument
		err  error
	)

	strargs := valuesToStrings(args)
	fetchPlan := ""
	docs, err = obinary.SQLQuery(dbc, query, fetchPlan, strargs...)
	ogl.Printf("oC.Q:  %v\n", docs)
	return NewRows(docs), err
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

func valuesToStrings(args []driver.Value) []string {
	strargs := make([]string, len(args))
	for i, valarg := range args {
		ogl.Printf("valarg: %T: %v; isValue=%v\n", valarg, valarg, driver.IsValue(valarg)) // DEBUG
		switch valarg.(type) {
		case string:
			strargs[i] = valarg.(string)
		case int64:
			strargs[i] = strconv.FormatInt(valarg.(int64), 10)
		case float64:
			strargs[i] = strconv.FormatFloat(valarg.(float64), 'f', -1, 10)
		case bool:
			strargs[i] = strconv.FormatBool(valarg.(bool))
		case []byte:
			strargs[i] = string(valarg.([]byte))
		case time.Time:
			strargs[i] = valarg.(time.Time).String() // TODO: this is probably not the format we want -> fix it later
		default:
			_, file, line, _ := runtime.Caller(0)
			ogl.Warn(fmt.Sprintf("Unexpected type in ogonoriConn#Exec: %T. (%s:%d)", valarg, file, line))
		}
	}
	return strargs
}
