package orient

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/istreamdata/orientgo/oschema"
)

const DriverNameSQL = "orient"

var (
	_ driver.Driver  = (*orientDriver)(nil)
	_ driver.Conn    = (*Database)(nil)
	_ driver.Execer  = (*Database)(nil)
	_ driver.Queryer = (*Database)(nil)
)

var dsnRx *regexp.Regexp = regexp.MustCompile(`([^@]+)@([^:]+):([^/]+)/(.+)`)

func init() {
	sql.Register(DriverNameSQL, &orientDriver{})
}

// DialDSN returns a new connection to the database.
// The dsn (driver-specific name) is a string in a driver-specific format.
// For orientgo, the dsn should be of the format:
//   user@pass:host:port/db
//   or
//   user@pass:host/db  (default port of 2424 is used)
//
// Function is also used for database/sql driver:
//   db, err := sql.Open("orient", "admin@admin:127.0.0.1/db")
func DialDSN(dsn string) (*Database, error) {
	user, pass, host, port, dbname, err := parseDsn(dsn)
	if port == "" {
		port = "2424"
	}
	if err != nil {
		return nil, err
	}
	dbc, err := Dial(net.JoinHostPort(host, port))
	if err != nil {
		return nil, err
	}
	// TODO: right now assumes DocumentDB type - pass in on the dsn??
	//       NOTE: I tried a graphDB with DocumentDB type and it worked, so why is it necesary at all?
	// TODO: this maybe shouldn't happen in this method -> might do it lazily in Query/Exec methods?
	db, err := dbc.Open(dbname, DocumentDB, user, pass)
	if err != nil {
		dbc.Close()
		return nil, err
	}
	return db, nil
}

// Implements the Go sql/driver.Driver interface.
type orientDriver struct{}

// Open implements sql/driver.Driver interface
// See DialDSN for more info.
func (d *orientDriver) Open(dsn string) (driver.Conn, error) {
	glog.V(10).Infoln("OgonoriDriver#Open")
	return DialDSN(dsn)
}

func parseDsn(dsn string) (uname, passw, host, port, dbname string, err error) {
	matches := dsnRx.FindStringSubmatch(dsn)
	if matches == nil || len(matches) != 5 {
		return "", "", "", "", "",
			fmt.Errorf("Unable to parse connection string: %s. Must be of the format: %s",
				dsn, "uname@passw:ip-or-host/dbname")
	}
	toks := strings.Split(matches[3], ":")
	host = toks[0]
	if len(toks) > 1 {
		port = toks[1]
	}
	return matches[1], matches[2], host, port, matches[4], nil
}

// Prepare implements sql/driver.Conn interface
func (db *Database) Prepare(query string) (driver.Stmt, error) {
	glog.V(10).Infoln("ogoConn.Prepare: ", query)
	return &ogonoriStmt{db: db, query: query}, nil
}

// Prepare implements sql/driver.Conn interface
func (db *Database) Begin() (driver.Tx, error) {
	glog.V(10).Infoln("ogoConn.Begin")
	return nil, fmt.Errorf("orientgo: transactions are not supported for now")
}

// Prepare implements sql/driver.Execer interface
func (db *Database) Exec(cmd string, args []driver.Value) (driver.Result, error) {
	glog.V(10).Infoln("ogoConn.Exec")
	if db == nil {
		return nil, ErrInvalidConn{Msg: "db not initialized in ogonoriConn#Exec"}
	}
	recs, err := db.SQLCommand(nil, cmd, driverArgs(args)...)
	if err != nil {
		return ogonoriResult{-1, -1}, err
	}
	if n, err := recs.AsInt(); err == nil {
		return ogonoriResult{int64(n), -1}, nil
	}
	var docs []*oschema.ODocument
	if err = recs.DeserializeAll(&docs); err != nil {
		return ogonoriResult{-1, -1}, err
	}
	lastdoc := docs[len(docs)-1]
	// sepIdx := strings.Index(lastDoc.RID, ":")
	// if sepIdx < 0 {
	// 	return ogonoriResult{len64(docs), -1}, fmt.Errorf("RID of returned doc not of expected format: %v", lastDoc.RID)
	// }
	// lastId, err := strconv.ParseInt(lastDoc.RID[sepIdx+1:], 10, 64)
	// if err != nil {
	// 	return ogonoriResult{len64(docs), -1}, fmt.Errorf("Couldn't parse ID from doc RID: %v: %v", lastDoc.RID, err)
	// }
	return ogonoriResult{int64(len(docs)), lastdoc.RID.ClusterPos}, err
}

// Prepare implements sql/driver.Queryer interface
func (db *Database) Query(query string, args []driver.Value) (driver.Rows, error) {
	glog.V(10).Infoln("ogoConn.Query")
	if db == nil {
		return nil, ErrInvalidConn{Msg: "db not initialized in ogonoriConn#Exec"}
	}
	var docs []*oschema.ODocument
	_, err := db.SQLQuery(&docs, nil, query, driverArgs(args)...)
	if err != nil {
		return nil, err
	}
	return newRows(docs), nil
}

func driverArgs(args []driver.Value) []interface{} {
	out := make([]interface{}, len(args))
	for i, val := range args {
		glog.V(10).Infof("valarg: %T: %v; isValue=%v\n", val, val, driver.IsValue(val)) // DEBUG
		switch val.(type) {
		case string, int64, float64, bool, []byte:
			out[i] = val
		case time.Time:
			out[i] = val.(time.Time).String() // TODO: this is probably not the format we want -> fix it later
		default:
			_, file, line, _ := runtime.Caller(0)
			glog.Warningf("Unexpected type in ogonoriConn#Exec: %T. (%s:%d)", val, file, line)
		}
	}
	return out
}

var _ driver.Result = (*ogonoriResult)(nil)

// ogonoriResult implements the sql/driver.Result inteface.
type ogonoriResult struct {
	affectedRows int64
	insertId     int64
}

// LastInsertId returns the database's auto-generated ID after,
// for example, an INSERT into a table with primary key.
func (res ogonoriResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

// RowsAffected returns the number of rows affected by the query.
func (res ogonoriResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

var _ driver.Rows = (*ogonoriRows)(nil)

// ogonoriRows implements the sql/driver.Rows interface.
type ogonoriRows struct {
	pos     int // index of next row (doc) to return
	docs    []*oschema.ODocument
	cols    []string
	fulldoc bool // whether query returned a full document or just properties
	// TODO: maybe a reference to the appropriate schema is needed here?
}

func newRows(docs []*oschema.ODocument) *ogonoriRows {
	var cols []string
	if docs == nil || len(docs) == 0 {
		cols = []string{}
		return &ogonoriRows{docs: docs, cols: cols}
	}

	var fulldoc bool
	if docs[0].Classname == "" {
		cols = make([]string, 0, len(docs[0].FieldNames()))
		for _, fname := range docs[0].FieldNames() {
			cols = append(cols, fname)
		}
	} else {
		fulldoc = true
		// if Classname is set then the user queried for a full document
		// not individual properties of a Document/Class
		cols = []string{docs[0].Classname}
	}
	return &ogonoriRows{docs: docs, cols: cols, fulldoc: fulldoc}
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice.  If a particular column name isn't known, an empty
// string should be returned for that entry.
func (rows *ogonoriRows) Columns() []string {
	glog.V(10).Infof("ogonoriRows.Columns = %v\n", rows.cols)
	return rows.cols
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// The dest slice may be populated only with
// a driver Value type, but excluding string.
// All string values must be converted to []byte.
//
// Next should return io.EOF when there are no more rows.
func (rows *ogonoriRows) Next(dest []driver.Value) error {
	glog.V(10).Infoln("ogonoriRows.Next")
	// TODO: right now I return the entire resultSet as an array, thus all loaded into memory
	//       it would be better to have obinary.dbCommands provide an iterator based model
	//       that only needs to read a "row" (ODocument) at a time
	if rows.pos >= len(rows.docs) {
		return io.EOF
	}
	// TODO: may need to do a type switch here -> what else can come in from a query in OrientDB
	//       besides an ODocument ??
	currdoc := rows.docs[rows.pos]
	if rows.fulldoc {
		dest[0] = currdoc

	} else {
		// was a property only query
		for i := range dest {
			// TODO: need to check field.Type and see if it is one that can map to Value
			//       what will I do for types that don't map to Value (e.g., EmbeddedRecord, EmbeddedMap) ??
			field := currdoc.GetField(rows.cols[i])
			if field != nil {
				dest[i] = field.Value
			}
		}
	}

	rows.pos++
	// TODO: this is where we need to return any errors that occur
	return nil
}

// Close closes the rows iterator.
func (rows *ogonoriRows) Close() error {
	glog.V(10).Infoln("ogonoriRows.Close")
	return nil
}

var _ driver.Stmt = (*ogonoriStmt)(nil)

// ogonoriStmt implements the Go sql/driver.Stmt interface.
type ogonoriStmt struct {
	// TODO: review this - this is how the mysql driver does it
	db    *Database
	query string // the SQL query/cmd specified by the user
}

// NumInput returns the number of placeholder parameters.
func (st *ogonoriStmt) NumInput() int {
	glog.V(10).Infoln("ogonoriStmt.NumInput")
	return -1 // -1 means sql package will not "sanity check" arg counts
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (st *ogonoriStmt) Exec(args []driver.Value) (driver.Result, error) {
	glog.V(10).Infoln("ogonoriStmt.Exec")
	if st.db == nil {
		return nil, ErrInvalidConn{Msg: "obinary.DBClient not initialized in ogonoriStmt#Exec"}
	}
	return st.db.Exec(st.query, args)
}

// Query executes a query that may return rows, such as a SELECT.
func (st *ogonoriStmt) Query(args []driver.Value) (driver.Rows, error) {
	glog.V(10).Infoln("ogonoriStmt.Query")
	if st.db == nil {
		return nil, ErrInvalidConn{Msg: "obinary.DBClient not initialized in ogonoriStmt#Query"}
	}
	return st.db.Query(st.query, args)
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use by any queries.
func (st *ogonoriStmt) Close() error {
	glog.V(10).Info("ogonoriStmt.Close")
	// nothing to do here since there is no special statement handle in OrientDB
	// that is referenced by a client driver
	return nil
}
