package orient_test

import (
	//	"database/sql"
	"flag"
	//	"log"
	//	"os"
	"path/filepath"
	"reflect"
	"runtime"
	//	"runtime/debug"
	//	"runtime/pprof"
	"sort"
	//	"strconv"
	"fmt"
	"strings"
	"time"

	//	"net/http"
	_ "net/http/pprof"

	"github.com/istreamdata/orientgo/oerror"
	"github.com/istreamdata/orientgo/oschema"
	//_ "github.com/istreamdata/orientgo/osql"
	"github.com/golang/glog"
	"github.com/istreamdata/orientgo"
	"github.com/stretchr/testify/assert"
	"runtime/debug"
	"testing"
)

//
// This is a "functional" tester class against a live OrientDB 2.x server
// I'm using while developing the ogonori OrientDB Go client.  There is
// "HELP WANTED" issue on GitHub to split this into a more managable piece
// of code.
//
// How to run:
// OPTION 1: Set schema and data up before hand and only run data statements, not DDL
//
//  Before running this test, you can to run the scripts/ogonori-setup.sql
//  with the `console.sh` program of OrientDB:
//     ./console.sh ogonori-setup.sql
//
//  Then run this code with:
//     ./ogonori
//
// OPTION 2: Run full DDL - create and drop the database, in between
//           run the data statements
//      ./ogonori full
//
// OPTION 3: Run create DDL, but not the drop
//      ./ogonori create
//   After doing this then you can run with
//      ./ogonori
//   to test the data statements only
//

const (
	dbUser = "root"
	dbPass = "root"
)

// Flags - specify these on the cmd line to change from the defaults
var (
	dbDocumentName, dbGraphName string
)

var (
	equalsFmt, okFmt, assertFmt, fatalFmt string
)

//
// initialize formatting strings for "assert" methods
//
func init() {
	flag.StringVar(&dbDocumentName, "dbdocumentname", "ogonoriTest", "OrientDB document DB tests")
	flag.StringVar(&dbGraphName, "dbgraphname", "ogonoriGraphTest", "OrientDB graph DB tests")

	if runtime.GOOS == "windows" {
		equalsFmt = "%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\n\n"
		okFmt = "FATAL: %s:%d: %v\n\n"
		assertFmt = "FAIL: %s:%d: %s\n\n"
		fatalFmt = "FATAL: %s:%d: %v\n\n"
	} else {
		equalsFmt = "\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n"
		okFmt = "\033[31mFATAL: %s:%d: %v\033[39m\n\n"
		assertFmt = "\033[31mFAIL: %s:%d: %s\033[39m\n\n"
		fatalFmt = "\033[31mFATAL: %s:%d: %v\033[39m\n\n"
	}
}

func Nil(t *testing.T, obj interface{}, msg ...interface{}) {
	if !assert.Nil(t, obj, msg...) {
		debug.PrintStack()
		t.Fatal(obj)
	}
}

func True(t *testing.T, obj bool, msg ...interface{}) {
	if !assert.True(t, obj, msg...) {
		debug.PrintStack()
		t.Fatal(obj)
	}
}

// Equals compares two values for equality (DeepEquals).
// If they are not equal, an error message is printed
// and the function panics.  Use only in test scenarios.
func Equals(t *testing.T, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		debug.PrintStack()
		t.Fatalf(equalsFmt, filepath.Base(file), line, exp, act)
	}
}

func createOgonoriTestDB(t *testing.T, dbc orient.Client, dbUser, dbPass string) {
	glog.Infof("%s\n\n", "-------- Create OgonoriTest DB --------")

	sess, err := dbc.Auth(dbUser, dbPass)
	Nil(t, err)

	//	True(t, dbc.GetSessionId() >= int32(0), "sessionid")
	//	True(t, dbc.GetCurrDB() == nil, "currDB should be nil")

	mapDBs, err := sess.ListDatabases()
	Nil(t, err)
	glog.V(10).Infof("mapDBs: %v\n", mapDBs)
	gratefulTestPath, ok := mapDBs["GratefulDeadConcerts"]
	True(t, ok, "GratefulDeadConcerts not in DB list")
	True(t, strings.HasPrefix(gratefulTestPath, "plocal"), "plocal prefix for db path")

	// first check if ogonoriTest db exists and if so, drop it
	dbexists, err := sess.DatabaseExists(dbDocumentName, orient.Persistent)
	Nil(t, err)

	if dbexists {
		err = sess.DropDatabase(dbDocumentName, orient.Persistent)
		Nil(t, err)
	}

	// err = dbc.CreateDatabase(dbc, dbDocumentName, constants.DocumentDbType, constants.Volatile)
	err = sess.CreateDatabase(dbDocumentName, orient.DocumentDB, orient.Persistent)
	Nil(t, err)
	dbexists, err = sess.DatabaseExists(dbDocumentName, orient.Persistent)
	Nil(t, err)
	True(t, dbexists, dbDocumentName+" should now exists after creating it")

	seedInitialData(t, dbc)

	mapDBs, err = sess.ListDatabases()
	Nil(t, err)
	//fmt.Printf("%v\n", mapDBs)
	ogonoriTestPath, ok := mapDBs[dbDocumentName]
	True(t, ok, dbDocumentName+" not in DB list")
	True(t, strings.HasPrefix(ogonoriTestPath, "plocal"), "plocal prefix for db path")
}

func seedInitialData(t *testing.T, dbc orient.Client) {
	db, err := dbc.Open(dbDocumentName, orient.DocumentDB, "admin", "admin")
	Nil(t, err)

	SeedDB(t, db)
}

func deleteNewRecordsDocDB(db orient.Database) {
	_, err := db.SQLCommand(nil, "delete from Cat where name <> 'Linus' AND name <> 'Keiko'")
	if err != nil {
		glog.Warning(err.Error())
		return
	}
}

func deleteNewClustersDocDB(t *testing.T, db orient.Database) {
	// doing DROP CLUSTER via SQL will not return an exception - it just
	// returns "false" as the retval (first return value), so safe to do this
	// even if these clusters don't exist
	for _, clustName := range []string{"CatUSA", "CatAmerica", "bigapple"} {
		_, err := db.SQLCommand(nil, "DROP CLUSTER "+clustName)
		Nil(t, err)
	}
}

func deleteNewRecordsGraphDB(db orient.Database) {
	_, _ = db.SQLCommand(nil, "DELETE VERTEX Person")
	_, err := db.SQLCommand(nil, "DROP CLASS Person")
	if err != nil {
		glog.Warning(err.Error())
		return
	}
	_, err = db.SQLCommand(nil, "DROP CLASS Friend")
	if err != nil {
		glog.Warning(err.Error())
		return
	}
}

func dropDatabase(t *testing.T, dbc orient.Client, dbname string, dbtype orient.StorageType) {
	//_ = dbc.Close()
	sess, err := dbc.Auth(dbUser, dbPass)
	Nil(t, err)

	err = sess.DropDatabase(dbname, dbtype)
	Nil(t, err)
	dbexists, err := sess.DatabaseExists(dbname, orient.Persistent)
	if err != nil {
		glog.Warning(err.Error())
		return
	}
	if dbexists {
		glog.Warningf("ERROR: Deletion of database %s failed\n", dbname)
	}
}

/*
func graphCommandsSQLAPI(conxStr string) {
	db, err := sql.Open("ogonori", conxStr)
	Nil(t, err)
	defer db.Close()

	err = db.Ping()
	Nil(t, err)

	insertSQL := "insert into Person SET firstName='Joe', lastName='Namath'"
	res, err := db.Exec(insertSQL)
	Nil(t, err)

	nrows, _ := res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ := res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	createVtxSQL := `CREATE VERTEX Person SET firstName = 'Terry', lastName = 'Bradshaw'`
	res, err = db.Exec(createVtxSQL)
	Nil(t, err)

	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	sql := `CREATE EDGE Friend FROM
            (SELECT FROM Person where firstName = 'Joe' AND lastName = 'Namath')
            TO
            (SELECT FROM Person where firstName = 'Terry' AND lastName = 'Bradshaw')`
	res, err = db.Exec(sql)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	sql = `select from Friend order by @rid desc LIMIT 1`
	rows, err := db.Query(sql)
	rowdocs := make([]*oschema.ODocument, 0, 1)
	for rows.Next() {
		var newdoc oschema.ODocument
		err = rows.Scan(&newdoc)
		rowdocs = append(rowdocs, &newdoc)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 1, len(rowdocs))
	Equals(t, "Friend", rowdocs[0].Classname)
	friendOutLink := rowdocs[0].GetField("out").Value.(*oschema.OLink)
	True(t, friendOutLink.Record == nil, "should be nil")

	glog.V(10).Infof("friendOutLink: %v\n", friendOutLink)

	// REMOVE THE STUFF BELOW since can't specify fetchPlain in SQL (??? => ask on user group)'
	// sql = `select from Friend order by @rid desc LIMIT 1 fetchPlan=*:-1`
	// rows, err = db.Query(sql)
	// rowdocs = make([]*oschema.ODocument, 0, 1)
	// for rows.Next() {
	// 	var newdoc oschema.ODocument
	// 	err = rows.Scan(&newdoc)
	// 	rowdocs = append(rowdocs, &newdoc)
	// }
	// err = rows.Err()
	// Nil(t, err)

	// Equals(t, 1, len(rowdocs))
	// Equals(t, "Friend", rowdocs[0].Classname)
	// friendOutLink = rowdocs[0].GetField("out").Value.(*oschema.OLink)
	// // True(t, friendOutLink.Record != nil, "should NOT be nil") // FAILS: looks like you cannot put a fetchplain in an SQL query itself?

	// nrows, _ = res.RowsAffected()
	// glog.V(10).Infof("nrows: %v\n", nrows)
	// lastID, _ = res.LastInsertId()
	// glog.V(10).Infof("last insert id: %v\n", lastID)
	// Equals(t, int64(1), nrows)
	// True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

}

func databaseSQLAPI(conxStr string) {
	glog.Infof("\n%s\n\n", "-------- Using database/sql API --------")

	// ---[ OPEN ]---
	db, err := sql.Open("ogonori", conxStr)
	Nil(t, err)
	defer db.Close()

	err = db.Ping()
	Nil(t, err)

	// ---[ DELETE #1 ]---
	// should not delete any rows
	delcmd := "delete from Cat where name ='Jared'"
	res, err := db.Exec(delcmd)
	Nil(t, err)
	nrows, _ := res.RowsAffected()
	glog.Infof(">> RES num rows affected: %v\n", nrows)
	Equals(t, int64(0), nrows)

	// ---[ INSERT #1 ]---
	// insert with no params
	insertSQL := "insert into Cat (name, age, caretaker) values('Jared', 11, 'The Subway Guy')"
	glog.Infoln(insertSQL, "=> 'Jared', 11, 'The Subway Guy'")
	res, err = db.Exec(insertSQL)
	Nil(t, err)

	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ := res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	// ---[ INSERT #2 ]---
	// insert with no params
	insertSQL = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	glog.Infoln(insertSQL, "=> 'Filo', 4, 'Greek'")
	res, err = db.Exec(insertSQL, "Filo", 4, "Greek")
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	// ---[ QUERY #1: QueryRow ]---
	// it is safe to query properties -> not sure how to return docs yet
	querySQL := "select name, age from Cat where caretaker = 'Greek'"
	row := db.QueryRow(querySQL)

	var retname string
	var retage int64
	err = row.Scan(&retname, &retage)
	Nil(t, err)
	Equals(t, "Filo", retname)
	Equals(t, int64(4), retage)

	// ---[ QUERY #2: Query (multiple rows returned) ]---

	querySQL = "select name, age, caretaker from Cat order by age"

	var rName, rCaretaker string
	var rAge int64

	names := make([]string, 0, 4)
	ctakers := make([]string, 0, 4)
	ages := make([]int64, 0, 4)
	rows, err := db.Query(querySQL)
	for rows.Next() {
		err = rows.Scan(&rName, &rAge, &rCaretaker)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 4, len(names))
	Equals(t, 4, len(ctakers))
	Equals(t, 4, len(ages))

	Equals(t, []string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals(t, []string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(t, int64(4), ages[0])
	Equals(t, int64(10), ages[1])
	Equals(t, int64(11), ages[2])
	Equals(t, int64(15), ages[3])

	// ---[ QUERY #3: Same Query as above but change property order ]---

	querySQL = "select age, caretaker, name from Cat order by age"

	names = make([]string, 0, 4)
	ctakers = make([]string, 0, 4)
	ages = make([]int64, 0, 4)
	rows, err = db.Query(querySQL)
	for rows.Next() {
		err = rows.Scan(&rAge, &rCaretaker, &rName)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 4, len(names))
	Equals(t, 4, len(ctakers))
	Equals(t, 4, len(ages))

	Equals(t, []string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals(t, []string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(t, int64(4), ages[0])
	Equals(t, int64(10), ages[1])
	Equals(t, int64(11), ages[2])
	Equals(t, int64(15), ages[3])

	// ---[ QUERY #4: Property query using parameterized SQL ]---
	querySQL = "select caretaker, name, age from Cat where age >= ? order by age desc"

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	rows, err = db.Query(querySQL, "11")
	for rows.Next() {
		err = rows.Scan(&rCaretaker, &rName, &rAge)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	if err = rows.Err(); err != nil {
		Fatal(err)
	}

	Equals(t, 2, len(names))
	Equals(t, "Linus", names[0])
	Equals(t, "Jared", names[1])

	Equals(t, 2, len(ctakers))
	Equals(t, "Michael", ctakers[0])
	Equals(t, "The Subway Guy", ctakers[1])

	Equals(t, 2, len(ages))
	Equals(t, int64(15), ages[0])
	Equals(t, int64(11), ages[1])

	// ---[ DELETE #2 ]---
	res, err = db.Exec(delcmd)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.Infof(">> DEL2 RES num rows affected: %v\n", nrows)
	Equals(t, int64(1), nrows)

	// ---[ DELETE #3 ]---
	res, err = db.Exec(delcmd)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.Infof(">> DEL3 RES num rows affected: %v\n", nrows)
	Equals(t, int64(0), nrows)

	// ---[ DELETE #4 ]---
	delcmd = "delete from Cat where name <> 'Linus' AND name <> 'Keiko'"
	res, err = db.Exec(delcmd)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.Infof(">> DEL4 RES num rows affected: %v\n", nrows)
	Equals(t, int64(1), nrows)

	// ---[ Full ODocument Queries with database/sql ]---
	// ---[ QueryRow ]---
	glog.Infoln(">>>>>>>>> QueryRow of full ODocument<<<<<<<<<<<")
	querySQL = "select from Cat where name = 'Linus'"

	row = db.QueryRow(querySQL)

	var retdoc oschema.ODocument
	err = row.Scan(&retdoc)
	Nil(t, err)
	Equals(t, "Cat", retdoc.Classname)
	Equals(t, 3, len(retdoc.FieldNames()))
	Equals(t, "Linus", retdoc.GetField("name").Value)
	Equals(t, int32(15), retdoc.GetField("age").Value)
	Equals(t, "Michael", retdoc.GetField("caretaker").Value)

	// ---[ Query (return multiple rows) ]---
	querySQL = "select from Cat order by caretaker desc"
	rows, err = db.Query(querySQL)
	rowdocs := make([]*oschema.ODocument, 0, 2)
	for rows.Next() {
		var newdoc oschema.ODocument
		err = rows.Scan(&newdoc)
		rowdocs = append(rowdocs, &newdoc)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 2, len(rowdocs))
	Equals(t, "Cat", rowdocs[0].Classname)
	Equals(t, "Linus", rowdocs[0].GetField("name").Value)
	Equals(t, "Keiko", rowdocs[1].GetField("name").Value)
	Equals(t, "Anna", rowdocs[1].GetField("caretaker").Value)
}

func databaseSQLPreparedStmtAPI(conxStr string) {
	glog.Infof("\n%s\n\n", "-------- Using database/sql PreparedStatement API --------")

	db, err := sql.Open("ogonori", conxStr)
	Nil(t, err)
	defer db.Close()

	querySQL := "select caretaker, name, age from Cat where age >= ? order by age desc"

	stmt, err := db.Prepare(querySQL)
	Nil(t, err)
	defer stmt.Close()

	names := make([]string, 0, 2)
	ctakers := make([]string, 0, 2)
	ages := make([]int64, 0, 2)

	var (
		rCaretaker, rName string
		rAge              int64
	)

	// ---[ First use ]---
	rows, err := stmt.Query("10")
	for rows.Next() {
		err = rows.Scan(&rCaretaker, &rName, &rAge)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	if err = rows.Err(); err != nil {
		Fatal(err)
	}

	Equals(t, 2, len(names))
	Equals(t, "Linus", names[0])
	Equals(t, "Keiko", names[1])

	Equals(t, 2, len(ctakers))
	Equals(t, "Michael", ctakers[0])
	Equals(t, "Anna", ctakers[1])

	Equals(t, 2, len(ages))
	Equals(t, int64(15), ages[0])
	Equals(t, int64(10), ages[1])

	// ---[ Second use ]---
	rows, err = stmt.Query("14")

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	for rows.Next() {
		err = rows.Scan(&rCaretaker, &rName, &rAge)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	if err = rows.Err(); err != nil {
		Fatal(err)
	}

	Equals(t, 1, len(names))
	Equals(t, "Linus", names[0])
	Equals(t, int64(15), ages[0])
	Equals(t, "Michael", ctakers[0])

	// ---[ Third use ]---
	rows, err = stmt.Query("100")

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	if err = rows.Err(); err != nil {
		Fatal(err)
	}

	Equals(t, 0, len(names))
	Equals(t, 0, len(ages))
	Equals(t, 0, len(ctakers))

	stmt.Close()

	// ---[ Now prepare Command, not query ]---
	cmdStmt, err := db.Prepare("INSERT INTO Cat (age, caretaker, name) VALUES(?, ?, ?)")
	Nil(t, err)
	defer cmdStmt.Close()

	// use once
	result, err := cmdStmt.Exec(1, "Ralph", "Max")
	Nil(t, err)
	nrows, err := result.RowsAffected()
	Nil(t, err)
	Equals(t, 1, int(nrows))
	insertID, err := result.LastInsertId()
	Nil(t, err)
	True(t, int(insertID) >= 0, "insertId was: "+strconv.Itoa(int(insertID)))

	// use again
	result, err = cmdStmt.Exec(2, "Jimmy", "John")
	Nil(t, err)
	nrows, err = result.RowsAffected()
	Nil(t, err)
	Equals(t, 1, int(nrows))
	insertID2, err := result.LastInsertId()
	Nil(t, err)
	True(t, insertID != insertID2, "insertID was: "+strconv.Itoa(int(insertID)))

	row := db.QueryRow("select count(*) from Cat")
	var cnt int64
	err = row.Scan(&cnt)
	Nil(t, err)
	Equals(t, 4, int(cnt))

	cmdStmt.Close()

	// ---[ Prepare DELETE command ]---
	delStmt, err := db.Prepare("DELETE from Cat where name = ? OR caretaker = ?")
	Nil(t, err)
	defer delStmt.Close()
	result, err = delStmt.Exec("Max", "Jimmy")
	Nil(t, err)
	nrows, err = result.RowsAffected()
	Nil(t, err)
	Equals(t, 2, int(nrows))
	insertID3, err := result.LastInsertId()
	Nil(t, err)
	True(t, int(insertID3) < 0, "should have negative insertId for a DELETE")

}

func dbClusterCommandsNativeAPI(dbc orient.Client) {
	glog.V(10).Infoln("\n-------- CLUSTER commands --------\n")

	recint := func(recs obinary.Records) int {
		val, err := recs.AsInt()
		Nil(t, err)
		return val
	}
	recbool := func(recs obinary.Records) bool {
		val, err := recs.AsBool()
		Nil(t, err)
		return val
	}

	err := dbc.OpenDatabase(dbDocumentName, orient.DocumentDB, "admin", "admin")
	Nil(t, err)
	defer dbc.CloseDatabase()

	cnt1, err := dbc.GetClusterCountIncludingDeleted("default", "index", "ouser")
	Nil(t, err)
	True(t, cnt1 > 0, "should be clusters")

	cnt2, err := dbc.GetClusterCount("default", "index", "ouser")
	Nil(t, err)
	True(t, cnt1 >= cnt2, "counts should match or have more deleted")
	glog.V(10).Infof("Cluster count: %d\n", cnt2)

	begin, end, err := dbc.FetchClusterDataRange("ouser")
	Nil(t, err)
	glog.V(10).Infoln(">> cluster data range: %d, %d", begin, end)
	True(t, end >= begin, "begin and end of ClusterDataRange")

	glog.V(10).Infoln("\n-------- CLUSTER SQL commands --------\n")

	recs, err := db.SQLCommand(nil, "CREATE CLUSTER CatUSA")
	Nil(t, err)
	ival := recint(recs)
	True(t, ival > 5, fmt.Sprintf("Unexpected value of ival: %d", ival))

	recs, err = db.SQLCommand(nil, "ALTER CLUSTER CatUSA Name CatAmerica")
	Nil(t, err)
	//glog.Infof("ALTER CLUSTER CatUSA Name CatAmerica: retval: %v; docs: %v\n", retval, docs)

	recs, err = db.SQLCommand(nil, "DROP CLUSTER CatUSA")
	Nil(t, err)
	Equals(t, false, recbool(recs))

	recs, err = db.SQLCommand(nil, "DROP CLUSTER CatAmerica")
	Nil(t, err)
	Equals(t, true, recbool(recs))
	//glog.Infof("DROP CLUSTER CatAmerica: retval: %v; docs: %v\n", retval, docs)

	glog.V(10).Infoln("\n-------- CLUSTER Direct commands (not SQL) --------\n")
	clusterID, err := dbc.AddCluster("bigapple")
	if err != nil {
		Fatal(err)
	}
	True(t, clusterID > 0, "clusterID should be bigger than zero")

	cnt, err := dbc.GetClusterCount("bigapple")
	if err != nil {
		Fatal(err)
	}
	Equals(t, 0, int(cnt)) // should be no records in bigapple cluster

	err = dbc.DropCluster("bigapple")
	if err != nil {
		Fatal(err)
	}

	// this time it should return an error
	err = dbc.DropCluster("bigapple")
	True(t, err != nil, "DropCluster should return error when cluster doesn't exist")
}
*/

func dbCommandsNativeAPI(t *testing.T, dbc orient.Client) {
	glog.Infof("\n%s\n\n", "-------- database-level commands --------")

	var sql string
	var recs orient.Records

	db, err := dbc.Open(dbDocumentName, orient.DocumentDB, "admin", "admin")
	Nil(t, err)
	defer db.Close()

	// ---[ query from the ogonoriTest database ]---

	sql = "select from Cat where name = 'Linus'"

	var docs []*oschema.ODocument
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)

	linusDocRID := docs[0].RID

	True(t, linusDocRID.ClusterID != oschema.ClusterIDInvalid, "linusDocRID should not be nil")
	True(t, docs[0].Version > 0, fmt.Sprintf("Version is: %d", docs[0].Version))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Cat", docs[0].Classname)

	nameField := docs[0].GetField("name")
	True(t, nameField != nil, "should be a 'name' field")

	ageField := docs[0].GetField("age")
	True(t, ageField != nil, "should be a 'age' field")

	caretakerField := docs[0].GetField("caretaker")
	True(t, caretakerField != nil, "should be a 'caretaker' field")

	True(t, nameField.Id != caretakerField.Id, "IDs should not match")
	Equals(t, oschema.STRING, nameField.Type)
	Equals(t, oschema.STRING, caretakerField.Type)
	Equals(t, oschema.INTEGER, ageField.Type)
	Equals(t, "Linus", nameField.Value)
	Equals(t, int32(15), ageField.Value)
	Equals(t, "Michael", caretakerField.Value)

	// ---[ get by RID ]---
	docs, err = db.GetRecordByRID(linusDocRID, "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	docByRID := docs[0]
	Equals(t, linusDocRID, docByRID.RID)
	True(t, docByRID.Version > 0, fmt.Sprintf("Version is: %d", docByRID.Version))
	Equals(t, 3, len(docByRID.FieldNames()))
	Equals(t, "Cat", docByRID.Classname)

	nameField = docByRID.GetField("name")
	True(t, nameField != nil, "should be a 'name' field")

	ageField = docByRID.GetField("age")
	True(t, ageField != nil, "should be a 'age' field")

	caretakerField = docByRID.GetField("caretaker")
	True(t, caretakerField != nil, "should be a 'caretaker' field")

	True(t, nameField.Id != caretakerField.Id, "IDs should not match")
	Equals(t, oschema.STRING, nameField.Type)
	Equals(t, oschema.INTEGER, ageField.Type)
	Equals(t, oschema.STRING, caretakerField.Type)
	Equals(t, "Linus", nameField.Value)
	Equals(t, int32(15), ageField.Value)
	Equals(t, "Michael", caretakerField.Value)

	glog.Infof("docs returned by RID: %v\n", *(docs[0]))

	// ---[ cluster data range ]---
	//	begin, end, err := db.FetchClusterDataRange("cat")
	//	Nil(t, err)
	//	glog.Infof("begin = %v; end = %v\n", begin, end)

	sql = "insert into Cat (name, age, caretaker) values(\"Zed\", 3, \"Shaw\")"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ---[ query after inserting record(s) ]---

	sql = "select * from Cat order by name asc"
	glog.Infoln("Issuing command query: " + sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 3, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Cat", docs[0].Classname)
	Equals(t, 3, len(docs[1].FieldNames()))
	Equals(t, "Cat", docs[1].Classname)
	Equals(t, 3, len(docs[2].FieldNames()))
	Equals(t, "Cat", docs[2].Classname)

	keiko := docs[0]
	Equals(t, "Keiko", keiko.GetField("name").Value)
	Equals(t, int32(10), keiko.GetField("age").Value)
	Equals(t, "Anna", keiko.GetField("caretaker").Value)
	Equals(t, oschema.STRING, keiko.GetField("caretaker").Type)
	True(t, keiko.Version > int32(0), "Version should be greater than zero")
	True(t, keiko.RID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	linus := docs[1]
	Equals(t, "Linus", linus.GetField("name").Value)
	Equals(t, int32(15), linus.GetField("age").Value)
	Equals(t, "Michael", linus.GetField("caretaker").Value)

	zed := docs[2]
	Equals(t, "Zed", zed.GetField("name").Value)
	Equals(t, int32(3), zed.GetField("age").Value)
	Equals(t, "Shaw", zed.GetField("caretaker").Value)
	Equals(t, oschema.STRING, zed.GetField("caretaker").Type)
	Equals(t, oschema.INTEGER, zed.GetField("age").Type)
	True(t, zed.Version > int32(0), "Version should be greater than zero")
	True(t, zed.RID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	sql = "select name, caretaker from Cat order by caretaker"
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 3, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].Classname) // property queries do not come back with Classname set
	Equals(t, 2, len(docs[1].FieldNames()))
	Equals(t, "", docs[1].Classname)
	Equals(t, 2, len(docs[2].FieldNames()))

	Equals(t, "Anna", docs[0].GetField("caretaker").Value)
	Equals(t, "Michael", docs[1].GetField("caretaker").Value)
	Equals(t, "Shaw", docs[2].GetField("caretaker").Value)

	Equals(t, "Keiko", docs[0].GetField("name").Value)
	Equals(t, "Linus", docs[1].GetField("name").Value)
	Equals(t, "Zed", docs[2].GetField("name").Value)

	Equals(t, "name", docs[0].GetField("name").Name)

	// ---[ delete newly added record(s) ]---
	glog.Infoln("Deleting (sync) record #" + zed.RID.String())
	err = db.DeleteRecordByRID(zed.RID.String(), zed.Version)
	Nil(t, err)

	// glog.Infoln("Deleting (Async) record #11:4")
	// err = dbc.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	Fatal(err)
	// }

	sql = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	_, err = db.SQLCommand(nil, sql, "June", "8", "Cleaver") // TODO: check if numeric types are passed as strings in the Java client
	Nil(t, err)

	sql = "select name, age from Cat where caretaker = ?"
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql, "Cleaver")
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].Classname) // property queries do not come back with Classname set
	Equals(t, "June", docs[0].GetField("name").Value)
	Equals(t, int32(8), docs[0].GetField("age").Value)

	sql = "select caretaker, name, age from Cat where age > ? order by age desc"
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql, "9")
	Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].Classname) // property queries do not come back with Classname set
	Equals(t, "Linus", docs[0].GetField("name").Value)
	Equals(t, int32(15), docs[0].GetField("age").Value)
	Equals(t, "Keiko", docs[1].GetField("name").Value)
	Equals(t, int32(10), docs[1].GetField("age").Value)
	Equals(t, "Anna", docs[1].GetField("caretaker").Value)

	sql = "delete from Cat where name ='June'" // TODO: can we use a param here too ?
	glog.Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	glog.Infoln("+++++++++ END: SQL COMMAND w/ PARAMS ++++++++++++===")

	glog.Infoln("+++++++++ START: Basic DDL ++++++++++++===")

	sql = "DROP CLASS Patient"
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	if retval != "" {
	//		Equals(t, "true", retval)
	//	}

	// ------

	sql = "CREATE CLASS Patient"
	glog.V(10).Infoln(sql)
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		sql = "DROP CLASS Patient"
		_, err = db.SQLCommand(nil, sql)
		if err != nil {
			glog.Warningf("WARN: clean up error: %v\n", err)
			return
		}

		// TRUNCATE after drop should return an OServerException type
		sql = "TRUNCATE CLASS Patient"
		_, err = db.SQLCommand(nil, sql)
		True(t, err != nil, "Error from TRUNCATE should not be null")
		glog.V(10).Infoln(oerror.GetFullTrace(err))

		err = oerror.ExtractCause(err)
		switch err.(type) {
		case oerror.OServerException:
			glog.V(10).Infoln("type == oerror.OServerException")
		default:
			t.Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
		}
	}()

	ncls, err := recs.AsInt()
	Nil(t, err)
	True(t, ncls > 10, "classnum should be greater than 10 but was: ") //+retval)

	// ------

	sql = "Create property Patient.name string"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "alter property Patient.name min 3"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "Create property Patient.married boolean"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	db.ReloadSchema()
	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true')"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "TRUNCATE CLASS Patient"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true'), ('Martha', 'false')"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "SELECT count(*) from Patient"
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	fldCount := docs[0].GetField("count")
	Equals(t, int64(2), fldCount.Value)

	sql = "CREATE PROPERTY Patient.gender STRING"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "ALTER PROPERTY Patient.gender REGEXP [M|F]"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married, gender) VALUES ('Larry', 'true', 'M'), ('Shirley', 'false', 'F')"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married, gender) VALUES ('Lt. Dan', 'true', 'T'), ('Sally', 'false', 'F')"
	glog.Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	True(t, err != nil, "should be error - T is not an allowed gender")
	err = oerror.ExtractCause(err)
	switch err.(type) {
	case oerror.OServerException:
		glog.V(10).Infoln("type == oerror.OServerException")
	default:
		t.Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
	}

	sql = "SELECT FROM Patient ORDER BY @rid desc"
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 4, len(docs))
	Equals(t, "Shirley", docs[0].GetField("name").Value)

	sql = "ALTER PROPERTY Patient.gender NAME sex"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	err = db.ReloadSchema()
	Nil(t, err)

	sql = "DROP PROPERTY Patient.sex"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	sql = "select from Patient order by RID"
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 4, len(docs))
	Equals(t, 2, len(docs[0].Fields)) // has name and married
	Equals(t, "Hank", docs[0].Fields["name"].Value)

	Equals(t, 4, len(docs[3].Fields)) // has name, married, sex and for some reason still has `gender`
	Equals(t, "Shirley", docs[3].Fields["name"].Value)
	Equals(t, "F", docs[3].Fields["gender"].Value)

	sql = "TRUNCATE CLASS Patient"
	glog.Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ---[ Attempt to create, insert and read back EMBEDDEDLIST types ]---

	sql = "CREATE PROPERTY Patient.tags EMBEDDEDLIST STRING"
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	numval, err := recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `insert into Patient (name, married, tags) values ("George", "false", ["diabetic", "osteoarthritis"])`
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))

	sql = `SELECT from Patient where name = 'George'`
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	glog.V(10).Infof("docs: %v\n", docs)
	Equals(t, 1, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	embListTagsField := docs[0].GetField("tags")

	embListTags := embListTagsField.Value.([]interface{})
	Equals(t, 2, len(embListTags))
	Equals(t, "diabetic", embListTags[0].(string))
	Equals(t, "osteoarthritis", embListTags[1].(string))

	// ---[ try JSON content insertion notation ]---

	sql = `insert into Patient content {"name": "Freddy", "married":false}`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Freddy", docs[0].GetField("name").Value)
	Equals(t, false, docs[0].GetField("married").Value)

	// ---[ Try LINKs ! ]---

	sql = `select from Cat WHERE name = 'Linus' OR name='Keiko' ORDER BY @rid`
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Equals(t, 2, len(docs))
	linusRID := docs[0].RID
	keikoRID := docs[1].RID

	sql = `CREATE PROPERTY Cat.buddy LINK`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "buddy")

	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `insert into Cat SET name='Tilde', age=8, caretaker='Earl', buddy=(SELECT FROM Cat WHERE name = 'Linus')`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: >>%v<<\n", retval)
	//	glog.V(10).Infof("docs: >>%v<<\n", docs)
	Equals(t, 1, len(docs))
	Equals(t, "Tilde", docs[0].GetField("name").Value)
	Equals(t, 8, int(docs[0].GetField("age").Value.(int32)))
	Equals(t, linusRID, docs[0].GetField("buddy").Value.(*oschema.OLink).RID)

	tildeRID := docs[0].RID

	// ---[ Test EMBEDDED ]---

	sql = `CREATE PROPERTY Cat.embeddedCat EMBEDDED`
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "embeddedCat")

	emb := `{"name": "Spotty", "age": 2, emb: {"@type": "d", "@class":"Cat", "name": "yowler", "age":13}}`
	docs = nil
	_, err = db.SQLCommand(&docs, "insert into Cat content "+emb)
	Nil(t, err)

	Equals(t, 1, len(docs))
	Equals(t, "Spotty", docs[0].GetField("name").Value)
	Equals(t, 2, int(docs[0].GetField("age").Value.(int32)))
	Equals(t, oschema.EMBEDDED, docs[0].GetField("emb").Type)

	embCat := docs[0].GetField("emb").Value.(*oschema.ODocument)
	Equals(t, "Cat", embCat.Classname)
	True(t, embCat.Version < 0, "Version should be unset")
	True(t, embCat.RID.ClusterID < 0, "RID.ClusterID should be unset")
	True(t, embCat.RID.ClusterPos < 0, "RID.ClusterPos should be unset")
	Equals(t, "yowler", embCat.GetField("name").Value.(string))
	Equals(t, int(13), toInt(embCat.GetField("age").Value))

	_, err = db.SQLCommand(nil, "delete from Cat where name = 'Spotty'")
	Nil(t, err)

	// ---[ Test LINKLIST ]---

	sql = `CREATE PROPERTY Cat.buddies LINKLIST`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "buddies")
	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `insert into Cat SET name='Felix', age=6, caretaker='Ed', buddies=(SELECT FROM Cat WHERE name = 'Linus' OR name='Keiko')`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Felix", docs[0].GetField("name").Value)
	Equals(t, 6, int(docs[0].GetField("age").Value.(int32)))
	buddies := docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddies))
	Equals(t, 2, len(buddies))
	Equals(t, linusRID, buddies[0].RID)
	Equals(t, keikoRID, buddies[1].RID)

	felixRID := docs[0].RID

	// ---[ Try LINKMAP ]---
	sql = `CREATE PROPERTY Cat.notes LINKMAP`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "notes")

	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = fmt.Sprintf(`INSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": %s, '30': %s}`,
		linusRID, keikoRID)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 4, len(docs[0].FieldNames()))
	Equals(t, "Anna", docs[0].GetField("caretaker").Value)
	Equals(t, linusRID, docs[0].GetField("notes").Value.(map[string]*oschema.OLink)["bff"].RID)
	Equals(t, keikoRID, docs[0].GetField("notes").Value.(map[string]*oschema.OLink)["30"].RID)

	charlieRID := docs[0].RID

	// query with a fetchPlan that does NOT follow all the links
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL`
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	doc := docs[0]
	Equals(t, "Charlie", doc.GetField("name").Value)
	notesField := doc.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(notesField))

	bffNote := notesField["bff"]
	True(t, bffNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, bffNote.Record == nil, "RID should be nil")

	thirtyNote := notesField["30"]
	True(t, thirtyNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, thirtyNote.Record == nil, "RID should be nil")

	// query with a fetchPlan that does follow all the links

	sql = `SELECT FROM Cat WHERE notes IS NOT NULL`
	docs = nil
	recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	True(t, len(docs) > 0)
	doc = docs[0]
	Equals(t, "Charlie", doc.GetField("name").Value)
	notesField = doc.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(notesField))

	bffNote = notesField["bff"]
	True(t, bffNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, bffNote.Record != nil, "Record should be filled in")
	Equals(t, "Linus", bffNote.Record.GetField("name").Value)

	thirtyNote = notesField["30"]
	True(t, thirtyNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, thirtyNote.Record != nil, "Record should be filled in")
	Equals(t, "Keiko", thirtyNote.Record.GetField("name").Value)

	// ---[ Try LINKSET ]---

	sql = `CREATE PROPERTY Cat.buddySet LINKSET`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "buddySet")

	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	db.ReloadSchema() // good thing to do after modifying the schema

	// insert record with all the LINK types
	sql = `insert into Cat SET name='Germaine', age=2, caretaker='Minnie', ` +
		`buddies=(SELECT FROM Cat WHERE name = 'Linus' OR name='Keiko'), ` +
		`buddySet=(SELECT FROM Cat WHERE name = 'Linus' OR name='Felix'), ` +
		fmt.Sprintf(`notes = {"bff": %s, "30": %s}`, keikoRID, linusRID)

	// status of Cat at this point in time
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     #   |@RID |@CLASS|name    |age |caretaker|buddy|buddies        |notes                |buddySet
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     0   |#10:0|Cat   |Linus   |15  |Michael  |null |null           |null                 |null
	//     1   |#10:1|Cat   |Keiko   |10  |Anna     |null |null           |null                 |null
	//     2   |#10:4|Cat   |Tilde   |8   |Earl     |#10:0|null           |null                 |null
	//     3   |#10:5|Cat   |Felix   |6   |Ed       |null |[#10:0, #10:1] |null                 |null
	//     4   |#10:6|Cat   |Charlie |5   |Anna     |null |null           |{bff:#10:0, 30:#10:1}|null
	//     5   |#10:7|Cat   |Germaine|2   |Minnie   |null |[#10:0, #10:1] |{bff:#10:1, 30:#10:0}|[#10:0, #10:5]
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     Germaine references
	//     Felix references => Linus and Keiko as "buddies" (LINKLIST)

	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Germaine", docs[0].GetField("name").Value)
	Equals(t, 2, int(docs[0].GetField("age").Value.(int32)))

	germaineRID := docs[0].RID

	buddyList := docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddyList))
	Equals(t, 2, len(buddies))
	Equals(t, linusRID, buddyList[0].RID)
	Equals(t, keikoRID, buddyList[1].RID)

	buddySet := docs[0].GetField("buddySet").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddySet))
	Equals(t, 2, len(buddySet))
	Equals(t, linusRID, buddySet[0].RID)
	Equals(t, felixRID, buddySet[1].RID)

	notesMap := docs[0].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(buddies))
	Equals(t, keikoRID, notesMap["bff"].RID)
	Equals(t, linusRID, notesMap["30"].RID)

	// now query with fetchPlan that retrieves all links
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL ORDER BY name`
	docs = nil
	recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, "Charlie", docs[0].GetField("name").Value)
	Equals(t, "Germaine", docs[1].GetField("name").Value)
	Equals(t, "Minnie", docs[1].GetField("caretaker").Value)

	charlieNotesField := docs[0].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(charlieNotesField))

	bffNote = charlieNotesField["bff"]
	Equals(t, "Linus", bffNote.Record.GetField("name").Value)

	thirtyNote = charlieNotesField["30"]
	Equals(t, "Keiko", thirtyNote.Record.GetField("name").Value)

	// test Germaine's notes (LINKMAP)
	germaineNotesField := docs[1].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(germaineNotesField))

	bffNote = germaineNotesField["bff"]
	Equals(t, "Keiko", bffNote.Record.GetField("name").Value)

	thirtyNote = germaineNotesField["30"]
	Equals(t, "Linus", thirtyNote.Record.GetField("name").Value)

	// test Germaine's buddySet (LINKSET)
	germaineBuddySet := docs[1].GetField("buddySet").Value.([]*oschema.OLink)
	sort.Sort(byRID(germaineBuddySet))
	Equals(t, "Linus", germaineBuddySet[0].Record.GetField("name").Value)
	Equals(t, "Felix", germaineBuddySet[1].Record.GetField("name").Value)
	True(t, germaineBuddySet[1].RID.ClusterID != -1, "RID should be filled in")

	// Felix Document has references, so those should also be filled in
	felixDoc := germaineBuddySet[1].Record
	felixBuddiesList := felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, 2, len(felixBuddiesList))
	True(t, felixBuddiesList[0].Record != nil, "Felix links should be filled in")
	Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)

	// test Germaine's buddies (LINKLIST)
	germaineBuddyList := docs[1].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(germaineBuddyList))
	Equals(t, "Linus", germaineBuddyList[0].Record.GetField("name").Value)
	Equals(t, "Keiko", germaineBuddyList[1].Record.GetField("name").Value)
	True(t, germaineBuddyList[0].RID.ClusterID != -1, "RID should be filled in")

	// now make a circular reference -> give Linus to Germaine as buddy
	sql = `UPDATE Cat SET buddy = ` + germaineRID.String() + ` where name = 'Linus'`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	ret, err := recs.AsInt()
	Equals(t, 1, ret)

	// status of Cat at this point in time
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     #   |@RID |@CLASS|name    |age |caretaker|buddy|buddies        |notes                |buddySet
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     0   |#10:0|Cat   |Linus   |15  |Michael  |#10:7|null           |null                 |null
	//     1   |#10:1|Cat   |Keiko   |10  |Anna     |null |null           |null                 |null
	//     2   |#10:4|Cat   |Tilde   |8   |Earl     |#10:0|null           |null                 |null
	//     3   |#10:5|Cat   |Felix   |6   |Ed       |null |[#10:0, #10:1] |null                 |null
	//     4   |#10:6|Cat   |Charlie |5   |Anna     |null |null           |{bff:#10:0, 30:#10:1}|null
	//     5   |#10:7|Cat   |Germaine|2   |Minnie   |null |[#10:0, #10:1] |{bff:#10:1, 30:#10:0}|[#10:0, #10:5]
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------

	// ---[ queries with extended fetchPlan (simple case) ]---
	sql = `select * from Cat where name = 'Tilde'`
	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	doc = docs[0]
	Equals(t, "Tilde", doc.GetField("name").Value)
	tildeBuddyField := doc.GetField("buddy").Value.(*oschema.OLink)
	Equals(t, linusRID, tildeBuddyField.RID)
	Equals(t, "Linus", tildeBuddyField.Record.GetField("name").Value)

	// now pull in both records with non-null buddy links
	//     Tilde and Linus are the primary docs
	//     Tilde.buddy -> Linus
	//     Linus.buddy -> Felix
	//     Felix.buddies -> Linus and Keiko
	//     so Tilde, Linus, Felix and Keiko should all be pulled in, but only
	//     Tilde and Linus returned directly from the query
	sql = `SELECT FROM Cat where buddy is not null ORDER BY name`

	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, "Linus", docs[0].GetField("name").Value)
	Equals(t, "Tilde", docs[1].GetField("name").Value)

	linusBuddy := docs[0].GetField("buddy").Value.(*oschema.OLink)
	True(t, linusBuddy.Record != nil, "Record should be filled in")
	Equals(t, "Germaine", linusBuddy.Record.GetField("name").Value)

	tildeBuddy := docs[1].GetField("buddy").Value.(*oschema.OLink)
	True(t, tildeBuddy.Record != nil, "Record should be filled in")
	Equals(t, "Linus", tildeBuddy.Record.GetField("name").Value)

	// now check that Felix buddies were pulled in too
	felixDoc = linusBuddy.Record
	felixBuddiesList = felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, 2, len(felixBuddiesList))
	Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)
	Equals(t, "Keiko", felixBuddiesList[1].Record.GetField("name").Value)

	// Linus.buddy links to Felix
	// Felix.buddies links Linux and Keiko
	sql = `SELECT FROM Cat WHERE name = 'Linus' OR name = 'Felix' ORDER BY name DESC`
	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	linusBuddy = docs[0].GetField("buddy").Value.(*oschema.OLink)
	True(t, linusBuddy.Record != nil, "Record should be filled in")
	Equals(t, "Germaine", linusBuddy.Record.GetField("name").Value)

	True(t, docs[1].GetField("buddy") == nil, "Felix should have no 'buddy'")
	felixBuddiesList = docs[1].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)
	Equals(t, "Keiko", felixBuddiesList[1].Record.GetField("name").Value)
	Equals(t, "Anna", felixBuddiesList[1].Record.GetField("caretaker").Value)

	// check that Felix's reference to Linus has Linus' link filled in
	Equals(t, "Germaine", felixBuddiesList[0].Record.GetField("buddy").Value.(*oschema.OLink).Record.GetField("name").Value)

	// ------

	sql = `select * from Cat where buddies is not null ORDER BY name`
	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	felixDoc = docs[0]
	Equals(t, "Felix", felixDoc.GetField("name").Value)
	felixBuddiesList = felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, 2, len(felixBuddiesList))
	felixBuddy0 := felixBuddiesList[0]
	True(t, felixBuddy0.RID.ClusterID != -1, "RID should be filled in")
	Equals(t, "Linus", felixBuddy0.Record.GetField("name").Value)
	felixBuddy1 := felixBuddiesList[1]
	True(t, felixBuddy1.RID.ClusterID != -1, "RID should be filled in")
	Equals(t, "Keiko", felixBuddy1.Record.GetField("name").Value)

	// now test that the LINK docs had their LINKs filled in
	linusDocViaFelix := felixBuddy0.Record
	linusBuddyLink := linusDocViaFelix.GetField("buddy").Value.(*oschema.OLink)
	Equals(t, "Germaine", linusBuddyLink.Record.GetField("name").Value)

	// ------

	// Create two records that reference only each other (a.buddy = b and b.buddy = a)
	//  do:  SELECT FROM Cat where name = "a" OR name = "b" with *:-1 fetchPlan
	//  and make sure if the LINK fields are filled in
	//  with the *:-1 fetchPlan, OrientDB server will return all the link docs in the
	//  "supplementary section" even if they are already in the primary docs section

	sql = `INSERT INTO Cat SET name='Tom', age=3`
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	tomRID := docs[0].RID
	True(t, tomRID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	sql = `INSERT INTO Cat SET name='Nick', age=4, buddy=?`
	docs = nil
	_, err = db.SQLCommand(&docs, sql, tomRID)
	Nil(t, err)
	Equals(t, 1, len(docs))
	nickRID := docs[0].RID

	sql = `UPDATE Cat SET buddy=? WHERE name='Tom' and age=3`
	_, err = db.SQLCommand(nil, sql, nickRID)
	Nil(t, err)

	db.ReloadSchema()

	// in this case the buddy links should be filled in with full Documents
	sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
	docs = nil
	recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql, "Tom", "Nick")
	Nil(t, err)
	Equals(t, 2, len(docs))
	tomDoc := docs[0]
	nickDoc := docs[1]
	Equals(t, "Tom", tomDoc.GetField("name").Value)
	Equals(t, "Nick", nickDoc.GetField("name").Value)

	// TODO: FIX

	//	// TODO: this section fails with orientdb-community-2.1-rc5
	//	tomsBuddy := tomDoc.GetField("buddy").Value.(*oschema.OLink)
	//	nicksBuddy := nickDoc.GetField("buddy").Value.(*oschema.OLink)
	//	// True(t, tomsBuddy.Record != nil, "should have retrieved the link record")
	//	// True(t, nicksBuddy.Record != nil, "should have retrieved the link record")
	//	// Equals(t, "Nick", tomsBuddy.Record.GetField("name").Value)
	//	// Equals(t, "Tom", nicksBuddy.Record.GetField("name").Value)
	//
	//	// in this case the buddy links should NOT be filled in with full Documents
	//	sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
	//	docs = nil
	//	_, err = db.SQLQuery(&docs, nil, sql, "Tom", "Nick")
	//	Nil(t, err)
	//	Equals(t, 2, len(docs))
	//	tomDoc = docs[0]
	//	nickDoc = docs[1]
	//	Equals(t, "Tom", tomDoc.GetField("name").Value)
	//	Equals(t, "Nick", nickDoc.GetField("name").Value)
	//
	//	tomsBuddy = tomDoc.GetField("buddy").Value.(*oschema.OLink)
	//	nicksBuddy = nickDoc.GetField("buddy").Value.(*oschema.OLink)
	//	True(t, tomsBuddy.RID.ClusterID != -1, "RID should be filled in")
	//	True(t, nicksBuddy.RID.ClusterID != -1, "RID should be filled in")
	//	True(t, tomsBuddy.Record == nil, "Record should NOT be filled in")
	//	True(t, nicksBuddy.Record == nil, "Record should NOT be filled in")

	// ------

	// ----+-----+------+--------+----+---------+-----+-------+---------------------+--------
	// #   |@RID |@CLASS|name    |age |caretaker|buddy|buddies|notes                |buddySet
	// ----+-----+------+--------+----+---------+-----+-------+---------------------+--------
	// 0   |#10:0|Cat   |Linus   |15  |Michael  |#10:7|null   |null                 |null
	// 1   |#10:1|Cat   |Keiko   |10  |Anna     |null |null   |null                 |null
	// 2   |#10:4|Cat   |Tilde   |8   |Earl     |#10:0|null   |null                 |null
	// 3   |#10:5|Cat   |Felix   |6   |Ed       |null |[2]    |null                 |null
	// 4   |#10:6|Cat   |Charlie |5   |Anna     |null |null   |{bff:#10:0, 30:#10:1}|null
	// 5   |#10:7|Cat   |Germaine|2   |Minnie   |null |[2]    |{bff:#10:1, 30:#10:0}|[2]
	// 6   |#10:8|Cat   |Tom     |3   |null     |#10:9|null   |null                 |null
	// 7   |#10:9|Cat   |Nick    |4   |null     |#10:8|null   |null                 |null
	// ----+-----+------+--------+----+---------+-----+-------+---------------------+--------

	//
	// Use a fetchPlan that only gets some of the LINKS, not all
	//
	sql = `SELECT from Cat where name = ?`
	docs = nil
	_, err = db.SQLQuery(&docs, &orient.FetchPlan{Plan: "buddy:0 buddies:1 buddySet:0 notes:0"}, sql, "Felix")
	// docs, err = db.SQLQuery(dbc, sql, FetchPlanFollowAllLinks, "Felix")
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Felix", docs[0].GetField("name").Value)
	buddies = docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddies))
	Equals(t, 2, len(buddies))
	linusDoc := buddies[0].Record
	True(t, linusDoc != nil, "first level should be filled in")
	linusBuddy = linusDoc.GetField("buddy").Value.(*oschema.OLink)
	True(t, linusBuddy.RID.ClusterID != -1, "RID should be filled in")
	True(t, linusBuddy.Record == nil, "Record of second level should NOT be filled in")

	keikoDoc := buddies[1].Record
	True(t, keikoDoc != nil, "first level should be filled in")

	// ------

	// ---[ Try DATETIME ]---

	sql = `Create PROPERTY Cat.dt DATETIME`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "dt")
	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `Create PROPERTY Cat.birthday DATE`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "birthday")
	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	// OrientDB DATETIME is precise to the millisecond
	sql = `INSERT into Cat SET name = 'Bruce', dt = '2014-11-25 09:14:54'`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Bruce", docs[0].GetField("name").Value)

	dt := docs[0].GetField("dt").Value.(time.Time)
	zone, zoneOffset := dt.Zone()
	zoneLocation := time.FixedZone(zone, zoneOffset)
	expectedTm, err := time.Parse("2006-01-02 03:04:05", "2014-11-25 09:14:54") //time.ParseInLocation("2006-01-02 03:04:05", "2014-11-25 09:14:54", zoneLocation)
	Nil(t, err)
	Equals(t, expectedTm.Local().String(), dt.String())

	bruceRID := docs[0].RID

	sql = `INSERT into Cat SET name = 'Tiger', birthday = '2014-11-25'`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Tiger", docs[0].GetField("name").Value)

	birthdayTm := docs[0].GetField("birthday").Value.(time.Time)
	zone, zoneOffset = birthdayTm.Zone()
	zoneLocation = time.FixedZone(zone, zoneOffset)
	expectedTm, err = time.ParseInLocation("2006-01-02", "2014-11-25", zoneLocation)
	Nil(t, err)
	Equals(t, expectedTm.String(), birthdayTm.String())

	tigerRID := docs[0].RID

	// ---[ Clean up above expts ]---

	ridsToDelete := []interface{}{felixRID, tildeRID, charlieRID, bruceRID, tigerRID, germaineRID, tomRID, nickRID}
	sql = fmt.Sprintf("DELETE from [%s,%s,%s,%s,%s,%s,%s,%s]", ridsToDelete...)

	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	ret, err = recs.AsInt()
	Nil(t, err)
	Equals(t, len(ridsToDelete), ret)

	db.ReloadSchema()

	sql = "DROP CLASS Patient"
	glog.V(10).Infoln(sql)
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	retb, err := recs.AsBool()
	Nil(t, err)
	Equals(t, true, retb)
}

/*
func createAndUpdateRecordsViaNativeAPI(dbc orient.Client) {
	err := dbc.OpenDatabase(dbc, dbDocumentName, orient.DocumentDB, "admin", "admin")
	Nil(t, err)
	defer dbc.CloseDatabase(dbc)

	// ---[ creation ]---

	winston := oschema.NewDocument("Cat")
	winston.Field("name", "Winston").
		Field("caretaker", "Churchill").
		FieldWithType("age", 7, oschema.INTEGER)
	Equals(t, -1, int(winston.RID.ClusterID))
	Equals(t, -1, int(winston.RID.ClusterPos))
	Equals(t, -1, int(winston.Version))
	err = dbc.CreateRecord(dbc, winston)
	Nil(t, err)
	True(t, int(winston.RID.ClusterID) > -1, "RID should be filled in")
	True(t, int(winston.RID.ClusterPos) > -1, "RID should be filled in")
	True(t, int(winston.Version) > -1, "Version should be filled in")

	// ---[ update STRING and INTEGER field ]---

	versionBefore := winston.Version

	winston.Field("caretaker", "Lolly")      // this updates the field locally
	winston.Field("age", 8)                  // this updates the field locally
	err = dbc.UpdateRecord(dbc, winston) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < winston.Version, "version should have incremented")

	docs, err := db.SQLQuery(dbc, "select * from Cat where @rid="+winston.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))

	winstonFromQuery := docs[0]
	Equals(t, "Winston", winstonFromQuery.GetField("name").Value)
	Equals(t, 8, toInt(winstonFromQuery.GetField("age").Value))
	Equals(t, "Lolly", winstonFromQuery.GetField("caretaker").Value)

	// ---[ next creation ]---

	daemon := oschema.NewDocument("Cat")
	daemon.Field("name", "Daemon").Field("caretaker", "Matt").Field("age", 4)
	err = dbc.CreateRecord(dbc, daemon)
	Nil(t, err)

	indy := oschema.NewDocument("Cat")
	indy.Field("name", "Indy").Field("age", 6)
	err = dbc.CreateRecord(dbc, indy)
	Nil(t, err)

	sql := fmt.Sprintf("select from Cat where @rid=%s or @rid=%s or @rid=%s ORDER BY name",
		winston.RID, daemon.RID, indy.RID)
	resultDocs, err := db.SQLQuery(dbc, sql, "")
	Nil(t, err)
	Equals(t, 3, len(resultDocs))
	Equals(t, daemon.RID, resultDocs[0].RID)
	Equals(t, indy.RID, resultDocs[1].RID)
	Equals(t, winston.RID, resultDocs[2].RID)

	Equals(t, indy.Version, resultDocs[1].Version)
	Equals(t, "Matt", resultDocs[0].GetField("caretaker").Value)

	sql = fmt.Sprintf("DELETE FROM [%s, %s, %s]", winston.RID, daemon.RID, indy.RID)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ---[ Test DATE Serialization ]---
	createAndUpdateRecordsWithDate(dbc)

	// ---[ Test DATETIME Serialization ]---
	createAndUpdateRecordsWithDateTime(dbc)

	// test inserting wrong values for date and datetime
	testCreationOfMismatchedTypesAndValues(dbc)

	// ---[ Test Boolean, Byte and Short Serialization ]---
	createAndUpdateRecordsWithBooleanByteAndShort(dbc)

	// ---[ Test Int, Long, Float and Double Serialization ]---
	createAndUpdateRecordsWithIntLongFloatAndDouble(dbc)

	// ---[ Test BINARY Serialization ]---
	createAndUpdateRecordsWithBINARYType(dbc)

	// ---[ Test EMBEDDEDRECORD Serialization ]---
	createAndUpdateRecordsWithEmbeddedRecords(dbc)

	// ---[ Test EMBEDDEDLIST, EMBEDDEDSET Serialization ]---
	createAndUpdateRecordsWithEmbeddedLists(dbc, oschema.EMBEDDEDLIST)
	createAndUpdateRecordsWithEmbeddedLists(dbc, oschema.EMBEDDEDSET)

	// ---[ Test Link Serialization ]---
	createAndUpdateRecordsWithLinks(dbc)

	// ---[ Test LinkList/LinkSet Serialization ]---
	createAndUpdateRecordsWithLinkLists(dbc, oschema.LINKLIST)
	// createAndUpdateRecordsWithLinkLists(dbc, oschema.LINKSET)  // TODO: get this working

	// ---[ Test LinkMap Serialization ]---
	createAndUpdateRecordsWithLinkMap(dbc)
}

func createAndUpdateRecordsWithLinkMap(dbc orient.Client) {
	sql := `CREATE PROPERTY Cat.notes LINKMAP`
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer removeProperty(dbc, "Cat", "notes")
	ridsToDelete := make([]string, 0, 4)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, err = db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+delrid)
			Nil(t, err)
		}
	}()

	cat1 := oschema.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 1).
		Field("caretaker", "Jackie")

	err = dbc.CreateRecord(dbc, cat1)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	linkToCat1 := &oschema.OLink{RID: cat1.RID, Record: cat1}
	linkmap := map[string]*oschema.OLink{"bff": linkToCat1}

	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "A2").
		Field("age", 2).
		Field("caretaker", "Ben").
		FieldWithType("notes", linkmap, oschema.LINKMAP)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	linkmap["7th-best-friend"] = &oschema.OLink{RID: cat2.RID}

	cat3 := oschema.NewDocument("Cat")
	cat3.Field("name", "A3").
		Field("age", 3).
		Field("caretaker", "Konrad").
		FieldWithType("notes", linkmap, oschema.LINKMAP)

	err = dbc.CreateRecord(dbc, cat3)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	docs, err := db.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Nil(t, err)
	Equals(t, 2, len(docs))

	cat2FromQuery := docs[0]
	Equals(t, "A2", cat2FromQuery.GetField("name").Value)
	Equals(t, 2, toInt(cat2FromQuery.GetField("age").Value))
	notesFromQuery := cat2FromQuery.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 1, len(notesFromQuery))
	Equals(t, notesFromQuery["bff"].RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 3, toInt(cat3FromQuery.GetField("age").Value))
	notesFromQuery = cat3FromQuery.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(notesFromQuery))
	Equals(t, notesFromQuery["bff"].RID, cat1.RID)
	Equals(t, notesFromQuery["7th-best-friend"].RID, cat2.RID)

	///////////////////////

	// ---[ update ]---

	versionBefore := cat3.Version

	// add to cat3's linkmap

	cat3map := cat3.GetField("notes").Value.(map[string]*oschema.OLink)
	cat3map["new1"] = &oschema.OLink{RID: cat2.RID}
	cat3map["new2"] = &oschema.OLink{RID: cat2.RID}

	err = dbc.UpdateRecord(dbc, cat3) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat3.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat3.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat3FromQuery = docs[0]

	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	cat3MapFromQuery := cat3FromQuery.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 4, len(cat3MapFromQuery))
	Equals(t, cat3MapFromQuery["bff"].RID, cat1.RID)
	Equals(t, cat3MapFromQuery["7th-best-friend"].RID, cat2.RID)
	Equals(t, cat3MapFromQuery["new1"].RID, cat2.RID)
	Equals(t, cat3MapFromQuery["new2"].RID, cat2.RID)
}

func createAndUpdateRecordsWithLinkLists(dbc orient.Client, collType oschema.OType) {
	sql := "CREATE PROPERTY Cat.catfriends " + oschema.ODataTypeNameFor(collType) + " Cat"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer removeProperty(dbc, "Cat", "catfriends")
	ridsToDelete := make([]string, 0, 4)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, err = db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+delrid)
			Nil(t, err)
		}
	}()

	cat1 := oschema.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 1).
		Field("caretaker", "Jackie")

	err = dbc.CreateRecord(dbc, cat1)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	linkToCat1 := &oschema.OLink{RID: cat1.RID, Record: cat1}

	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "A2").
		Field("age", 2).
		Field("caretaker", "Ben").
		FieldWithType("catfriends", []*oschema.OLink{linkToCat1}, collType)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	linkToCat2 := &oschema.OLink{RID: cat2.RID}
	twoCatLinks := []*oschema.OLink{linkToCat1, linkToCat2}

	cat3 := oschema.NewDocument("Cat")
	cat3.Field("name", "A3")

	if collType == oschema.LINKSET {
		cat3.FieldWithType("catfriends", twoCatLinks, collType)
	} else {
		cat3.Field("catfriends", twoCatLinks)
	}
	cat3.Field("age", 3).
		Field("caretaker", "Conrad")

	err = dbc.CreateRecord(dbc, cat3)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	docs, err := db.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Nil(t, err)
	Equals(t, 2, len(docs))

	cat2FromQuery := docs[0]
	Equals(t, "A2", cat2FromQuery.GetField("name").Value)
	Equals(t, 2, toInt(cat2FromQuery.GetField("age").Value))
	catFriendsFromQuery := cat2FromQuery.GetField("catfriends").Value.([]*oschema.OLink)
	Equals(t, 1, len(catFriendsFromQuery))
	Equals(t, catFriendsFromQuery[0].RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 3, toInt(cat3FromQuery.GetField("age").Value))
	catFriendsFromQuery = cat3FromQuery.GetField("catfriends").Value.([]*oschema.OLink)
	Equals(t, 2, len(catFriendsFromQuery))
	sort.Sort(byRID(catFriendsFromQuery))
	Equals(t, catFriendsFromQuery[0].RID, cat1.RID)
	Equals(t, catFriendsFromQuery[1].RID, cat2.RID)

	// ---[ update ]---

	versionBefore := cat3.Version

	// cat2 ("A2") currently has linklist to cat1 ("A2")
	// -> change this to a linklist to cat1 and cat3

	linkToCat3 := &oschema.OLink{RID: cat3.RID}
	linksCat1and3 := []*oschema.OLink{linkToCat1, linkToCat3}

	cat2.Field("catfriends", linksCat1and3) // updates the field locally

	err = dbc.UpdateRecord(dbc, cat2) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat2.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat2.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat2FromQuery = docs[0]

	Equals(t, "A2", cat2FromQuery.GetField("name").Value)
	catFriendsFromQuery = cat2FromQuery.GetField("catfriends").Value.([]*oschema.OLink)
	Equals(t, 2, len(catFriendsFromQuery))
	sort.Sort(byRID(catFriendsFromQuery))
	Equals(t, catFriendsFromQuery[0].RID, cat1.RID)
	Equals(t, catFriendsFromQuery[1].RID, cat3.RID)
}

func createAndUpdateRecordsWithLinks(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.catlink LINK Cat"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer removeProperty(dbc, "Cat", "catlink")
	ridsToDelete := make([]string, 0, 10)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, err = db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+delrid)
			Nil(t, err)
		}
	}()

	// ------

	cat1 := oschema.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 2).
		Field("caretaker", "Jackie")

	err = dbc.CreateRecord(dbc, cat1)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	cat2 := oschema.NewDocument("Cat")
	linkToCat1 := &oschema.OLink{RID: cat1.RID, Record: cat1}
	cat2.Field("name", "A2").
		Field("age", 3).
		Field("caretaker", "Jimmy").
		FieldWithType("catlink", linkToCat1, oschema.LINK)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	// ---[ try without FieldWithType ]---

	cat3 := oschema.NewDocument("Cat")
	linkToCat2 := &oschema.OLink{RID: cat2.RID, Record: cat2} // also, only use RID, not record
	cat3.Field("name", "A3").
		Field("age", 4).
		Field("caretaker", "Ralston").
		Field("catlink", linkToCat2)

	err = dbc.CreateRecord(dbc, cat3)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	// test that they were inserted correctly and come back correctly

	docs, err := db.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Nil(t, err)
	Equals(t, 2, len(docs))

	cat2FromQuery := docs[0]
	Equals(t, "A2", cat2FromQuery.GetField("name").Value)
	Equals(t, 3, toInt(cat2FromQuery.GetField("age").Value))
	linkToCat1FromQuery := cat2FromQuery.GetField("catlink").Value.(*oschema.OLink)
	Equals(t, linkToCat1FromQuery.RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 4, toInt(cat3FromQuery.GetField("age").Value))
	linkToCat2FromQuery := cat3FromQuery.GetField("catlink").Value.(*oschema.OLink)
	Equals(t, linkToCat2FromQuery.RID, cat2.RID)

	// ---[ update ]---

	versionBefore := cat3.Version

	// cat3 ("A3") currently has link to cat2 ("A2")
	// -> change this to a link to cat1 ("A1")

	cat3.Field("catlink", linkToCat1) // updates the field locally

	err = dbc.UpdateRecord(dbc, cat3) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat3.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat3.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat3FromQuery = docs[0]

	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 4, toInt(cat3FromQuery.GetField("age").Value))
	linkToCat1FromQuery = cat3FromQuery.GetField("catlink").Value.(*oschema.OLink)
	Equals(t, linkToCat1FromQuery.RID, cat1.RID)
}

func createAndUpdateRecordsWithEmbeddedLists(dbc orient.Client, embType oschema.OType) {
	sql := "CREATE PROPERTY Cat.embstrings " + embType.String() + " string"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	sql = "CREATE PROPERTY Cat.emblongs " + embType.String() + " LONG"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	sql = "CREATE PROPERTY Cat.embcats " + embType.String() + " Cat"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ------
	// housekeeping

	defer removeProperty(dbc, "Cat", "embstrings")
	defer removeProperty(dbc, "Cat", "emblongs")
	defer removeProperty(dbc, "Cat", "embcats")
	ridsToDelete := make([]string, 0, 10)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, err = db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+delrid)
			Nil(t, err)
		}
	}()

	// ------

	embStrings := []interface{}{"one", "two", "three"}
	stringList := oschema.NewEmbeddedSlice(embStrings, oschema.STRING)

	Equals(t, oschema.STRING, stringList.Type())
	Equals(t, "two", stringList.Values()[1])

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "Yugo").
		Field("age", 33)

	if embType == oschema.EMBEDDEDLIST {
		cat.Field("embstrings", stringList)
	} else {
		cat.FieldWithType("embstrings", stringList, embType)
	}

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	True(t, cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err := db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery := docs[0]
	Equals(t, 33, toInt(catFromQuery.GetField("age").Value))
	embstringsFieldFromQuery := catFromQuery.GetField("embstrings")

	Equals(t, "embstrings", embstringsFieldFromQuery.Name)
	Equals(t, embType, embstringsFieldFromQuery.Type)
	embListFromQuery, ok := embstringsFieldFromQuery.Value.([]interface{})
	True(t, ok, "Cast to oschema.[]interface{} failed")

	sort.Sort(byStringVal(embListFromQuery))
	Equals(t, 3, len(embListFromQuery))
	Equals(t, "one", embListFromQuery[0])
	Equals(t, "three", embListFromQuery[1])
	Equals(t, "two", embListFromQuery[2])

	// ------

	embLongs := []interface{}{int64(22), int64(4444), int64(constants.MaxInt64 - 12)}
	int64List := oschema.NewEmbeddedSlice(embLongs, oschema.LONG)

	Equals(t, oschema.LONG, int64List.Type())
	Equals(t, int64(22), int64List.Values()[0])

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "Barry").
		Field("age", 40).
		FieldWithType("emblongs", int64List, embType)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	True(t, cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]
	Equals(t, 40, toInt(catFromQuery.GetField("age").Value))
	emblongsFieldFromQuery := catFromQuery.GetField("emblongs")

	Equals(t, "emblongs", emblongsFieldFromQuery.Name)
	Equals(t, embType, emblongsFieldFromQuery.Type)
	embListFromQuery, ok = emblongsFieldFromQuery.Value.([]interface{})
	True(t, ok, "Cast to oschema.[]interface{} failed")

	sort.Sort(byLongVal(embListFromQuery))
	Equals(t, 3, len(embListFromQuery))
	Equals(t, int64(22), embListFromQuery[0])
	Equals(t, int64(4444), embListFromQuery[1])
	Equals(t, int64(constants.MaxInt64-12), embListFromQuery[2])

	// ------

	// how to insert into embcats from the OrientDB console:
	// insert into Cat set name="Draydon", age=223, embcats=[{"@class":"Cat", "name": "geary", "age":33}, {"@class":"Cat", "name": "joan", "age": 44}]

	embCat0 := oschema.NewDocument("Cat")
	embCat0.Field("name", "Gordo").Field("age", 40)

	embCat1 := oschema.NewDocument("Cat")
	embCat1.Field("name", "Joan").Field("age", 14).Field("caretaker", "Marcia")

	embCats := []interface{}{embCat0, embCat1}
	embcatList := oschema.NewEmbeddedSlice(embCats, oschema.EMBEDDED)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "Draydon").
		Field("age", 3)

	if embType == oschema.EMBEDDEDLIST {
		cat.Field("embcats", embcatList)
	} else {
		cat.FieldWithType("embcats", embcatList, embType)
	}

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	True(t, cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]
	Equals(t, 3, toInt(catFromQuery.GetField("age").Value))
	embcatsFieldFromQuery := catFromQuery.GetField("embcats")

	Equals(t, "embcats", embcatsFieldFromQuery.Name)
	Equals(t, embType, embcatsFieldFromQuery.Type)
	embListFromQuery, ok = embcatsFieldFromQuery.Value.([]interface{})
	True(t, ok, "Cast to oschema.[]interface{} failed")

	Equals(t, 2, len(embListFromQuery))
	sort.Sort(byEmbeddedCatName(embListFromQuery))

	embCatDoc0, ok := embListFromQuery[0].(*oschema.ODocument)
	True(t, ok, "Cast to *oschema.ODocument failed")
	embCatDoc1, ok := embListFromQuery[1].(*oschema.ODocument)
	True(t, ok, "Cast to *oschema.ODocument failed")

	Equals(t, "Gordo", embCatDoc0.GetField("name").Value)
	Equals(t, 40, toInt(embCatDoc0.GetField("age").Value))
	Equals(t, "Joan", embCatDoc1.GetField("name").Value)
	Equals(t, "Marcia", embCatDoc1.GetField("caretaker").Value)

	// ---[ update ]---

	// update embedded string list
	versionBefore := cat.Version

	newEmbStrings := []interface{}{"A", "BB", "CCCC"}
	newStringList := oschema.NewEmbeddedSlice(newEmbStrings, oschema.STRING)
	cat.FieldWithType("embstrings", newStringList, embType) // updates the field locally

	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]

	embstringsFieldFromQuery = catFromQuery.GetField("embstrings")

	Equals(t, "embstrings", embstringsFieldFromQuery.Name)
	Equals(t, embType, embstringsFieldFromQuery.Type)
	embListFromQuery, ok = embstringsFieldFromQuery.Value.([]interface{})
	True(t, ok, "Cast to oschema.[]interface{} failed")

	sort.Sort(byStringVal(embListFromQuery))
	Equals(t, 3, len(embListFromQuery))
	Equals(t, "A", embListFromQuery[0])
	Equals(t, "BB", embListFromQuery[1])
	Equals(t, "CCCC", embListFromQuery[2])

	// update embedded long list + embedded Cats

	newEmbLongs := []interface{}{int64(18), int64(1234567890)}
	newInt64List := oschema.NewEmbeddedSlice(newEmbLongs, oschema.LONG)

	cat.FieldWithType("emblongs", newInt64List, embType)

	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]

	emblongsFieldFromQuery = catFromQuery.GetField("emblongs")

	Equals(t, "emblongs", emblongsFieldFromQuery.Name)
	Equals(t, embType, emblongsFieldFromQuery.Type)
	embListFromQuery, ok = emblongsFieldFromQuery.Value.([]interface{})
	True(t, ok, "Cast to oschema.[]interface{} failed")

	sort.Sort(byLongVal(embListFromQuery))
	Equals(t, 2, len(embListFromQuery))
	Equals(t, int64(18), embListFromQuery[0])
	Equals(t, int64(1234567890), embListFromQuery[1])

	// add another cat to the embedded cat list
	embCat2 := oschema.NewDocument("Cat")
	embCat2.Field("name", "Mickey").Field("age", 1)

	cat.GetField("embcats").Value.(oschema.OEmbeddedList).Add(embCat2)

	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]

	embCatsFieldFromQuery := catFromQuery.GetField("embcats")

	Equals(t, "embcats", embCatsFieldFromQuery.Name)
	Equals(t, embType, embCatsFieldFromQuery.Type)
	embListFromQuery, ok = embCatsFieldFromQuery.Value.([]interface{})
	True(t, ok, "Cast to oschema.[]interface{} failed")

	Equals(t, 3, len(embListFromQuery))
	sort.Sort(byEmbeddedCatName(embListFromQuery))

	embCatDoc0, ok = embListFromQuery[0].(*oschema.ODocument)
	True(t, ok, "Cast to *oschema.ODocument failed")
	embCatDoc1, ok = embListFromQuery[1].(*oschema.ODocument)
	True(t, ok, "Cast to *oschema.ODocument failed")
	embCatDoc2, ok := embListFromQuery[2].(*oschema.ODocument)
	True(t, ok, "Cast to *oschema.ODocument failed")

	Equals(t, "Gordo", embCatDoc0.GetField("name").Value)
	Equals(t, 40, toInt(embCatDoc0.GetField("age").Value))
	Equals(t, "Joan", embCatDoc1.GetField("name").Value)
	Equals(t, "Marcia", embCatDoc1.GetField("caretaker").Value)
	Equals(t, "Mickey", embCatDoc2.GetField("name").Value)
	Equals(t, 1, toInt(embCatDoc2.GetField("age").Value))

}

func createAndUpdateRecordsWithEmbeddedRecords(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.embcat EMBEDDED"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer removeProperty(dbc, "Cat", "embcat")

	ridsToDelete := make([]string, 0, 10)
	defer func() {
		for _, delrid := range ridsToDelete {
			_, err = db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+delrid)
			Nil(t, err)
		}
	}()

	// ---[ FieldWithType ]---

	embcat := oschema.NewDocument("Cat")
	embcat.Field("name", "MaryLulu").
		Field("age", 47)

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "Willard").
		Field("age", 4).
		FieldWithType("embcat", embcat, oschema.EMBEDDED)

	// err = db.ReloadSchema(dbc) // TMP => LEFT OFF: try without this => does it work if write name and type, rather than id?
	// Nil(t, err)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)

	True(t, int(embcat.RID.ClusterID) < int(0), "embedded RID should be NOT filled in")
	True(t, cat.RID.ClusterID >= 0, "RID should be filled in")

	ridsToDelete = append(ridsToDelete, cat.RID.String())

	docs, err := db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery := docs[0]
	Equals(t, "Willard", catFromQuery.GetField("name").Value.(string))
	Equals(t, 4, toInt(catFromQuery.GetField("age").Value))
	Equals(t, oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	embCatFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	True(t, embCatFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	True(t, embCatFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	True(t, embCatFromQuery.Version < 0, "Version should be unset")
	Equals(t, 2, len(embCatFromQuery.FieldNames()))
	Equals(t, 47, toInt(embCatFromQuery.GetField("age").Value))
	Equals(t, "MaryLulu", embCatFromQuery.GetField("name").Value.(string))

	// ---[ Field No Type Specified ]---

	embcat = oschema.NewDocument("Cat")
	embcat.Field("name", "Tsunami").
		Field("age", 33).
		Field("purebreed", false)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "Cara").
		Field("age", 3).
		Field("embcat", embcat)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	True(t, int(embcat.RID.ClusterID) < int(0), "embedded RID should be NOT filled in")
	True(t, cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery = docs[0]
	Equals(t, "Cara", catFromQuery.GetField("name").Value.(string))
	Equals(t, 3, toInt(catFromQuery.GetField("age").Value))
	Equals(t, oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	embCatFromQuery = catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	True(t, embCatFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	True(t, embCatFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	True(t, embCatFromQuery.Version < 0, "Version should be unset")
	Equals(t, "Cat", embCatFromQuery.Classname)
	Equals(t, 3, len(embCatFromQuery.FieldNames()))
	Equals(t, 33, toInt(embCatFromQuery.GetField("age").Value))
	Equals(t, "Tsunami", embCatFromQuery.GetField("name").Value.(string))
	Equals(t, false, embCatFromQuery.GetField("purebreed").Value.(bool))

	// ---[ Embedded with New Classname (not in DB) ]---

	moonpie := oschema.NewDocument("Moonpie")
	moonpie.Field("sku", "AB425827ACX3").
		Field("allnatural", false).
		FieldWithType("oz", 6.5, oschema.FLOAT)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "LeCara").
		Field("age", 7).
		Field("embcat", moonpie)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery = docs[0]
	Equals(t, "LeCara", catFromQuery.GetField("name").Value.(string))
	Equals(t, 7, toInt(catFromQuery.GetField("age").Value))
	Equals(t, oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	moonpieFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	True(t, moonpieFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	True(t, moonpieFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	True(t, moonpieFromQuery.Version < 0, "Version should be unset")
	Equals(t, "", moonpieFromQuery.Classname) // it throws out the classname => TODO: check serialized binary on this
	Equals(t, 3, len(moonpieFromQuery.FieldNames()))
	Equals(t, "AB425827ACX3", moonpieFromQuery.GetField("sku").Value)
	Equals(t, float32(6.5), moonpieFromQuery.GetField("oz").Value.(float32))
	Equals(t, false, moonpieFromQuery.GetField("allnatural").Value.(bool))

	noclass := oschema.NewDocument("")
	noclass.Field("sku", "AB425827ACX3222").
		Field("allnatural", true).
		FieldWithType("oz", 6.5, oschema.DOUBLE)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "LeCarre").
		Field("age", 87).
		Field("embcat", noclass)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery = docs[0]
	Equals(t, "LeCarre", catFromQuery.GetField("name").Value.(string))
	Equals(t, 87, toInt(catFromQuery.GetField("age").Value))
	Equals(t, oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	noclassFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Equals(t, "", noclassFromQuery.Classname) // it throws out the classname
	Equals(t, 3, len(noclassFromQuery.FieldNames()))
	Equals(t, "AB425827ACX3222", noclassFromQuery.GetField("sku").Value)
	Equals(t, float64(6.5), noclassFromQuery.GetField("oz").Value.(float64))
	Equals(t, true, noclassFromQuery.GetField("allnatural").Value.(bool))

	// ---[ update ]---

	versionBefore := cat.Version

	moonshine := oschema.NewDocument("")
	moonshine.Field("sku", "123").
		Field("allnatural", true).
		FieldWithType("oz", 99.092, oschema.FLOAT)

	cat.FieldWithType("embcat", moonshine, oschema.EMBEDDED) // updates the field locally

	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]

	mshineFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Equals(t, "123", mshineFromQuery.GetField("sku").Value)
	Equals(t, true, mshineFromQuery.GetField("allnatural").Value)
	Equals(t, float32(99.092), mshineFromQuery.GetField("oz").Value)
}

func createAndUpdateRecordsWithBINARYType(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.bin BINARY"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.bin")
	}()

	// ---[ FieldWithType ]---
	str := "four, five, six, pick up sticks"
	bindata := []byte(str)

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "little-jimmy").
		Field("age", 1).
		FieldWithType("bin", bindata, oschema.BINARY)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	True(t, cat.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+cat.RID.String())
	}()

	docs, err := db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery := docs[0]

	Equals(t, cat.GetField("bin").Value, catFromQuery.GetField("bin").Value)
	Equals(t, str, string(catFromQuery.GetField("bin").Value.([]byte)))

	// ---[ Field No Type Specified ]---
	binN := 6500 // TODO: can't go much above ~650K bytes => why? is this an OrientDB limit?
	// TODO: or do we need to do a second query -> determine how the Java client does this
	bindata2 := make([]byte, binN)

	for i := 0; i < binN; i++ {
		bindata2[i] = byte(i)
	}

	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "Sauron").
		Field("age", 1111).
		Field("bin", bindata2)

	True(t, cat2.RID.ClusterID <= 0, "RID should NOT be filled in")

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	True(t, cat2.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+cat2.RID.String())
	}()

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat2.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat2FromQuery := docs[0]

	Equals(t, bindata2, cat2FromQuery.GetField("bin").Value.([]byte))

	// ---[ update ]---

	versionBefore := cat.Version

	newbindata := []byte("Now Gluten Free!")
	cat.FieldWithType("bin", newbindata, oschema.BINARY)
	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]
	Equals(t, newbindata, catFromQuery.GetField("bin").Value)
}

func createAndUpdateRecordsWithIntLongFloatAndDouble(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.ii INTEGER"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.ii")
	}()

	sql = "CREATE PROPERTY Cat.lg LONG"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.lg")
	}()

	sql = "CREATE PROPERTY Cat.ff FLOAT"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.ff")
	}()

	sql = "CREATE PROPERTY Cat.dd DOUBLE"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.dd")
	}()

	floatval := float32(constants.MaxInt32) + 0.5834
	// doubleval := float64(7976931348623157 + e308) // Double.MIN_VALUE in Java => too big for Go
	// doubleval := float64(9E-324) // Double.MIN_VALUE in Java  => too big for Go
	doubleval := float64(7.976931348623157E+222)

	// ---[ FieldWithType ]---
	cat := oschema.NewDocument("Cat")
	cat.Field("name", "sourpuss").
		Field("age", 15).
		FieldWithType("ii", constants.MaxInt32, oschema.INTEGER).
		FieldWithType("lg", constants.MaxInt64, oschema.LONG).
		FieldWithType("ff", floatval, oschema.FLOAT).
		FieldWithType("dd", doubleval, oschema.DOUBLE)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	True(t, cat.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+cat.RID.String())
	}()

	docs, err := db.SQLQuery(dbc, "select from Cat where ii = ?", "", strconv.Itoa(int(constants.MaxInt32)))
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery := docs[0]

	Equals(t, toInt(cat.GetField("ii").Value), toInt(catFromQuery.GetField("ii").Value))
	Equals(t, toInt(cat.GetField("lg").Value), toInt(catFromQuery.GetField("lg").Value))
	Equals(t, cat.GetField("ff").Value, catFromQuery.GetField("ff").Value)
	Equals(t, cat.GetField("dd").Value, catFromQuery.GetField("dd").Value)

	// ---[ Field ]---

	iival := int32(constants.MaxInt32) - 100
	lgval := int64(constants.MinInt64) + 4
	ffval := float32(constants.MinInt32) * 4.996413569
	ddval := float64(-9.834782455017E+225)

	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "Jerry").
		Field("age", 18).
		Field("ii", iival).
		Field("lg", lgval).
		Field("ff", ffval).
		Field("dd", ddval)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)

	True(t, cat2.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+cat2.RID.String())
	}()

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat2.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat2FromQuery := docs[0]

	Equals(t, toInt(cat2.GetField("ii").Value), toInt(cat2FromQuery.GetField("ii").Value))
	Equals(t, toInt(cat2.GetField("lg").Value), toInt(cat2FromQuery.GetField("lg").Value))
	Equals(t, cat2.GetField("ff").Value, cat2FromQuery.GetField("ff").Value)
	Equals(t, cat2.GetField("dd").Value, cat2FromQuery.GetField("dd").Value)

	// ---[ update ]---

	cat2.Field("ii", int32(1)).
		Field("lg", int64(2)).
		Field("ff", float32(3.3)).
		Field("dd", float64(4.444))

	versionBefore := cat2.Version

	err = dbc.UpdateRecord(dbc, cat2) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat2.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat2.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat2FromQuery = docs[0]
	Equals(t, int32(1), cat2FromQuery.GetField("ii").Value)
	Equals(t, int64(2), cat2FromQuery.GetField("lg").Value)
	Equals(t, float32(3.3), cat2FromQuery.GetField("ff").Value)
	Equals(t, float64(4.444), cat2FromQuery.GetField("dd").Value)
}
*/
func toInt(value interface{}) int {
	switch value.(type) {
	case int:
		return value.(int)
	case int32:
		return int(value.(int32))
	case int64:
		return int(value.(int64))
	}
	panic(fmt.Sprintf("Value %v cannot be cast to int", value))
}

/*
func createAndUpdateRecordsWithBooleanByteAndShort(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.x BOOLEAN"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.x")
	}()

	sql = "CREATE PROPERTY Cat.y BYTE"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.y")
	}()

	sql = "CREATE PROPERTY Cat.z SHORT"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.z")
	}()

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "kitteh").
		Field("age", 4).
		Field("x", false).
		Field("y", byte(55)).
		Field("z", int16(5123))

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
	True(t, cat.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+cat.RID.String())
	}()

	docs, err := db.SQLQuery(dbc, "select from Cat where y = 55", "")
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery := docs[0]
	Equals(t, cat.GetField("x").Value.(bool), catFromQuery.GetField("x").Value.(bool))
	Equals(t, cat.GetField("y").Value.(byte), catFromQuery.GetField("y").Value.(byte))
	Equals(t, cat.GetField("z").Value.(int16), catFromQuery.GetField("z").Value.(int16))

	// try with explicit types
	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "cat2").
		Field("age", 14).
		FieldWithType("x", true, oschema.BOOLEAN).
		FieldWithType("y", byte(44), oschema.BYTE).
		FieldWithType("z", int16(16000), oschema.SHORT)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	True(t, cat2.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		db.SQLCommand(nil, "DELETE FROM Cat WHERE @rid="+cat2.RID.String())
	}()

	docs, err = db.SQLQuery(dbc, "select from Cat where x = true", "")
	Nil(t, err)
	Equals(t, 1, len(docs))

	cat2FromQuery := docs[0]
	Equals(t, cat2.GetField("x").Value.(bool), cat2FromQuery.GetField("x").Value.(bool))
	Equals(t, cat2.GetField("y").Value.(byte), cat2FromQuery.GetField("y").Value.(byte))
	Equals(t, cat2.GetField("z").Value.(int16), cat2FromQuery.GetField("z").Value.(int16))

	// ---[ update ]---

	versionBefore := cat.Version

	cat.Field("x", true)
	cat.Field("y", byte(19))
	cat.Field("z", int16(6789))

	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]
	Equals(t, true, catFromQuery.GetField("x").Value)
	Equals(t, byte(19), catFromQuery.GetField("y").Value)
	Equals(t, int16(6789), catFromQuery.GetField("z").Value)

}

func testCreationOfMismatchedTypesAndValues(dbc orient.Client) {
	c1 := oschema.NewDocument("Cat")
	c1.Field("name", "fluffy1").
		Field("age", 22).
		FieldWithType("ddd", "not a datetime", oschema.DATETIME)
	err := dbc.CreateRecord(dbc, c1)
	True(t, err != nil, "Should have returned error")
	_, ok := oerror.ExtractCause(err).(oerror.ErrDataTypeMismatch)
	True(t, ok, "should be DataTypeMismatch error")

	c2 := oschema.NewDocument("Cat")
	c2.Field("name", "fluffy1").
		Field("age", 22).
		FieldWithType("ddd", float32(33244.2), oschema.DATE)
	err = dbc.CreateRecord(dbc, c2)
	True(t, err != nil, "Should have returned error")
	_, ok = oerror.ExtractCause(err).(oerror.ErrDataTypeMismatch)
	True(t, ok, "should be DataTypeMismatch error")

	// no fluffy1 should be in the database
	docs, err := db.SQLQuery(dbc, "select from Cat where name = 'fluffy1'", "")
	Nil(t, err)
	Equals(t, 0, len(docs))
}

func createAndUpdateRecordsWithDateTime(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.ddd DATETIME"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.ddd")
	}()

	// ---[ creation ]---

	now := time.Now()
	simba := oschema.NewDocument("Cat")
	simba.Field("name", "Simba").
		Field("age", 11).
		FieldWithType("ddd", now, oschema.DATETIME)
	err = dbc.CreateRecord(dbc, simba)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DELETE FROM "+simba.RID.String())
	}()

	True(t, simba.RID.ClusterID > 0, "ClusterID should be set")
	True(t, simba.RID.ClusterPos >= 0, "ClusterID should be set")

	docs, err := db.SQLQuery(dbc, "select from Cat where @rid="+simba.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	simbaFromQuery := docs[0]
	Equals(t, simba.RID, simbaFromQuery.RID)
	Equals(t, simba.GetField("ddd").Value, simbaFromQuery.GetField("ddd").Value)

	// ---[ update ]---

	versionBefore := simba.Version

	twoDaysAgo := now.AddDate(0, 0, -2)

	simba.FieldWithType("ddd", twoDaysAgo, oschema.DATETIME) // updates the field locally
	err = dbc.UpdateRecord(dbc, simba)                   // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < simba.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+simba.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	simbaFromQuery = docs[0]
	Equals(t, twoDaysAgo.Unix(), simbaFromQuery.GetField("ddd").Value.(time.Time).Unix())
}

func createAndUpdateRecordsWithDate(dbc orient.Client) {
	sql := "CREATE PROPERTY Cat.bday DATE"
	_, err := db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DROP PROPERTY Cat.bday")
	}()

	const dtTemplate = "Jan 2, 2006 at 3:04pm (MST)"
	bdayTm, err := time.Parse(dtTemplate, "Feb 3, 1932 at 7:54pm (EST)")
	Nil(t, err)

	jj := oschema.NewDocument("Cat")
	jj.Field("name", "JJ").
		Field("age", 2).
		FieldWithType("bday", bdayTm, oschema.DATE)
	err = dbc.CreateRecord(dbc, jj)
	Nil(t, err)

	defer func() {
		db.SQLCommand(nil, "DELETE FROM "+jj.RID.String())
	}()

	True(t, jj.RID.ClusterID > 0, "ClusterID should be set")
	True(t, jj.RID.ClusterPos >= 0, "ClusterID should be set")
	jjbdayAfterCreate := jj.GetField("bday").Value.(time.Time)
	Equals(t, 0, jjbdayAfterCreate.Hour())
	Equals(t, 0, jjbdayAfterCreate.Minute())
	Equals(t, 0, jjbdayAfterCreate.Second())

	docs, err := db.SQLQuery(dbc, "select from Cat where @rid="+jj.RID.String(), "")
	Equals(t, 1, len(docs))
	jjFromQuery := docs[0]
	Equals(t, jj.RID, jjFromQuery.RID)
	Equals(t, 1932, jjFromQuery.GetField("bday").Value.(time.Time).Year())

	// ---[ update ]---

	versionBefore := jj.Version

	oneYearLater := bdayTm.AddDate(1, 0, 0)

	jj.FieldWithType("bday", oneYearLater, oschema.DATE) // updates the field locally
	err = dbc.UpdateRecord(dbc, jj)                  // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < jj.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+jj.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	jjFromQuery = docs[0]
	Equals(t, 1933, jjFromQuery.GetField("bday").Value.(time.Time).Year())
}
*/
// ------

func removeProperty(db orient.Database, class, property string) {
	sql := fmt.Sprintf("UPDATE %s REMOVE %s", class, property)
	_, err := db.SQLCommand(nil, sql)
	if err != nil {
		glog.Warningf("WARN: clean up error: %v\n", err)
	}
	sql = fmt.Sprintf("DROP PROPERTY %s.%s", class, property)
	_, err = db.SQLCommand(nil, sql)
	if err != nil {
		glog.Warningf("WARN: clean up error: %v\n", err)
	}
}

// ------
// Sort OLinks by RID

type byRID []*oschema.OLink

func (slnk byRID) Len() int {
	return len(slnk)
}

func (slnk byRID) Swap(i, j int) {
	slnk[i], slnk[j] = slnk[j], slnk[i]
}

func (slnk byRID) Less(i, j int) bool {
	return slnk[i].RID.String() < slnk[j].RID.String()
}

// ------
// sort ODocuments by name field

type byEmbeddedCatName []interface{}

func (a byEmbeddedCatName) Len() int {
	return len(a)
}

func (a byEmbeddedCatName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byEmbeddedCatName) Less(i, j int) bool {
	return a[i].(*oschema.ODocument).GetField("name").Value.(string) < a[j].(*oschema.ODocument).GetField("name").Value.(string)
}

// ------

type byStringVal []interface{}

func (sv byStringVal) Len() int {
	return len(sv)
}

func (sv byStringVal) Swap(i, j int) {
	sv[i], sv[j] = sv[j], sv[i]
}

func (sv byStringVal) Less(i, j int) bool {
	return sv[i].(string) < sv[j].(string)
}

// ------

type byLongVal []interface{}

func (sv byLongVal) Len() int {
	return len(sv)
}

func (sv byLongVal) Swap(i, j int) {
	sv[i], sv[j] = sv[j], sv[i]
}

func (sv byLongVal) Less(i, j int) bool {
	return sv[i].(int64) < sv[j].(int64)
}

// ------
/*
func ogonoriTestAgainstOrientDBServer() {
	var (
		dbc orient.Client
		err error
	)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// ---[ set ogl log level ]---
	ogl.SetLevel(ogl.WARN)

	testType := "dataOnly"

	if len(os.Args) > 1 {
		if os.Args[1] == "full" || os.Args[1] == "create" {
			testType = os.Args[1]
		}
	}

	dbc, err = dbc.NewDBClient(obinary.ClientOptions{})
	Nil(t, err)
	defer dbc.Close()

	// ---[ run clean up in case of panics ]---
	defer func() {
		if r := recover(); r != nil {
			lvl := ogl.GetLevel()
			ogl.SetLevel(ogl.NORMAL)
			switch r {
			case "Equals fail", "Assert fail", "Ok fail":
				// do not print stack trace
			default:
				glog.Infof("panic recovery: %v\nTrace:\n%s\n", r, debug.Stack())
			}
			ogl.SetLevel(lvl)
			cleanUp(dbc, testType == "full")
			os.Exit(1)
		}
	}()

	// ---[ Use "native" API ]---
	createOgonoriTestDB(dbc, dbUser, dbPass, testType != "dataOnly")
	defer cleanUp(dbc, testType == "full")

	// document database tests
	ogl.SetLevel(ogl.WARN)
	dbCommandsNativeAPI(dbc, testType != "dataOnly")
	if testType == "full" {
		ogl.SetLevel(ogl.WARN)
		dbClusterCommandsNativeAPI(dbc)
	}

	// create new records from low-level create API (not SQL)
	createAndUpdateRecordsViaNativeAPI(dbc)

	// ---[ Use Go database/sql API on Document DB ]---
	ogl.SetLevel(ogl.WARN)
	conxStr := "admin@admin:localhost/" + dbDocumentName
	databaseSQLAPI(conxStr)
	databaseSQLPreparedStmtAPI(conxStr)

	// ---[ Graph DB ]---
	// graph database tests
	ogl.SetLevel(ogl.WARN)
	graphCommandsNativeAPI(dbc, testType != "dataOnly")
	graphConxStr := "admin@admin:localhost/" + dbGraphName
	ogl.SetLevel(ogl.NORMAL)
	graphCommandsSQLAPI(graphConxStr)

	// ------

	//
	// experimenting with JSON functionality
	//
	// glog.Infoln("-------- JSON ---------")
	// fld := oschema.OField{int32(44), "foo", oschema.LONG, int64(33341234)}
	// bsjson, err := fld.ToJSON()
	// Nil(t, err)
	// glog.Infof("%v\n", string(bsjson))

	// doc := oschema.NewDocument("Coolio")
	// doc.AddField("foo", &fld)
	// bsjson, err = doc.ToJSON()
	// Nil(t, err)
	// glog.Infof("%v\n", string(bsjson))

	glog.Infoln("DONE")
}

func explore() {
	dbc, err := dbc.NewDBClient(obinary.ClientOptions{})
	Nil(t, err)
	defer dbc.Close()

	err = dbc.OpenDatabase(dbc, "ogonoriTest", orient.DocumentDB, "admin", "admin")
	Nil(t, err)
	defer dbc.CloseDatabase(dbc)

	_, err = db.SQLCommand(nil, "Create class Dalek")
	Nil(t, err)

	// err = db.ReloadSchema(dbc) // TMP => LEFT OFF: do the Dalek example with ogonori in explore
	// Nil(t, err)

	dingo := oschema.NewDocument("Dingo")
	dingo.FieldWithType("foo", "bar", oschema.STRING).
		FieldWithType("salad", 44, oschema.INTEGER)

	cat := oschema.NewDocument("Dalek")
	cat.Field("name", "dalek3").
		FieldWithType("embeddedDingo", dingo, oschema.EMBEDDED)

	// ogl.SetLevel(ogl.DEBUG)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
}

type testrange struct {
	start int64
	end   int64
}

//
// client.go acts as a functional test for the ogonori client
//
func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var xplore = flag.Bool("x", false, "run explore fn")
	var conc = flag.Bool("c", false, "run concurrent client tests")

	flag.Parse()
	if *cpuprofile != "" {
		fmt.Println("Running with profiling")
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *xplore {
		explore()
	} else if *conc {
		testConcurrentClients()
	} else {
		ogonoriTestAgainstOrientDBServer()
	}
}
*/
