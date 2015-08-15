package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
	_ "github.com/quux00/ogonori/osql"
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

// Flags - specify these on the cmd line to change from the defaults
var (
	dbUser, dbPass, dbDocumentName, dbGraphName string
)

var (
	equalsFmt, okFmt, assertFmt, fatalFmt string
)

// Do not edit these
const (
	FetchPlanFollowAllLinks = "*:-1"
)

//
// initialize formatting strings for "assert" methods
//
func init() {
	flag.StringVar(&dbUser, "dbuser", "root", "OrientDB root user name")
	flag.StringVar(&dbPass, "dbpass", "jiffylube", "OrientDB root user pass")
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

//
// Equals compares two values for equality (DeepEquals).
// If they are not equal, an error message is printed
// and the function panics.  Use only in test scenarios.
//
func Equals(exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf(equalsFmt, filepath.Base(file), line, exp, act)
		ogl.SetLevel(ogl.WARN)
		panic("Equals fail")
	}
}

//
// Ok checks whether an error is null. If the error is
// not null, an error message is printed and the function
// panics.  Use only in test scenarios.
//
func Ok(err error) {
	if err != nil {
		fmt.Println(err)
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf(okFmt, filepath.Base(file), line, err.Error())
		ogl.SetLevel(ogl.WARN)
		panic("Ok fail")
	}
}

//
// Assert checks that some boolean is true. If the bool is
// not true, then an error message is printed and the function
// panics.  Use only in test scenarios.
//
func Assert(b bool, msg string) {
	if !b {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf(assertFmt, filepath.Base(file), line, msg)
		ogl.SetLevel(ogl.WARN)
		panic("Assert fail")
	}
}

//
// Fatal prints the error passed in and panics.
// Use only in test scenarios.
//
func Fatal(err error) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf(fatalFmt, filepath.Base(file), line, err.Error())
	ogl.SetLevel(ogl.WARN)
	panic(err)
}

//
// Pause prints msg to the console and waits for
// the user to type some input to continue.
// Use only for debugging.
//
func Pause(msg string) {
	fmt.Print(msg, "[Press Enter to Continue]: ")
	var s string
	_, err := fmt.Scan(&s)
	if err != nil {
		panic(err)
	}
}

func createOgonoriTestDB(dbc *obinary.DBClient, dbUser, dbPass string, fullTest bool) {
	ogl.Printf("%s\n\n", "-------- Create OgonoriTest DB --------")

	err := obinary.ConnectToServer(dbc, dbUser, dbPass)
	Ok(err)

	Assert(dbc.GetSessionID() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	mapDBs, err := obinary.RequestDBList(dbc)
	Ok(err)
	ogl.Debugf("mapDBs: %v\n", mapDBs)
	gratefulTestPath, ok := mapDBs["GratefulDeadConcerts"]
	Assert(ok, "GratefulDeadConcerts not in DB list")
	Assert(strings.HasPrefix(gratefulTestPath, "plocal"), "plocal prefix for db path")

	// first check if ogonoriTest db exists and if so, drop it
	dbexists, err := obinary.DatabaseExists(dbc, dbDocumentName, constants.Persistent)
	Ok(err)

	if dbexists {
		if !fullTest {
			return
		}

		err = obinary.DropDatabase(dbc, dbDocumentName, constants.DocumentDB)
		Ok(err)
	}

	// err = obinary.CreateDatabase(dbc, dbDocumentName, constants.DocumentDbType, constants.Volatile)
	err = obinary.CreateDatabase(dbc, dbDocumentName, constants.DocumentDB, constants.Persistent)
	Ok(err)
	dbexists, err = obinary.DatabaseExists(dbc, dbDocumentName, constants.Persistent)
	Ok(err)
	Assert(dbexists, dbDocumentName+" should now exists after creating it")

	seedInitialData(dbc)

	// bug in OrientDB 2.0.1? :
	//  ERROR: com.orientechnologies.orient.core.exception.ODatabaseException Database 'plocal:/home/midpeter444/apps/orientdb-community-2.0.1/databases/ogonoriTest' is closed}
	// mapDBs, err = obinary.RequestDBList(dbc)
	// if err != nil {
	// 	Fatal(err)
	// }
	// fmt.Printf("%v\n", mapDBs)
	// ogonoriTestPath, ok := mapDBs[dbDocumentName]
	// Assert(ok, dbDocumentName+" not in DB list")
	// Assert(strings.HasPrefix(ogonoriTestPath, "plocal"), "plocal prefix for db path")
}

func seedInitialData(dbc *obinary.DBClient) {
	fmt.Println("OpenDatabase (seed round)")
	err := obinary.OpenDatabase(dbc, dbDocumentName, constants.DocumentDB, "admin", "admin")
	Ok(err)

	defer obinary.CloseDatabase(dbc)

	// seed initial data
	var sqlCmd string
	sqlCmd = "CREATE CLASS Animal"
	_, _, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)

	sqlCmd = "CREATE property Animal.name string"
	_, _, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)

	sqlCmd = "CREATE property Animal.age integer"
	_, _, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)

	sqlCmd = "CREATE CLASS Cat extends Animal"
	_, _, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)

	sqlCmd = "CREATE property Cat.caretaker string"
	_, _, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)

	sqlCmd = `INSERT INTO Cat (name, age, caretaker) VALUES ("Linus", 15, "Michael"), ("Keiko", 10, "Anna")`
	_, _, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
}

func deleteNewRecordsDocDB(dbc *obinary.DBClient) {
	_, _, err := obinary.SQLCommand(dbc, "delete from Cat where name <> 'Linus' AND name <> 'Keiko'")
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
}

func deleteNewClustersDocDB(dbc *obinary.DBClient) {
	// doing DROP CLUSTER via SQL will not return an exception - it just
	// returns "false" as the retval (first return value), so safe to do this
	// even if these clusters don't exist
	for _, clustName := range []string{"CatUSA", "CatAmerica", "bigapple"} {
		_, _, err := obinary.SQLCommand(dbc, "DROP CLUSTER "+clustName)
		Ok(err)
	}
}

func deleteNewRecordsGraphDB(dbc *obinary.DBClient) {
	_, _, _ = obinary.SQLCommand(dbc, "DELETE VERTEX Person")
	_, _, err := obinary.SQLCommand(dbc, "DROP CLASS Person")
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
	_, _, err = obinary.SQLCommand(dbc, "DROP CLASS Friend")
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
}

func cleanUp(dbc *obinary.DBClient, fullTest bool) {
	cleanUpDocDB(dbc, fullTest)
	cleanUpGraphDB(dbc, fullTest)
}

func dropDatabase(dbc *obinary.DBClient, dbname string, dbtype constants.DatabaseType) {
	_ = obinary.CloseDatabase(dbc)
	err := obinary.ConnectToServer(dbc, dbUser, dbPass)
	Ok(err)

	err = obinary.DropDatabase(dbc, dbname, dbtype)
	Ok(err)
	dbexists, err := obinary.DatabaseExists(dbc, dbname, constants.Persistent)
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
	if dbexists {
		ogl.Warnf("ERROR: Deletion of database %s failed\n", dbname)
	}
}

func cleanUpDocDB(dbc *obinary.DBClient, fullTest bool) {
	if fullTest {
		dropDatabase(dbc, dbDocumentName, constants.DocumentDB)

	} else {
		_ = obinary.CloseDatabase(dbc)
		err := obinary.OpenDatabase(dbc, dbDocumentName, constants.DocumentDB, "admin", "admin")
		if err != nil {
			ogl.Warn(err.Error())
			return
		}
		deleteNewRecordsDocDB(dbc)
		deleteNewClustersDocDB(dbc)
		err = obinary.CloseDatabase(dbc)
		if err != nil {
			ogl.Warn(err.Error())
		}
	}
}

func cleanUpGraphDB(dbc *obinary.DBClient, fullTest bool) {
	if fullTest {
		dropDatabase(dbc, dbGraphName, constants.GraphDB)

	} else {
		_ = obinary.CloseDatabase(dbc)
		err := obinary.OpenDatabase(dbc, dbGraphName, constants.GraphDB, "admin", "admin")
		if err != nil {
			ogl.Warn(err.Error())
			return
		}

		deleteNewRecordsGraphDB(dbc)
		err = obinary.CloseDatabase(dbc)
		if err != nil {
			ogl.Warn(err.Error())
		}
	}
}

func graphCommandsSQLAPI(conxStr string) {
	db, err := sql.Open("ogonori", conxStr)
	Ok(err)
	defer db.Close()

	err = db.Ping()
	Ok(err)

	insertSQL := "insert into Person SET firstName='Joe', lastName='Namath'"
	res, err := db.Exec(insertSQL)
	Ok(err)

	nrows, _ := res.RowsAffected()
	ogl.Printf("nrows: %v\n", nrows)
	lastID, _ := res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastID)
	Equals(int64(1), nrows)
	Assert(lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	createVtxSQL := `CREATE VERTEX Person SET firstName = 'Terry', lastName = 'Bradshaw'`
	res, err = db.Exec(createVtxSQL)
	Ok(err)

	nrows, _ = res.RowsAffected()
	ogl.Printf("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastID)
	Equals(int64(1), nrows)
	Assert(lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	sql := `CREATE EDGE Friend FROM
            (SELECT FROM Person where firstName = 'Joe' AND lastName = 'Namath')
            TO
            (SELECT FROM Person where firstName = 'Terry' AND lastName = 'Bradshaw')`
	res, err = db.Exec(sql)
	Ok(err)
	nrows, _ = res.RowsAffected()
	ogl.Printf("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastID)
	Equals(int64(1), nrows)
	Assert(lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	sql = `select from Friend order by @rid desc LIMIT 1`
	rows, err := db.Query(sql)
	rowdocs := make([]*oschema.ODocument, 0, 1)
	for rows.Next() {
		var newdoc oschema.ODocument
		err = rows.Scan(&newdoc)
		rowdocs = append(rowdocs, &newdoc)
	}
	err = rows.Err()
	Ok(err)

	Equals(1, len(rowdocs))
	Equals("Friend", rowdocs[0].Classname)
	friendOutLink := rowdocs[0].GetField("out").Value.(*oschema.OLink)
	Assert(friendOutLink.Record == nil, "should be nil")

	ogl.Printf("friendOutLink: %v\n", friendOutLink)

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
	// Ok(err)

	// Equals(1, len(rowdocs))
	// Equals("Friend", rowdocs[0].Classname)
	// friendOutLink = rowdocs[0].GetField("out").Value.(*oschema.OLink)
	// // Assert(friendOutLink.Record != nil, "should NOT be nil") // FAILS: looks like you cannot put a fetchplain in an SQL query itself?

	// nrows, _ = res.RowsAffected()
	// ogl.Printf("nrows: %v\n", nrows)
	// lastID, _ = res.LastInsertId()
	// ogl.Printf("last insert id: %v\n", lastID)
	// Equals(int64(1), nrows)
	// Assert(lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

}

func databaseSQLAPI(conxStr string) {
	ogl.Printf("\n%s\n\n", "-------- Using database/sql API --------")

	/* ---[ OPEN ]--- */
	db, err := sql.Open("ogonori", conxStr)
	Ok(err)
	defer db.Close()

	err = db.Ping()
	Ok(err)

	/* ---[ DELETE #1 ]--- */
	// should not delete any rows
	delcmd := "delete from Cat where name ='Jared'"
	res, err := db.Exec(delcmd)
	Ok(err)
	nrows, _ := res.RowsAffected()
	ogl.Printf(">> RES num rows affected: %v\n", nrows)
	Equals(int64(0), nrows)

	/* ---[ INSERT #1 ]--- */
	// insert with no params
	insertSQL := "insert into Cat (name, age, caretaker) values('Jared', 11, 'The Subway Guy')"
	ogl.Println(insertSQL, "=> 'Jared', 11, 'The Subway Guy'")
	res, err = db.Exec(insertSQL)
	Ok(err)

	nrows, _ = res.RowsAffected()
	ogl.Printf("nrows: %v\n", nrows)
	lastID, _ := res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastID)
	Equals(int64(1), nrows)
	Assert(lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	/* ---[ INSERT #2 ]--- */
	// insert with no params
	insertSQL = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	ogl.Println(insertSQL, "=> 'Filo', 4, 'Greek'")
	res, err = db.Exec(insertSQL, "Filo", 4, "Greek")
	Ok(err)
	nrows, _ = res.RowsAffected()
	ogl.Printf("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastID)
	Equals(int64(1), nrows)
	Assert(lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	/* ---[ QUERY #1: QueryRow ]--- */
	// it is safe to query properties -> not sure how to return docs yet
	querySQL := "select name, age from Cat where caretaker = 'Greek'"
	row := db.QueryRow(querySQL)

	var retname string
	var retage int64
	err = row.Scan(&retname, &retage)
	Ok(err)
	Equals("Filo", retname)
	Equals(int64(4), retage)

	/* ---[ QUERY #2: Query (multiple rows returned) ]--- */

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
	Ok(err)

	Equals(4, len(names))
	Equals(4, len(ctakers))
	Equals(4, len(ages))

	Equals([]string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals([]string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(int64(4), ages[0])
	Equals(int64(10), ages[1])
	Equals(int64(11), ages[2])
	Equals(int64(15), ages[3])

	/* ---[ QUERY #3: Same Query as above but change property order ]--- */

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
	Ok(err)

	Equals(4, len(names))
	Equals(4, len(ctakers))
	Equals(4, len(ages))

	Equals([]string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals([]string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(int64(4), ages[0])
	Equals(int64(10), ages[1])
	Equals(int64(11), ages[2])
	Equals(int64(15), ages[3])

	/* ---[ QUERY #4: Property query using parameterized SQL ]--- */
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

	Equals(2, len(names))
	Equals("Linus", names[0])
	Equals("Jared", names[1])

	Equals(2, len(ctakers))
	Equals("Michael", ctakers[0])
	Equals("The Subway Guy", ctakers[1])

	Equals(2, len(ages))
	Equals(int64(15), ages[0])
	Equals(int64(11), ages[1])

	/* ---[ DELETE #2 ]--- */
	res, err = db.Exec(delcmd)
	Ok(err)
	nrows, _ = res.RowsAffected()
	ogl.Printf(">> DEL2 RES num rows affected: %v\n", nrows)
	Equals(int64(1), nrows)

	/* ---[ DELETE #3 ]--- */
	res, err = db.Exec(delcmd)
	Ok(err)
	nrows, _ = res.RowsAffected()
	ogl.Printf(">> DEL3 RES num rows affected: %v\n", nrows)
	Equals(int64(0), nrows)

	/* ---[ DELETE #4 ]--- */
	delcmd = "delete from Cat where name <> 'Linus' AND name <> 'Keiko'"
	res, err = db.Exec(delcmd)
	Ok(err)
	nrows, _ = res.RowsAffected()
	ogl.Printf(">> DEL4 RES num rows affected: %v\n", nrows)
	Equals(int64(1), nrows)

	/* ---[ Full ODocument Queries with database/sql ]--- */
	/* ---[ QueryRow ]--- */
	ogl.Println(">>>>>>>>> QueryRow of full ODocument<<<<<<<<<<<")
	querySQL = "select from Cat where name = 'Linus'"

	row = db.QueryRow(querySQL)

	var retdoc oschema.ODocument
	err = row.Scan(&retdoc)
	Ok(err)
	Equals("Cat", retdoc.Classname)
	Equals(3, len(retdoc.FieldNames()))
	Equals("Linus", retdoc.GetField("name").Value)
	Equals(int32(15), retdoc.GetField("age").Value)
	Equals("Michael", retdoc.GetField("caretaker").Value)

	/* ---[ Query (return multiple rows) ]--- */
	querySQL = "select from Cat order by caretaker desc"
	rows, err = db.Query(querySQL)
	rowdocs := make([]*oschema.ODocument, 0, 2)
	for rows.Next() {
		var newdoc oschema.ODocument
		err = rows.Scan(&newdoc)
		rowdocs = append(rowdocs, &newdoc)
	}
	err = rows.Err()
	Ok(err)

	Equals(2, len(rowdocs))
	Equals("Cat", rowdocs[0].Classname)
	Equals("Linus", rowdocs[0].GetField("name").Value)
	Equals("Keiko", rowdocs[1].GetField("name").Value)
	Equals("Anna", rowdocs[1].GetField("caretaker").Value)
}

func databaseSQLPreparedStmtAPI(conxStr string) {
	ogl.Printf("\n%s\n\n", "-------- Using database/sql PreparedStatement API --------")

	db, err := sql.Open("ogonori", conxStr)
	Ok(err)
	defer db.Close()

	querySQL := "select caretaker, name, age from Cat where age >= ? order by age desc"

	stmt, err := db.Prepare(querySQL)
	Ok(err)
	defer stmt.Close()

	names := make([]string, 0, 2)
	ctakers := make([]string, 0, 2)
	ages := make([]int64, 0, 2)

	var (
		rCaretaker, rName string
		rAge              int64
	)

	/* ---[ First use ]--- */
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

	Equals(2, len(names))
	Equals("Linus", names[0])
	Equals("Keiko", names[1])

	Equals(2, len(ctakers))
	Equals("Michael", ctakers[0])
	Equals("Anna", ctakers[1])

	Equals(2, len(ages))
	Equals(int64(15), ages[0])
	Equals(int64(10), ages[1])

	/* ---[ Second use ]--- */
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

	Equals(1, len(names))
	Equals("Linus", names[0])
	Equals(int64(15), ages[0])
	Equals("Michael", ctakers[0])

	/* ---[ Third use ]--- */
	rows, err = stmt.Query("100")

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	if err = rows.Err(); err != nil {
		Fatal(err)
	}

	Equals(0, len(names))
	Equals(0, len(ages))
	Equals(0, len(ctakers))

	stmt.Close()

	/* ---[ Now prepare Command, not query ]--- */
	cmdStmt, err := db.Prepare("INSERT INTO Cat (age, caretaker, name) VALUES(?, ?, ?)")
	Ok(err)
	defer cmdStmt.Close()

	// use once
	result, err := cmdStmt.Exec(1, "Ralph", "Max")
	Ok(err)
	nrows, err := result.RowsAffected()
	Ok(err)
	Equals(1, int(nrows))
	insertID, err := result.LastInsertId()
	Ok(err)
	Assert(int(insertID) >= 0, "insertId was: "+strconv.Itoa(int(insertID)))

	// use again
	result, err = cmdStmt.Exec(2, "Jimmy", "John")
	Ok(err)
	nrows, err = result.RowsAffected()
	Ok(err)
	Equals(1, int(nrows))
	insertID2, err := result.LastInsertId()
	Ok(err)
	Assert(insertID != insertID2, "insertID was: "+strconv.Itoa(int(insertID)))

	row := db.QueryRow("select count(*) from Cat")
	var cnt int64
	err = row.Scan(&cnt)
	Ok(err)
	Equals(4, int(cnt))

	cmdStmt.Close()

	/* ---[ Prepare DELETE command ]--- */
	delStmt, err := db.Prepare("DELETE from Cat where name = ? OR caretaker = ?")
	Ok(err)
	defer delStmt.Close()
	result, err = delStmt.Exec("Max", "Jimmy")
	Ok(err)
	nrows, err = result.RowsAffected()
	Ok(err)
	Equals(2, int(nrows))
	insertID3, err := result.LastInsertId()
	Ok(err)
	Assert(int(insertID3) < 0, "should have negative insertId for a DELETE")

}

func dbClusterCommandsNativeAPI(dbc *obinary.DBClient) {
	ogl.Debugln("\n-------- CLUSTER commands --------\n")

	err := obinary.OpenDatabase(dbc, dbDocumentName, constants.DocumentDB, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	cnt1, err := obinary.FetchClusterCountIncludingDeleted(dbc, "default", "index", "ouser")
	Ok(err)
	Assert(cnt1 > 0, "should be clusters")

	cnt2, err := obinary.FetchClusterCount(dbc, "default", "index", "ouser")
	Ok(err)
	Assert(cnt1 >= cnt2, "counts should match or have more deleted")
	ogl.Debugf("Cluster count: %d\n", cnt2)

	begin, end, err := obinary.FetchClusterDataRange(dbc, "ouser")
	Ok(err)
	ogl.Debugln(">> cluster data range: %d, %d", begin, end)
	Assert(end >= begin, "begin and end of ClusterDataRange")

	ogl.Debugln("\n-------- CLUSTER SQL commands --------\n")

	retval, docs, err := obinary.SQLCommand(dbc, "CREATE CLUSTER CatUSA")
	Ok(err)
	ival, err := strconv.Atoi(retval)
	Ok(err)
	Assert(ival > 5, fmt.Sprintf("Unexpected value of ival: %d", ival))

	retval, docs, err = obinary.SQLCommand(dbc, "ALTER CLUSTER CatUSA Name CatAmerica")
	Ok(err)
	ogl.Printf("ALTER CLUSTER CatUSA Name CatAmerica: retval: %v; docs: %v\n", retval, docs)

	retval, docs, err = obinary.SQLCommand(dbc, "DROP CLUSTER CatUSA")
	Ok(err)
	Equals("false", retval)

	retval, docs, err = obinary.SQLCommand(dbc, "DROP CLUSTER CatAmerica")
	Ok(err)
	Equals("true", retval)
	ogl.Printf("DROP CLUSTER CatAmerica: retval: %v; docs: %v\n", retval, docs)

	ogl.Debugln("\n-------- CLUSTER Direct commands (not SQL) --------\n")
	clusterID, err := obinary.AddCluster(dbc, "bigapple")
	if err != nil {
		Fatal(err)
	}
	Assert(clusterID > 0, "clusterID should be bigger than zero")

	cnt, err := obinary.FetchClusterCount(dbc, "bigapple")
	if err != nil {
		Fatal(err)
	}
	Equals(0, int(cnt)) // should be no records in bigapple cluster

	err = obinary.DropCluster(dbc, "bigapple")
	if err != nil {
		Fatal(err)
	}

	// this time it should return an error
	err = obinary.DropCluster(dbc, "bigapple")
	Assert(err != nil, "DropCluster should return error when cluster doesn't exist")
}

func createOgonoriGraphDb(dbc *obinary.DBClient) {
	ogl.Println("- - - - - - CREATE GRAPHDB - - - - - - -")

	err := obinary.ConnectToServer(dbc, dbUser, dbPass)
	Ok(err)

	Assert(dbc.GetSessionID() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	dbexists, err := obinary.DatabaseExists(dbc, dbGraphName, constants.Persistent)
	Ok(err)
	if dbexists {
		dropDatabase(dbc, dbGraphName, constants.GraphDB)
	}

	err = obinary.CreateDatabase(dbc, dbGraphName, constants.GraphDB, constants.Persistent)
	Ok(err)
	dbexists, err = obinary.DatabaseExists(dbc, dbGraphName, constants.Persistent)
	Ok(err)
	Assert(dbexists, dbGraphName+" should now exists after creating it")
}

func graphCommandsNativeAPI(dbc *obinary.DBClient, fullTest bool) {
	var (
		sql    string
		retval string
		docs   []*oschema.ODocument
		err    error
	)

	createOgonoriGraphDb(dbc)

	ogl.Println("- - - - - - GRAPH COMMANDS - - - - - - -")

	err = obinary.OpenDatabase(dbc, dbGraphName, constants.GraphDB, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	sql = `CREATE Class Person extends V`
	retval, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	numval, err := strconv.Atoi(retval)
	Ok(err)
	Assert(numval > 0, "numval > 0 failed")
	Equals(0, len(docs))

	sql = `CREATE VERTEX Person SET firstName = 'Bob', lastName = 'Wilson'`
	_, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	Equals(1, len(docs))
	Equals(2, len(docs[0].FieldNames()))
	Equals("Wilson", docs[0].GetField("lastName").Value)

	sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	retval, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	Equals("1", retval)
	Equals(0, len(docs))

	sql = `INSERT INTO Person (firstName, lastName, SSN) VALUES ('Abbie', 'Wilson', '555-55-5555'), ('Zeke', 'Rossi', '444-44-4444')`
	_, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	Equals(2, len(docs))
	Equals(3, len(docs[0].FieldNames()))
	Equals("Wilson", docs[0].GetField("lastName").Value)
	abbieRID := docs[0].RID
	zekeRID := docs[1].RID

	sql = `CREATE CLASS Friend extends E`
	_, _, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)

	// sql = `CREATE EDGE Friend FROM ? to ?`
	// _, docs, err = obinary.SQLCommand(dbc, sql, abbieRID.String(), zekeRID.String())
	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, abbieRID.String(), zekeRID.String())
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	obinary.ReloadSchema(dbc)

	var abbieVtx, zekeVtx *oschema.ODocument
	var friendLinkBag *oschema.OLinkBag

	// TODO: this query fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	sql = `SELECT from Person where any() traverse(0,2) (firstName = 'Abbie') ORDER BY firstName`
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(2, len(docs))
	abbieVtx = docs[0]
	zekeVtx = docs[1]
	Equals("Wilson", abbieVtx.GetField("lastName").Value)
	Equals("Rossi", zekeVtx.GetField("lastName").Value)
	friendLinkBag = abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(0, friendLinkBag.GetRemoteSize()) // FIXME: this is probably wrong -> is now 0
	Equals(1, len(friendLinkBag.Links))
	Assert(zekeVtx.RID.ClusterID != friendLinkBag.Links[0].RID.ClusterID, "friendLink should be from friend table")
	Assert(friendLinkBag.Links[0].Record == nil, "Record should not be filled in (no extended fetchPlan)")

	// TODO: this query fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	//       error is: FATAL: client.go:904: github.com/quux00/ogonori/obinary/qrycmd.go:125; cause: ERROR: readResultSet: expected short value of 0 but is -3
	sql = `TRAVERSE * from ` + abbieRID.String()
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(3, len(docs))
	// AbbieVertex -out-> FriendEdge -in-> ZekeVertex, in that order
	abbieVtx = docs[0]
	friendEdge := docs[1]
	zekeVtx = docs[2]
	Equals("Person", abbieVtx.Classname)
	Equals("Friend", friendEdge.Classname)
	Equals("Person", zekeVtx.Classname)
	Equals("555-55-5555", abbieVtx.GetField("SSN").Value)
	linkBagInAbbieVtx := abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(0, linkBagInAbbieVtx.GetRemoteSize())
	Equals(1, len(linkBagInAbbieVtx.Links))
	Assert(linkBagInAbbieVtx.Links[0].Record == nil, "Record should not be filled in (no extended fetchPlan)")
	Equals(linkBagInAbbieVtx.Links[0].RID, friendEdge.RID)
	Equals(2, len(friendEdge.FieldNames()))
	outEdgeLink := friendEdge.GetField("out").Value.(*oschema.OLink)
	Equals(abbieVtx.RID, outEdgeLink.RID)
	inEdgeLink := friendEdge.GetField("in").Value.(*oschema.OLink)
	Equals(zekeVtx.RID, inEdgeLink.RID)
	linkBagInZekeVtx := zekeVtx.GetField("in_Friend").Value.(*oschema.OLinkBag)
	Equals(1, len(linkBagInZekeVtx.Links))
	Equals(friendEdge.RID, linkBagInZekeVtx.Links[0].RID)

	sql = `SELECT from Person where any() traverse(0,2) (firstName = ?)`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks, "Abbie")
	Ok(err)
	Equals(2, len(docs))
	abbieVtx = docs[0]
	zekeVtx = docs[1]
	Equals("Wilson", abbieVtx.GetField("lastName").Value)
	Equals("Rossi", zekeVtx.GetField("lastName").Value)
	friendLinkBag = abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(1, len(friendLinkBag.Links))
	Assert(zekeVtx.RID.ClusterID != friendLinkBag.Links[0].RID.ClusterID, "friendLink should be from friend table")
	// the link in abbie is an EDGE (of Friend class)
	Equals("Friend", friendLinkBag.Links[0].Record.Classname)
	outEdgeLink = friendLinkBag.Links[0].Record.GetField("out").Value.(*oschema.OLink)
	Equals(abbieVtx.RID, outEdgeLink.RID)
	inEdgeLink = friendLinkBag.Links[0].Record.GetField("in").Value.(*oschema.OLink)
	Equals(zekeVtx.RID, inEdgeLink.RID)

	// now add more entries and Friend edges
	// Abbie --Friend--> Zeke
	// Zeke  --Friend--> Jim
	// Jim   --Friend--> Zeke
	// Jim   --Friend--> Abbie
	// Zeke  --Friend--> Paul

	abbieRID = abbieVtx.RID
	zekeRID = zekeVtx.RID

	sql = `INSERT INTO Person (firstName, lastName, SSN) VALUES ('Jim', 'Sorrento', '222-22-2222'), ('Paul', 'Pepper', '333-33-3333')`
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(2, len(docs))
	jimRID := docs[0].RID
	paulRID := docs[1].RID

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, zekeRID.String(), jimRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, jimRID.String(), zekeRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, jimRID.String(), abbieRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, zekeRID.String(), paulRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	// ----+-----+------+---------+--------+-----------+----------+---------
	// #   |@RID |@CLASS|firstName|lastName|SSN        |out_Friend|in_Friend
	// ----+-----+------+---------+--------+-----------+----------+---------
	// 0   |#11:1|Person|Abbie    |Wilson  |555-55-5555|[size=1]  |[size=1]
	// 1   |#11:2|Person|Zeke     |Rossi   |444-44-4444|[size=2]  |[size=2]
	// 2   |#11:3|Person|Jim      |Sorrento|222-22-2222|[size=2]  |[size=1]
	// 3   |#11:4|Person|Paul     |Pepper  |333-33-3333|null      |[size=1]
	// ----+-----+------+---------+--------+-----------+----------+---------

	// [ODocument[Classname: Person; RID: #11:1; Version: 4; fields:
	//   OField[id: -1; name: firstName; datatype: 7; value: Abbie]
	//   OField[id: -1; name: lastName; datatype: 7; value: Wilson]
	//   OField[id: -1; name: SSN; datatype: 7; value: 555-55-5555]
	//   OField[id: -1; name: out_Friend; datatype: 22; value: [<OLink RID: #12:0, Record: <nil>>]]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:3, Record: <nil>>]]]
	//  ODocument[Classname: Person; RID: #11:3; Version: 4; fields:
	//   OField[id: -1; name: out_Friend; datatype: 22; value: [<OLink RID: #12:2, Record: <nil>> <OLink RID: #12:3, Record: <nil>>]]
	//   OField[id: -1; name: lastName; datatype: 7; value: Sorrento]
	//   OField[id: -1; name: SSN; datatype: 7; value: 222-22-2222]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:1, Record: <nil>>]]
	//   OField[id: -1; name: firstName; datatype: 7; value: Jim]]
	//  ODocument[Classname: Person; RID: #11:4; Version: 2; fields:
	//   OField[id: -1; name: firstName; datatype: 7; value: Paul]
	//   OField[id: -1; name: lastName; datatype: 7; value: Pepper]
	//   OField[id: -1; name: SSN; datatype: 7; value: 333-33-3333]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:4, Record: <nil>>]]]
	//  ODocument[Classname: Person; RID: #11:2; Version: 5; fields:
	//   OField[id: -1; name: out_Friend; datatype: 22; value: [<OLink RID: #12:1, Record: <nil>> <OLink RID: #12:4, Record: <nil>>]]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:0, Record: <nil>> <OLink RID: #12:2, Record: <nil>>]]
	//   OField[id: -1; name: firstName; datatype: 7; value: Zeke]
	//   OField[id: -1; name: lastName; datatype: 7; value: Rossi]
	//   OField[id: -1; name: SSN; datatype: 7; value: 444-44-4444]]

	sql = `SELECT from Person where any() traverse(0,5) (firstName = 'Jim') ORDER BY firstName`
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(4, len(docs))
	Equals("Abbie", docs[0].GetField("firstName").Value)
	Equals("Jim", docs[1].GetField("firstName").Value)
	Equals("Paul", docs[2].GetField("firstName").Value)
	Equals("Zeke", docs[3].GetField("firstName").Value)

	// Abbie should have one out_Friend and one in_Friend
	Equals(1, len(docs[0].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(1, len(docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Jim has two out_Friend and one in_Friend links
	Equals(1, len(docs[1].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(2, len(docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Paul has one in_Friend and zero out_Friend links
	Equals(1, len(docs[2].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Assert(docs[2].GetField("out_Friend") == nil, "Paul should have no out_Field edges")

	// Zeke has two in_Friend and two out_Friend edges
	Equals(2, len(docs[3].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(2, len(docs[3].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Paul's in_Friend should be Zeke's outFriend link to Paul
	// the links are edges not vertexes, so have to check for a match on edge RIDs
	paulsInFriendEdge := docs[2].GetField("in_Friend").Value.(*oschema.OLinkBag).Links[0]

	zekesOutFriendEdges := docs[3].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	sort.Sort(ByRID(zekesOutFriendEdges))
	// I know that zeke -> paul edge was the last one created, so it will be the second
	// in Zeke's LinkBag list
	Equals(paulsInFriendEdge.RID, zekesOutFriendEdges[1].RID)

	// ------

	// should return two links Abbie -> Zeke and Jim -> Abbie
	sql = `SELECT both('Friend') from ` + abbieRID.String()
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(1, len(docs))
	abbieBothLinks := docs[0].GetField("both").Value.([]*oschema.OLink)
	Equals(2, len(abbieBothLinks))
	sort.Sort(ByRID(abbieBothLinks))
	Equals(zekeRID, abbieBothLinks[0].RID)
	Equals(jimRID, abbieBothLinks[1].RID)

	sql = fmt.Sprintf(`SELECT dijkstra(%s, %s, 'weight') `, abbieRID.String(), paulRID.String())
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	// return value is a single Document with single field called 'dijkstra' with three links
	// from abbie to paul, namely: abbie -> zeke -> paul
	Equals(1, len(docs))
	pathLinks := docs[0].GetField("dijkstra").Value.([]*oschema.OLink)
	Equals(3, len(pathLinks))
	Equals(abbieRID, pathLinks[0].RID)
	Equals(zekeRID, pathLinks[1].RID)
	Equals(paulRID, pathLinks[2].RID)

	// sql = `DELETE VERTEX #24:434` // need to get the @rid of Bob
	// sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	// sql = `DELETE VERTEX Person WHERE in.@Class = 'MembershipExpired'`

	addManyLinksToFlipFriendLinkBagToExternalTreeBased(dbc, abbieRID)
	doCircularLinkExample(dbc)
}

func doCircularLinkExample(dbc *obinary.DBClient) {
	_, docs, err := obinary.SQLCommand(dbc, `create vertex Person content {"firstName":"AAA", "lastName":"BBB", "SSN":"111-11-1111"}`)
	Ok(err)
	Equals(1, len(docs))
	aaaDoc := docs[0]

	_, docs, err = obinary.SQLCommand(dbc, `create vertex Person content {"firstName":"YYY", "lastName":"ZZZ"}`)
	Ok(err)
	yyyDoc := docs[0]

	sql := fmt.Sprintf(`create edge Friend from %s to %s`, aaaDoc.RID.String(), yyyDoc.RID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	aaa2yyyFriendDoc := docs[0]

	sql = fmt.Sprintf(`create edge Friend from %s to %s`, yyyDoc.RID.String(), aaaDoc.RID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	yyy2aaaFriendDoc := docs[0]

	// [ODocument<Classname: Person; RID: #11:93; Version: 3; fields:
	//   OField<id: -1; name: firstName; datatype: 7; value: AAA>
	//   OField<id: -1; name: lastName; datatype: 7; value: BBB>
	//   OField<id: -1; name: SSN; datatype: 7; value: 111-11-1111>
	//   OField<id: -1; name: out_Friend; datatype: 22; value: &{[<OLink RID: #12:93, Record: ODocument<Classname: Friend; RID:
	//  #12:93; Version: 3; fields: [...]>>] {0 <nil>}}>
	//   OField<id: -1; name: in_Friend; datatype: 22; value: &{[<OLink RID: #12:94, Record: ODocument<Classname: Friend; RID:
	//  #12:94; Version: 3; fields: [...]>>] {0 <nil>}}>>

	//  ODocument<Classname: Person; RID: #11:94; Version: 3; fields:
	//   OField<id: -1; name: lastName; datatype: 7; value: ZZZ>
	//   OField<id: -1; name: in_Friend; datatype: 22; value: &{[<OLink RID: #12:93, Record: ODocument<Classname: Friend; RID:
	//  #12:93; Version: 3; fields: [...]>>] {0 <nil>}}>
	//   OField<id: -1; name: out_Friend; datatype: 22; value: &{[<OLink RID: #12:94, Record: ODocument<Classname: Friend; RID:
	//  #12:94; Version: 3; fields: [...]>>] {0 <nil>}}>
	//   OField<id: -1; name: firstName; datatype: 7; value: YYY>>
	// ]

	// [ODocument<Classname: Friend; RID: #12:93; Version: 3; fields:
	//   OField<id: -1; name: out; datatype: 13; value: <OLink RID: #11:93, Record: ODocument<Classname: Person; RID: #11:93;
	// Version: 3; fields: [...]>>>
	//   OField<id: -1; name: in; datatype: 13; value: <OLink RID: #11:94, Record: ODocument<Classname: Person; RID: #11:94;
	// Version: 3; fields: [...]>>>>
	//  ODocument<Classname: Friend; RID: #12:94; Version: 3; fields:
	//   OField<id: -1; name: out; datatype: 13; value: <OLink RID: #11:94, Record: ODocument<Classname: Person; RID: #11:94;
	// Version: 3; fields: [...]>>>
	//   OField<id: -1; name: in; datatype: 13; value: <OLink RID: #11:93, Record: ODocument<Classname: Person; RID: #11:93;
	// Version: 3; fields: [...]>>>>

	docs, err = obinary.SQLQuery(dbc, "SELECT FROM Person where firstName='AAA' OR firstName='YYY' SKIP 0 LIMIT 100 ORDER BY firstName", "")
	Ok(err)
	Equals(2, len(docs))
	Equals(aaaDoc.RID, docs[0].RID)
	aaaOutFriendLinks := docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(aaaOutFriendLinks))
	Equals(aaaOutFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Assert(aaaOutFriendLinks[0].Record == nil, "should not be filled in")

	yyyOutFriendLinks := docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(yyyOutFriendLinks))
	Equals(yyyOutFriendLinks[0].RID, yyy2aaaFriendDoc.RID)
	Assert(yyyOutFriendLinks[0].Record == nil, "should not be filled in")

	// ------

	docs, err = obinary.SQLQuery(dbc, "SELECT FROM Person where firstName='AAA' OR firstName='YYY' ORDER BY firstName", FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	Equals(aaaDoc.RID, docs[0].RID)
	aaaOutFriendLinks = docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(aaaOutFriendLinks))
	Equals(aaaOutFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Assert(aaaOutFriendLinks[0].Record != nil, "should not be filled in")

	Equals("YYY", docs[1].GetField("firstName").Value)
	yyyOutFriendLinks = docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(yyyOutFriendLinks))
	Equals(yyyOutFriendLinks[0].RID, yyy2aaaFriendDoc.RID)
	Assert(yyyOutFriendLinks[0].Record != nil, "should not be filled in")

	yyyInFriendLinks := docs[1].GetField("in_Friend").Value.(*oschema.OLinkBag).Links
	Equals(yyyInFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Equals(yyyInFriendLinks[0].Record.RID, aaa2yyyFriendDoc.RID)
	Equals("YYY", yyyInFriendLinks[0].Record.GetField("in").Value.(*oschema.OLink).Record.GetField("firstName").Value)

	// ------

	sql = fmt.Sprintf("select from friend where @rid=%s or @rid=%s ORDER BY @rid",
		aaa2yyyFriendDoc.RID, yyy2aaaFriendDoc.RID)
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	Equals(aaa2yyyFriendDoc.RID, docs[0].RID)
	outLinkToAAA := docs[0].GetField("out").Value.(*oschema.OLink)
	Equals(outLinkToAAA.RID, aaaDoc.RID)
	Equals("AAA", outLinkToAAA.Record.GetField("firstName").Value)

	inLinkFromYYY := docs[0].GetField("in").Value.(*oschema.OLink)
	Equals(inLinkFromYYY.RID, yyyDoc.RID)
	Equals("YYY", inLinkFromYYY.Record.GetField("firstName").Value)

	outLinkToYYY := docs[1].GetField("out").Value.(*oschema.OLink)
	Equals(outLinkToYYY.RID, yyyDoc.RID)
	Equals("YYY", outLinkToYYY.Record.GetField("firstName").Value)

	inLinkFromAAA := docs[1].GetField("in").Value.(*oschema.OLink)
	Equals(inLinkFromAAA.RID, aaaDoc.RID)
	Equals("AAA", inLinkFromAAA.Record.GetField("firstName").Value)
}

func addManyLinksToFlipFriendLinkBagToExternalTreeBased(dbc *obinary.DBClient, abbieRID oschema.ORID) {
	var (
		sql string
		err error
	)

	nAbbieOutFriends := 88
	for i := 0; i < nAbbieOutFriends; i++ {
		sql = fmt.Sprintf(`INSERT INTO Person (firstName, lastName) VALUES ('Matt%d', 'Black%d')`, i, i)
		_, docs, err := obinary.SQLCommand(dbc, sql)
		Assert(err == nil, fmt.Sprintf("Failure on Person insert #%d: %v", i, err))
		Equals(1, len(docs))

		sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, abbieRID.String(), docs[0].RID.String())
		_, docs, err = obinary.SQLCommand(dbc, sql)
		Ok(err)
	}

	// TODO: try the below query with FetchPlanFollowAllLinks -> are all the LinkBag docs returned ??
	sql = `SELECT from Person where any() traverse(0,2) (firstName = 'Abbie') ORDER BY firstName`
	// _, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	docs, err := obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(91, len(docs))
	// Abbie is the first entry and for in_Friend she has an embedded LinkBag,
	// buf for out_Fridn she has a tree-based remote LinkBag, not yet filled in
	abbieVtx := docs[0]
	Equals("Wilson", abbieVtx.GetField("lastName").Value)
	abbieInFriendLinkBag := abbieVtx.GetField("in_Friend").Value.(*oschema.OLinkBag)
	Equals(1, len(abbieInFriendLinkBag.Links))
	Equals(false, abbieInFriendLinkBag.IsRemote())
	Assert(abbieInFriendLinkBag.GetRemoteSize() <= 0, "GetRemoteSize should not be set to positive val")

	abbieOutFriendLinkBag := abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Assert(abbieOutFriendLinkBag.Links == nil, "out_Friends links should not be present")
	Equals(true, abbieOutFriendLinkBag.IsRemote())
	Assert(abbieInFriendLinkBag.GetRemoteSize() <= 0, "GetRemoteSize should not be set to positive val")

	sz, err := obinary.FetchSizeOfRemoteLinkBag(dbc, abbieOutFriendLinkBag)
	Ok(err)
	Equals(nAbbieOutFriends+1, sz)

	// TODO: what happens if you set inclusive to false?
	inclusive := true
	err = obinary.FetchEntriesOfRemoteLinkBag(dbc, abbieOutFriendLinkBag, inclusive)
	Ok(err)
	Equals(89, len(abbieOutFriendLinkBag.Links))

	// choose arbitrary Link from the LinkBag and fill in its Record doc
	link7 := abbieOutFriendLinkBag.Links[7]
	Assert(link7.RID.ClusterID > 1, "RID should be filled in")
	Assert(link7.Record == nil, "Link Record should NOT be filled in yet")

	// choose arbitrary Link from the LinkBag and fill in its Record doc
	link13 := abbieOutFriendLinkBag.Links[13]
	Assert(link13.RID.ClusterID > 1, "RID should be filled in")
	Assert(link13.Record == nil, "Link Record should NOT be filled in yet")

	fetchPlan := ""
	docs, err = obinary.FetchRecordByRID(dbc, link7.RID, fetchPlan)
	Equals(1, len(docs))
	link7.Record = docs[0]
	Assert(abbieOutFriendLinkBag.Links[7].Record != nil, "Link Record should be filled in")

	err = obinary.ResolveLinks(dbc, abbieOutFriendLinkBag.Links) // TODO: maybe include a fetchplan here?
	Ok(err)
	for i, outFriendLink := range abbieOutFriendLinkBag.Links {
		Assert(outFriendLink.Record != nil, fmt.Sprintf("Link Record not filled in for rec %d", i))
	}
}

func dbCommandsNativeAPI(dbc *obinary.DBClient, fullTest bool) {
	ogl.Printf("\n%s\n\n", "-------- database-level commands --------")

	var sql string
	var retval string

	err := obinary.OpenDatabase(dbc, dbDocumentName, constants.DocumentDB, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	/* ---[ query from the ogonoriTest database ]--- */

	sql = "select from Cat where name = 'Linus'"
	fetchPlan := ""
	docs, err := obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)

	linusDocRID := docs[0].RID

	Assert(linusDocRID.ClusterID != oschema.ClusterIDInvalid, "linusDocRID should not be nil")
	Assert(docs[0].Version > 0, fmt.Sprintf("Version is: %d", docs[0].Version))
	Equals(3, len(docs[0].FieldNames()))
	Equals("Cat", docs[0].Classname)

	nameField := docs[0].GetField("name")
	Assert(nameField != nil, "should be a 'name' field")

	ageField := docs[0].GetField("age")
	Assert(ageField != nil, "should be a 'age' field")

	caretakerField := docs[0].GetField("caretaker")
	Assert(caretakerField != nil, "should be a 'caretaker' field")

	Assert(nameField.ID != caretakerField.ID, "IDs should not match")
	Equals(oschema.STRING, nameField.Type)
	Equals(oschema.STRING, caretakerField.Type)
	Equals(oschema.INTEGER, ageField.Type)
	Equals("Linus", nameField.Value)
	Equals(int32(15), ageField.Value)
	Equals("Michael", caretakerField.Value)

	/* ---[ get by RID ]--- */
	docs, err = obinary.FetchRecordByRID(dbc, linusDocRID, "")
	Ok(err)
	Equals(1, len(docs))
	docByRID := docs[0]
	Equals(linusDocRID, docByRID.RID)
	Assert(docByRID.Version > 0, fmt.Sprintf("Version is: %d", docByRID.Version))
	Equals(3, len(docByRID.FieldNames()))
	Equals("Cat", docByRID.Classname)

	nameField = docByRID.GetField("name")
	Assert(nameField != nil, "should be a 'name' field")

	ageField = docByRID.GetField("age")
	Assert(ageField != nil, "should be a 'age' field")

	caretakerField = docByRID.GetField("caretaker")
	Assert(caretakerField != nil, "should be a 'caretaker' field")

	Assert(nameField.ID != caretakerField.ID, "IDs should not match")
	Equals(oschema.STRING, nameField.Type)
	Equals(oschema.INTEGER, ageField.Type)
	Equals(oschema.STRING, caretakerField.Type)
	Equals("Linus", nameField.Value)
	Equals(int32(15), ageField.Value)
	Equals("Michael", caretakerField.Value)

	ogl.Printf("docs returned by RID: %v\n", *(docs[0]))

	/* ---[ cluster data range ]--- */
	begin, end, err := obinary.FetchClusterDataRange(dbc, "cat")
	Ok(err)
	ogl.Printf("begin = %v; end = %v\n", begin, end)

	sql = "insert into Cat (name, age, caretaker) values(\"Zed\", 3, \"Shaw\")"
	nrows, docs, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	/* ---[ query after inserting record(s) ]--- */

	sql = "select * from Cat order by name asc"
	ogl.Println("Issuing command query: " + sql)
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)
	Equals(3, len(docs))
	Equals(3, len(docs[0].FieldNames()))
	Equals("Cat", docs[0].Classname)
	Equals(3, len(docs[1].FieldNames()))
	Equals("Cat", docs[1].Classname)
	Equals(3, len(docs[2].FieldNames()))
	Equals("Cat", docs[2].Classname)

	keiko := docs[0]
	Equals("Keiko", keiko.GetField("name").Value)
	Equals(int32(10), keiko.GetField("age").Value)
	Equals("Anna", keiko.GetField("caretaker").Value)
	Equals(oschema.STRING, keiko.GetField("caretaker").Type)
	Assert(keiko.Version > int32(0), "Version should be greater than zero")
	Assert(keiko.RID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	linus := docs[1]
	Equals("Linus", linus.GetField("name").Value)
	Equals(int32(15), linus.GetField("age").Value)
	Equals("Michael", linus.GetField("caretaker").Value)

	zed := docs[2]
	Equals("Zed", zed.GetField("name").Value)
	Equals(int32(3), zed.GetField("age").Value)
	Equals("Shaw", zed.GetField("caretaker").Value)
	Equals(oschema.STRING, zed.GetField("caretaker").Type)
	Equals(oschema.INTEGER, zed.GetField("age").Type)
	Assert(zed.Version > int32(0), "Version should be greater than zero")
	Assert(zed.RID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	sql = "select name, caretaker from Cat order by caretaker"
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)
	Equals(3, len(docs))
	Equals(2, len(docs[0].FieldNames()))
	Equals("", docs[0].Classname) // property queries do not come back with Classname set
	Equals(2, len(docs[1].FieldNames()))
	Equals("", docs[1].Classname)
	Equals(2, len(docs[2].FieldNames()))

	Equals("Anna", docs[0].GetField("caretaker").Value)
	Equals("Michael", docs[1].GetField("caretaker").Value)
	Equals("Shaw", docs[2].GetField("caretaker").Value)

	Equals("Keiko", docs[0].GetField("name").Value)
	Equals("Linus", docs[1].GetField("name").Value)
	Equals("Zed", docs[2].GetField("name").Value)

	Equals("name", docs[0].GetField("name").Name)

	/* ---[ delete newly added record(s) ]--- */
	ogl.Println("Deleting (sync) record #" + zed.RID.String())
	err = obinary.DeleteRecordByRID(dbc, zed.RID.String(), zed.Version)
	Ok(err)

	// ogl.Println("Deleting (Async) record #11:4")
	// err = obinary.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	Fatal(err)
	// }

	sql = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	nrows, docs, err = obinary.SQLCommand(dbc, sql, "June", "8", "Cleaver") // TODO: check if numeric types are passed as strings in the Java client
	Ok(err)

	sql = "select name, age from Cat where caretaker = ?"
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan, "Cleaver")
	Ok(err)
	Equals(1, len(docs))
	Equals(2, len(docs[0].FieldNames()))
	Equals("", docs[0].Classname) // property queries do not come back with Classname set
	Equals("June", docs[0].GetField("name").Value)
	Equals(int32(8), docs[0].GetField("age").Value)

	sql = "select caretaker, name, age from Cat where age > ? order by age desc"
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan, "9")
	Ok(err)
	Equals(2, len(docs))
	Equals(3, len(docs[0].FieldNames()))
	Equals("", docs[0].Classname) // property queries do not come back with Classname set
	Equals("Linus", docs[0].GetField("name").Value)
	Equals(int32(15), docs[0].GetField("age").Value)
	Equals("Keiko", docs[1].GetField("name").Value)
	Equals(int32(10), docs[1].GetField("age").Value)
	Equals("Anna", docs[1].GetField("caretaker").Value)

	sql = "delete from Cat where name ='June'" // TODO: can we use a param here too ?
	ogl.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	ogl.Println("+++++++++ END: SQL COMMAND w/ PARAMS ++++++++++++===")

	ogl.Println("+++++++++ START: Basic DDL ++++++++++++===")

	sql = "DROP CLASS Patient"
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))
	if retval != "" {
		Equals("true", retval)
	}

	// ------

	sql = "CREATE CLASS Patient"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		sql = "DROP CLASS Patient"
		_, _, err = obinary.SQLCommand(dbc, sql)
		if err != nil {
			ogl.Warnf("WARN: clean up error: %v\n", err)
			return
		}

		// TRUNCATE after drop should return an OServerException type
		sql = "TRUNCATE CLASS Patient"
		retval, docs, err = obinary.SQLCommand(dbc, sql)
		Assert(err != nil, "Error from TRUNCATE should not be null")
		ogl.Debugln(oerror.GetFullTrace(err))

		err = oerror.ExtractCause(err)
		switch err.(type) {
		case oerror.OServerException:
			ogl.Debugln("type == oerror.OServerException")
		default:
			Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
		}
	}()

	Equals(0, len(docs))
	ncls, err := strconv.ParseInt(retval, 10, 64)
	Ok(err)
	Assert(ncls > 10, "classnum should be greater than 10 but was: "+retval)

	// ------

	sql = "Create property Patient.name string"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", nrows)
	ogl.Debugf("docs: %v\n", docs)

	sql = "alter property Patient.name min 3"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "Create property Patient.married boolean"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	obinary.ReloadSchema(dbc)
	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true')"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "TRUNCATE CLASS Patient"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true'), ('Martha', 'false')"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "SELECT count(*) from Patient"
	ogl.Debugln(sql)
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(1, len(docs))
	fldCount := docs[0].GetField("count")
	Equals(int64(2), fldCount.Value)

	sql = "CREATE PROPERTY Patient.gender STRING"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "ALTER PROPERTY Patient.gender REGEXP [M|F]"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married, gender) VALUES ('Larry', 'true', 'M'), ('Shirley', 'false', 'F')"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: %v\n", retval)
	ogl.Debugf("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married, gender) VALUES ('Lt. Dan', 'true', 'T'), ('Sally', 'false', 'F')"
	ogl.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Assert(err != nil, "should be error - T is not an allowed gender")
	err = oerror.ExtractCause(err)
	switch err.(type) {
	case oerror.OServerException:
		ogl.Debugln("type == oerror.OServerException")
	default:
		Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
	}

	sql = "SELECT FROM Patient ORDER BY @rid desc"
	ogl.Debugln(sql)
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(4, len(docs))
	Equals("Shirley", docs[0].GetField("name").Value)

	sql = "ALTER PROPERTY Patient.gender NAME sex"
	ogl.Debugln(sql)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))

	err = obinary.ReloadSchema(dbc)
	Ok(err)

	sql = "DROP PROPERTY Patient.sex"
	ogl.Debugln(sql)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))

	sql = "select from Patient order by RID"
	ogl.Debugln(sql)
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(4, len(docs))
	Equals(2, len(docs[0].Fields)) // has name and married
	Equals("Hank", docs[0].Fields["name"].Value)

	Equals(4, len(docs[3].Fields)) // has name, married, sex and for some reason still has `gender`
	Equals("Shirley", docs[3].Fields["name"].Value)
	Equals("F", docs[3].Fields["gender"].Value)

	sql = "TRUNCATE CLASS Patient"
	ogl.Println(sql)
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	/* ---[ Attempt to create, insert and read back EMBEDDEDLIST types ]--- */

	sql = "CREATE PROPERTY Patient.tags EMBEDDEDLIST STRING"
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	numval, err := strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	sql = `insert into Patient (name, married, tags) values ("George", "false", ["diabetic", "osteoarthritis"])`
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	Equals(3, len(docs[0].FieldNames()))
	ogl.Debugf("retval: %v\n", retval)

	sql = `SELECT from Patient where name = 'George'`
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	ogl.Debugf("docs: %v\n", docs)
	Equals(1, len(docs))
	Equals(3, len(docs[0].FieldNames()))
	embListTagsField := docs[0].GetField("tags")

	embListTags := embListTagsField.Value.([]interface{})
	Equals(2, len(embListTags))
	Equals("diabetic", embListTags[0].(string))
	Equals("osteoarthritis", embListTags[1].(string))

	/* ---[ try JSON content insertion notation ]--- */

	sql = `insert into Patient content {"name": "Freddy", "married":false}`
	ogl.Debugln(sql)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	Equals("Freddy", docs[0].GetField("name").Value)
	Equals(false, docs[0].GetField("married").Value)

	/* ---[ Try LINKs ! ]--- */

	sql = `select from Cat WHERE name = 'Linus' OR name='Keiko' ORDER BY @rid`
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Equals(2, len(docs))
	linusRID := docs[0].RID
	keikoRID := docs[1].RID

	sql = `CREATE PROPERTY Cat.buddy LINK`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "buddy")

	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	sql = `insert into Cat SET name='Tilde', age=8, caretaker='Earl', buddy=(SELECT FROM Cat WHERE name = 'Linus')`
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	ogl.Debugf("retval: >>%v<<\n", retval)
	ogl.Debugf("docs: >>%v<<\n", docs)
	Equals(1, len(docs))
	Equals("Tilde", docs[0].GetField("name").Value)
	Equals(8, int(docs[0].GetField("age").Value.(int32)))
	Equals(linusRID, docs[0].GetField("buddy").Value.(*oschema.OLink).RID)

	tildeRID := docs[0].RID

	/* ---[ Test EMBEDDED ]--- */

	sql = `CREATE PROPERTY Cat.embeddedCat EMBEDDED`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "embeddedCat")

	emb := `{"name": "Spotty", "age": 2, emb: {"@type": "d", "@class":"Cat", "name": "yowler", "age":13}}`
	retval, docs, err = obinary.SQLCommand(dbc, "insert into Cat content "+emb)
	Ok(err)

	Equals(1, len(docs))
	Equals("Spotty", docs[0].GetField("name").Value)
	Equals(2, int(docs[0].GetField("age").Value.(int32)))
	Equals(oschema.EMBEDDED, docs[0].GetField("emb").Type)

	embCat := docs[0].GetField("emb").Value.(*oschema.ODocument)
	Equals("Cat", embCat.Classname)
	Assert(embCat.Version < 0, "Version should be unset")
	Assert(embCat.RID.ClusterID < 0, "RID.ClusterID should be unset")
	Assert(embCat.RID.ClusterPos < 0, "RID.ClusterPos should be unset")
	Equals("yowler", embCat.GetField("name").Value.(string))
	Equals(int(13), toInt(embCat.GetField("age").Value))

	_, _, err = obinary.SQLCommand(dbc, "delete from Cat where name = 'Spotty'")
	Ok(err)

	/* ---[ Test LINKLIST ]--- */

	sql = `CREATE PROPERTY Cat.buddies LINKLIST`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "buddies")
	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	sql = `insert into Cat SET name='Felix', age=6, caretaker='Ed', buddies=(SELECT FROM Cat WHERE name = 'Linus' OR name='Keiko')`
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals("", retval)
	Equals(1, len(docs))
	Equals("Felix", docs[0].GetField("name").Value)
	Equals(6, int(docs[0].GetField("age").Value.(int32)))
	buddies := docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(buddies))
	Equals(2, len(buddies))
	Equals(linusRID, buddies[0].RID)
	Equals(keikoRID, buddies[1].RID)

	felixRID := docs[0].RID

	/* ---[ Try LINKMAP ]--- */
	sql = `CREATE PROPERTY Cat.notes LINKMAP`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "notes")

	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	sql = fmt.Sprintf(`INSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": %s, '30': %s}`,
		linusRID, keikoRID)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	Equals(4, len(docs[0].FieldNames()))
	Equals("Anna", docs[0].GetField("caretaker").Value)
	Equals(linusRID, docs[0].GetField("notes").Value.(map[string]*oschema.OLink)["bff"].RID)
	Equals(keikoRID, docs[0].GetField("notes").Value.(map[string]*oschema.OLink)["30"].RID)

	charlieRID := docs[0].RID

	// query with a fetchPlan that does NOT follow all the links
	ogl.SetLevel(ogl.NORMAL)
	fetchPlan = ""
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL`
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)
	Equals(1, len(docs))
	doc := docs[0]
	Equals("Charlie", doc.GetField("name").Value)
	notesField := doc.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(2, len(notesField))

	bffNote := notesField["bff"]
	Assert(bffNote.RID.ClusterID != -1, "RID should be filled in")
	Assert(bffNote.Record == nil, "RID should be nil")

	thirtyNote := notesField["30"]
	Assert(thirtyNote.RID.ClusterID != -1, "RID should be filled in")
	Assert(thirtyNote.Record == nil, "RID should be nil")

	// query with a fetchPlan that does follow all the links

	fetchPlan = "*:-1"
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL`
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)
	doc = docs[0]
	Equals("Charlie", doc.GetField("name").Value)
	notesField = doc.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(2, len(notesField))

	bffNote = notesField["bff"]
	Assert(bffNote.RID.ClusterID != -1, "RID should be filled in")
	Equals("Linus", bffNote.Record.GetField("name").Value)

	thirtyNote = notesField["30"]
	Assert(thirtyNote.RID.ClusterID != -1, "RID should be filled in")
	Equals("Keiko", thirtyNote.Record.GetField("name").Value)

	/* ---[ Try LINKSET ]--- */

	sql = `CREATE PROPERTY Cat.buddySet LINKSET`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "buddySet")

	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	obinary.ReloadSchema(dbc) // good thing to do after modifying the schema

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

	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals("", retval)
	Equals(1, len(docs))
	Equals("Germaine", docs[0].GetField("name").Value)
	Equals(2, int(docs[0].GetField("age").Value.(int32)))

	germaineRID := docs[0].RID

	buddyList := docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(buddyList))
	Equals(2, len(buddies))
	Equals(linusRID, buddyList[0].RID)
	Equals(keikoRID, buddyList[1].RID)

	buddySet := docs[0].GetField("buddySet").Value.([]*oschema.OLink)
	sort.Sort(ByRID(buddySet))
	Equals(2, len(buddySet))
	Equals(linusRID, buddySet[0].RID)
	Equals(felixRID, buddySet[1].RID)

	notesMap := docs[0].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(2, len(buddies))
	Equals(keikoRID, notesMap["bff"].RID)
	Equals(linusRID, notesMap["30"].RID)

	// now query with fetchPlan that retrieves all links
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL ORDER BY name`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	Equals("Charlie", docs[0].GetField("name").Value)
	Equals("Germaine", docs[1].GetField("name").Value)
	Equals("Minnie", docs[1].GetField("caretaker").Value)

	charlieNotesField := docs[0].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(2, len(charlieNotesField))

	bffNote = charlieNotesField["bff"]
	Equals("Linus", bffNote.Record.GetField("name").Value)

	thirtyNote = charlieNotesField["30"]
	Equals("Keiko", thirtyNote.Record.GetField("name").Value)

	// test Germaine's notes (LINKMAP)
	germaineNotesField := docs[1].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(2, len(germaineNotesField))

	bffNote = germaineNotesField["bff"]
	Equals("Keiko", bffNote.Record.GetField("name").Value)

	thirtyNote = germaineNotesField["30"]
	Equals("Linus", thirtyNote.Record.GetField("name").Value)

	// test Germaine's buddySet (LINKSET)
	germaineBuddySet := docs[1].GetField("buddySet").Value.([]*oschema.OLink)
	sort.Sort(ByRID(germaineBuddySet))
	Equals("Linus", germaineBuddySet[0].Record.GetField("name").Value)
	Equals("Felix", germaineBuddySet[1].Record.GetField("name").Value)
	Assert(germaineBuddySet[1].RID.ClusterID != -1, "RID should be filled in")

	// Felix Document has references, so those should also be filled in
	felixDoc := germaineBuddySet[1].Record
	felixBuddiesList := felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(felixBuddiesList))
	Equals(2, len(felixBuddiesList))
	Assert(felixBuddiesList[0].Record != nil, "Felix links should be filled in")
	Equals("Linus", felixBuddiesList[0].Record.GetField("name").Value)

	// test Germaine's buddies (LINKLIST)
	germaineBuddyList := docs[1].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(germaineBuddyList))
	Equals("Linus", germaineBuddyList[0].Record.GetField("name").Value)
	Equals("Keiko", germaineBuddyList[1].Record.GetField("name").Value)
	Assert(germaineBuddyList[0].RID.ClusterID != -1, "RID should be filled in")

	// now make a circular reference -> give Linus to Germaine as buddy
	sql = `UPDATE Cat SET buddy = ` + germaineRID.String() + ` where name = 'Linus'`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals("1", retval)
	Equals(0, len(docs))

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

	/* ---[ queries with extended fetchPlan (simple case) ]--- */
	sql = `select * from Cat where name = 'Tilde'`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	Ok(err)
	Equals(1, len(docs))
	doc = docs[0]
	Equals("Tilde", doc.GetField("name").Value)
	tildeBuddyField := doc.GetField("buddy").Value.(*oschema.OLink)
	Equals(linusRID, tildeBuddyField.RID)
	Equals("Linus", tildeBuddyField.Record.GetField("name").Value)

	// now pull in both records with non-null buddy links
	//     Tilde and Linus are the primary docs
	//     Tilde.buddy -> Linus
	//     Linus.buddy -> Felix
	//     Felix.buddies -> Linus and Keiko
	//     so Tilde, Linus, Felix and Keiko should all be pulled in, but only
	//     Tilde and Linus returned directly from the query
	sql = `SELECT FROM Cat where buddy is not null ORDER BY name`
	fetchPlan = "*:-1"
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)
	Equals(2, len(docs))
	Equals("Linus", docs[0].GetField("name").Value)
	Equals("Tilde", docs[1].GetField("name").Value)

	linusBuddy := docs[0].GetField("buddy").Value.(*oschema.OLink)
	Assert(linusBuddy.Record != nil, "Record should be filled in")
	Equals("Germaine", linusBuddy.Record.GetField("name").Value)

	tildeBuddy := docs[1].GetField("buddy").Value.(*oschema.OLink)
	Assert(tildeBuddy.Record != nil, "Record should be filled in")
	Equals("Linus", tildeBuddy.Record.GetField("name").Value)

	// now check that Felix buddies were pulled in too
	felixDoc = linusBuddy.Record
	felixBuddiesList = felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(felixBuddiesList))
	Equals(2, len(felixBuddiesList))
	Equals("Linus", felixBuddiesList[0].Record.GetField("name").Value)
	Equals("Keiko", felixBuddiesList[1].Record.GetField("name").Value)

	// Linus.buddy links to Felix
	// Felix.buddies links Linux and Keiko
	sql = `SELECT FROM Cat WHERE name = 'Linus' OR name = 'Felix' ORDER BY name DESC`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	linusBuddy = docs[0].GetField("buddy").Value.(*oschema.OLink)
	Assert(linusBuddy.Record != nil, "Record should be filled in")
	Equals("Germaine", linusBuddy.Record.GetField("name").Value)

	Assert(docs[1].GetField("buddy") == nil, "Felix should have no 'buddy'")
	felixBuddiesList = docs[1].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(felixBuddiesList))
	Equals("Linus", felixBuddiesList[0].Record.GetField("name").Value)
	Equals("Keiko", felixBuddiesList[1].Record.GetField("name").Value)
	Equals("Anna", felixBuddiesList[1].Record.GetField("caretaker").Value)

	// check that Felix's reference to Linus has Linus' link filled in
	Equals("Germaine", felixBuddiesList[0].Record.GetField("buddy").Value.(*oschema.OLink).Record.GetField("name").Value)

	// ------

	sql = `select * from Cat where buddies is not null ORDER BY name`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	felixDoc = docs[0]
	Equals("Felix", felixDoc.GetField("name").Value)
	felixBuddiesList = felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(felixBuddiesList))
	Equals(2, len(felixBuddiesList))
	felixBuddy0 := felixBuddiesList[0]
	Assert(felixBuddy0.RID.ClusterID != -1, "RID should be filled in")
	Equals("Linus", felixBuddy0.Record.GetField("name").Value)
	felixBuddy1 := felixBuddiesList[1]
	Assert(felixBuddy1.RID.ClusterID != -1, "RID should be filled in")
	Equals("Keiko", felixBuddy1.Record.GetField("name").Value)

	// now test that the LINK docs had their LINKs filled in
	linusDocViaFelix := felixBuddy0.Record
	linusBuddyLink := linusDocViaFelix.GetField("buddy").Value.(*oschema.OLink)
	Equals("Germaine", linusBuddyLink.Record.GetField("name").Value)

	// ------

	// Create two records that reference only each other (a.buddy = b and b.buddy = a)
	//  do:  SELECT FROM Cat where name = "a" OR name = "b" with *:-1 fetchPlan
	//  and make sure if the LINK fields are filled in
	//  with the *:-1 fetchPlan, OrientDB server will return all the link docs in the
	//  "supplementary section" even if they are already in the primary docs section

	sql = `INSERT INTO Cat SET name='Tom', age=3`
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	tomRID := docs[0].RID
	Assert(tomRID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	sql = `INSERT INTO Cat SET name='Nick', age=4, buddy=?`
	_, docs, err = obinary.SQLCommand(dbc, sql, tomRID.String())
	Ok(err)
	Equals(1, len(docs))
	nickRID := docs[0].RID

	sql = `UPDATE Cat SET buddy=? WHERE name='Tom' and age=3`
	_, _, err = obinary.SQLCommand(dbc, sql, nickRID.String())
	Ok(err)

	obinary.ReloadSchema(dbc)

	// in this case the buddy links should be filled in with full Documents
	sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks, "Tom", "Nick")
	Ok(err)
	Equals(2, len(docs))
	tomDoc := docs[0]
	nickDoc := docs[1]
	Equals("Tom", tomDoc.GetField("name").Value)
	Equals("Nick", nickDoc.GetField("name").Value)

	// TODO: this section fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	tomsBuddy := tomDoc.GetField("buddy").Value.(*oschema.OLink)
	nicksBuddy := nickDoc.GetField("buddy").Value.(*oschema.OLink)
	// Assert(tomsBuddy.Record != nil, "should have retrieved the link record")
	// Assert(nicksBuddy.Record != nil, "should have retrieved the link record")
	// Equals("Nick", tomsBuddy.Record.GetField("name").Value)
	// Equals("Tom", nicksBuddy.Record.GetField("name").Value)

	// in this case the buddy links should NOT be filled in with full Documents
	sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
	docs, err = obinary.SQLQuery(dbc, sql, "", "Tom", "Nick")
	Ok(err)
	Equals(2, len(docs))
	tomDoc = docs[0]
	nickDoc = docs[1]
	Equals("Tom", tomDoc.GetField("name").Value)
	Equals("Nick", nickDoc.GetField("name").Value)

	tomsBuddy = tomDoc.GetField("buddy").Value.(*oschema.OLink)
	nicksBuddy = nickDoc.GetField("buddy").Value.(*oschema.OLink)
	Assert(tomsBuddy.RID.ClusterID != -1, "RID should be filled in")
	Assert(nicksBuddy.RID.ClusterID != -1, "RID should be filled in")
	Assert(tomsBuddy.Record == nil, "Record should NOT be filled in")
	Assert(nicksBuddy.Record == nil, "Record should NOT be filled in")

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
	docs, err = obinary.SQLQuery(dbc, sql, "buddy:0 buddies:1 buddySet:0 notes:0", "Felix")
	// docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks, "Felix")
	Ok(err)
	Equals(1, len(docs))
	Equals("Felix", docs[0].GetField("name").Value)
	buddies = docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(ByRID(buddies))
	Equals(2, len(buddies))
	linusDoc := buddies[0].Record
	Assert(linusDoc != nil, "first level should be filled in")
	linusBuddy = linusDoc.GetField("buddy").Value.(*oschema.OLink)
	Assert(linusBuddy.RID.ClusterID != -1, "RID should be filled in")
	Assert(linusBuddy.Record == nil, "Record of second level should NOT be filled in")

	keikoDoc := buddies[1].Record
	Assert(keikoDoc != nil, "first level should be filled in")

	// ------

	/* ---[ Try DATETIME ]--- */

	sql = `Create PROPERTY Cat.dt DATETIME`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "dt")
	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	sql = `Create PROPERTY Cat.birthday DATE`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	defer removeProperty(dbc, "Cat", "birthday")
	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	// OrientDB DATETIME is precise to the millisecond
	sql = `INSERT into Cat SET name = 'Bruce', dt = '2014-11-25 09:14:54'`
	ogl.Debugln(sql)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	Equals("Bruce", docs[0].GetField("name").Value)

	dt := docs[0].GetField("dt").Value.(time.Time)
	zone, zoneOffset := dt.Zone()
	zoneLocation := time.FixedZone(zone, zoneOffset)
	expectedTm, err := time.ParseInLocation("2006-01-02 03:04:05", "2014-11-25 09:14:54", zoneLocation)
	Ok(err)
	Equals(expectedTm.String(), dt.String())

	bruceRID := docs[0].RID

	sql = `INSERT into Cat SET name = 'Tiger', birthday = '2014-11-25'`
	ogl.Debugln(sql)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	Equals("Tiger", docs[0].GetField("name").Value)

	birthdayTm := docs[0].GetField("birthday").Value.(time.Time)
	zone, zoneOffset = birthdayTm.Zone()
	zoneLocation = time.FixedZone(zone, zoneOffset)
	expectedTm, err = time.ParseInLocation("2006-01-02", "2014-11-25", zoneLocation)
	Ok(err)
	Equals(expectedTm.String(), birthdayTm.String())

	tigerRID := docs[0].RID

	/* ---[ Clean up above expts ]--- */

	ridsToDelete := []interface{}{felixRID, tildeRID, charlieRID, bruceRID, tigerRID, germaineRID, tomRID, nickRID}
	sql = fmt.Sprintf("DELETE from [%s,%s,%s,%s,%s,%s,%s,%s]", ridsToDelete...)

	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(strconv.Itoa(len(ridsToDelete)), retval)
	Equals(0, len(docs))

	obinary.ReloadSchema(dbc)

	sql = "DROP CLASS Patient"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals("true", retval)
	Equals(0, len(docs))
}

func createAndUpdateRecordsViaNativeAPI(dbc *obinary.DBClient) {
	err := obinary.OpenDatabase(dbc, dbDocumentName, constants.DocumentDB, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	/* ---[ creation ]--- */

	winston := oschema.NewDocument("Cat")
	winston.Field("name", "Winston").
		Field("caretaker", "Churchill").
		FieldWithType("age", 7, oschema.INTEGER)
	Equals(-1, int(winston.RID.ClusterID))
	Equals(-1, int(winston.RID.ClusterPos))
	Equals(-1, int(winston.Version))
	err = obinary.CreateRecord(dbc, winston)
	Ok(err)
	Assert(int(winston.RID.ClusterID) > -1, "RID should be filled in")
	Assert(int(winston.RID.ClusterPos) > -1, "RID should be filled in")
	Assert(int(winston.Version) > -1, "Version should be filled in")

	/* ---[ update STRING and INTEGER field ]--- */

	versionBefore := winston.Version

	winston.Field("caretaker", "Lolly")      // this updates the field locally
	winston.Field("age", 8)                  // this updates the field locally
	err = obinary.UpdateRecord(dbc, winston) // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < winston.Version, "version should have incremented")

	docs, err := obinary.SQLQuery(dbc, "select * from Cat where @rid="+winston.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))

	winstonFromQuery := docs[0]
	Equals("Winston", winstonFromQuery.GetField("name").Value)
	Equals(8, toInt(winstonFromQuery.GetField("age").Value))
	Equals("Lolly", winstonFromQuery.GetField("caretaker").Value)

	/* ---[ next creation ]--- */

	daemon := oschema.NewDocument("Cat")
	daemon.Field("name", "Daemon").Field("caretaker", "Matt").Field("age", 4)
	err = obinary.CreateRecord(dbc, daemon)
	Ok(err)

	indy := oschema.NewDocument("Cat")
	indy.Field("name", "Indy").Field("age", 6)
	err = obinary.CreateRecord(dbc, indy)
	Ok(err)

	sql := fmt.Sprintf("select from Cat where @rid=%s or @rid=%s or @rid=%s ORDER BY name",
		winston.RID, daemon.RID, indy.RID)
	resultDocs, err := obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(3, len(resultDocs))
	Equals(daemon.RID, resultDocs[0].RID)
	Equals(indy.RID, resultDocs[1].RID)
	Equals(winston.RID, resultDocs[2].RID)

	Equals(indy.Version, resultDocs[1].Version)
	Equals("Matt", resultDocs[0].GetField("caretaker").Value)

	sql = fmt.Sprintf("DELETE FROM [%s, %s, %s]", winston.RID, daemon.RID, indy.RID)
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	/* ---[ Test DATE Serialization ]--- */
	createAndUpdateRecordsWithDate(dbc)

	/* ---[ Test DATETIME Serialization ]--- */
	createAndUpdateRecordsWithDateTime(dbc)

	// test inserting wrong values for date and datetime
	testCreationOfMismatchedTypesAndValues(dbc)

	/* ---[ Test Boolean, Byte and Short Serialization ]--- */
	createAndUpdateRecordsWithBooleanByteAndShort(dbc)

	/* ---[ Test Int, Long, Float and Double Serialization ]--- */
	createAndUpdateRecordsWithIntLongFloatAndDouble(dbc)

	/* ---[ Test BINARY Serialization ]--- */
	createAndUpdateRecordsWithBINARYType(dbc)

	/* ---[ Test EMBEDDEDRECORD Serialization ]--- */
	createAndUpdateRecordsWithEmbeddedRecords(dbc)

	/* ---[ Test EMBEDDEDLIST, EMBEDDEDSET Serialization ]--- */
	createAndUpdateRecordsWithEmbeddedLists(dbc, oschema.EMBEDDEDLIST)
	createAndUpdateRecordsWithEmbeddedLists(dbc, oschema.EMBEDDEDSET)

	/* ---[ Test Link Serialization ]--- */
	createAndUpdateRecordsWithLinks(dbc)

	/* ---[ Test LinkList/LinkSet Serialization ]--- */
	createAndUpdateRecordsWithLinkLists(dbc, oschema.LINKLIST)
	// createAndUpdateRecordsWithLinkLists(dbc, oschema.LINKSET)

	/* ---[ Test LinkMap Serialization ]--- */
	createAndUpdateRecordsWithLinkMap(dbc)
}

func createAndUpdateRecordsWithLinkMap(dbc *obinary.DBClient) {
	sql := `CREATE PROPERTY Cat.notes LINKMAP`
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer removeProperty(dbc, "Cat", "notes")
	ridsToDelete := make([]string, 0, 4)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, _, err = obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+delrid)
			Ok(err)
		}
	}()

	cat1 := oschema.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 1).
		Field("caretaker", "Jackie")

	err = obinary.CreateRecord(dbc, cat1)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	linkToCat1 := &oschema.OLink{RID: cat1.RID, Record: cat1}
	linkmap := map[string]*oschema.OLink{"bff": linkToCat1}

	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "A2").
		Field("age", 2).
		Field("caretaker", "Ben").
		FieldWithType("notes", linkmap, oschema.LINKMAP)

	err = obinary.CreateRecord(dbc, cat2)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	linkmap["7th-best-friend"] = &oschema.OLink{RID: cat2.RID}

	cat3 := oschema.NewDocument("Cat")
	cat3.Field("name", "A3").
		Field("age", 3).
		Field("caretaker", "Konrad").
		FieldWithType("notes", linkmap, oschema.LINKMAP)

	err = obinary.CreateRecord(dbc, cat3)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	docs, err := obinary.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Ok(err)
	Equals(2, len(docs))

	cat2FromQuery := docs[0]
	Equals("A2", cat2FromQuery.GetField("name").Value)
	Equals(2, toInt(cat2FromQuery.GetField("age").Value))
	notesFromQuery := cat2FromQuery.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(1, len(notesFromQuery))
	Equals(notesFromQuery["bff"].RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals("A3", cat3FromQuery.GetField("name").Value)
	Equals(3, toInt(cat3FromQuery.GetField("age").Value))
	notesFromQuery = cat3FromQuery.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(2, len(notesFromQuery))
	Equals(notesFromQuery["bff"].RID, cat1.RID)
	Equals(notesFromQuery["7th-best-friend"].RID, cat2.RID)
}

func createAndUpdateRecordsWithLinkLists(dbc *obinary.DBClient, collType oschema.ODataType) {
	sql := "CREATE PROPERTY Cat.catfriends " + oschema.ODataTypeNameFor(collType) + " Cat"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer removeProperty(dbc, "Cat", "catfriends")
	ridsToDelete := make([]string, 0, 4)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, _, err = obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+delrid)
			Ok(err)
		}
	}()

	cat1 := oschema.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 1).
		Field("caretaker", "Jackie")

	err = obinary.CreateRecord(dbc, cat1)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	linkToCat1 := &oschema.OLink{RID: cat1.RID, Record: cat1}

	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "A2").
		Field("age", 2).
		Field("caretaker", "Ben").
		FieldWithType("catfriends", []*oschema.OLink{linkToCat1}, collType)

	err = obinary.CreateRecord(dbc, cat2)
	Ok(err)
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

	err = obinary.CreateRecord(dbc, cat3)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	docs, err := obinary.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Ok(err)
	Equals(2, len(docs))

	cat2FromQuery := docs[0]
	Equals("A2", cat2FromQuery.GetField("name").Value)
	Equals(2, toInt(cat2FromQuery.GetField("age").Value))
	catFriendsFromQuery := cat2FromQuery.GetField("catfriends").Value.([]*oschema.OLink)
	Equals(1, len(catFriendsFromQuery))
	Equals(catFriendsFromQuery[0].RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals("A3", cat3FromQuery.GetField("name").Value)
	Equals(3, toInt(cat3FromQuery.GetField("age").Value))
	catFriendsFromQuery = cat3FromQuery.GetField("catfriends").Value.([]*oschema.OLink)
	Equals(2, len(catFriendsFromQuery))
	sort.Sort(ByRID(catFriendsFromQuery))
	Equals(catFriendsFromQuery[0].RID, cat1.RID)
	Equals(catFriendsFromQuery[1].RID, cat2.RID)
}

func createAndUpdateRecordsWithLinks(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.catlink LINK Cat"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer removeProperty(dbc, "Cat", "catlink")
	ridsToDelete := make([]string, 0, 10)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, _, err = obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+delrid)
			Ok(err)
		}
	}()

	// ------

	cat1 := oschema.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 2).
		Field("caretaker", "Jackie")

	err = obinary.CreateRecord(dbc, cat1)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	cat2 := oschema.NewDocument("Cat")
	linkToCat1 := &oschema.OLink{RID: cat1.RID, Record: cat1}
	cat2.Field("name", "A2").
		Field("age", 3).
		Field("caretaker", "Jimmy").
		FieldWithType("catlink", linkToCat1, oschema.LINK)

	err = obinary.CreateRecord(dbc, cat2)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	/* ---[ try without FieldWithType ]--- */

	cat3 := oschema.NewDocument("Cat")
	linkToCat2 := &oschema.OLink{RID: cat2.RID, Record: cat2} // also, only use RID, not record
	cat3.Field("name", "A3").
		Field("age", 4).
		Field("caretaker", "Ralston").
		Field("catlink", linkToCat2)

	err = obinary.CreateRecord(dbc, cat3)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	// test that they were inserted correctly and come back correctly

	docs, err := obinary.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Ok(err)
	Equals(2, len(docs))

	cat2FromQuery := docs[0]
	Equals("A2", cat2FromQuery.GetField("name").Value)
	Equals(3, toInt(cat2FromQuery.GetField("age").Value))
	linkToCat1FromQuery := cat2FromQuery.GetField("catlink").Value.(*oschema.OLink)
	Equals(linkToCat1FromQuery.RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals("A3", cat3FromQuery.GetField("name").Value)
	Equals(4, toInt(cat3FromQuery.GetField("age").Value))
	linkToCat2FromQuery := cat3FromQuery.GetField("catlink").Value.(*oschema.OLink)
	Equals(linkToCat2FromQuery.RID, cat2.RID)
}

func createAndUpdateRecordsWithEmbeddedLists(dbc *obinary.DBClient, embType oschema.ODataType) {
	sql := "CREATE PROPERTY Cat.embstrings " + oschema.ODataTypeNameFor(embType) + " string"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = "CREATE PROPERTY Cat.emblongs " + oschema.ODataTypeNameFor(embType) + " LONG"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = "CREATE PROPERTY Cat.embcats " + oschema.ODataTypeNameFor(embType) + " Cat"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	// ------
	// housekeeping

	defer removeProperty(dbc, "Cat", "embstrings")
	defer removeProperty(dbc, "Cat", "emblongs")
	defer removeProperty(dbc, "Cat", "embcats")
	ridsToDelete := make([]string, 0, 10)

	defer func() {
		for _, delrid := range ridsToDelete {
			_, _, err = obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+delrid)
			Ok(err)
		}
	}()

	// ------

	embStrings := []interface{}{"one", "two", "three"}
	stringList := oschema.NewEmbeddedSlice(embStrings, oschema.STRING)

	Equals(oschema.STRING, stringList.Type())
	Equals("two", stringList.Values()[1])

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "Yugo").
		Field("age", 33)

	if embType == oschema.EMBEDDEDLIST {
		cat.Field("embstrings", stringList)
	} else {
		cat.FieldWithType("embstrings", stringList, embType)
	}

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	Assert(cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err := obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))
	catFromQuery := docs[0]
	Equals(33, toInt(catFromQuery.GetField("age").Value))
	embstringsFieldFromQuery := catFromQuery.GetField("embstrings")

	Equals("embstrings", embstringsFieldFromQuery.Name)
	Equals(embType, embstringsFieldFromQuery.Type)
	embListFromQuery, ok := embstringsFieldFromQuery.Value.([]interface{})
	Assert(ok, "Cast to oschema.[]interface{} failed")

	sort.Sort(ByStringVal(embListFromQuery))
	Equals(3, len(embListFromQuery))
	Equals("one", embListFromQuery[0])
	Equals("three", embListFromQuery[1])
	Equals("two", embListFromQuery[2])

	// ------

	embLongs := []interface{}{int64(22), int64(4444), int64(constants.MaxInt64 - 12)}
	int64List := oschema.NewEmbeddedSlice(embLongs, oschema.LONG)

	Equals(oschema.LONG, int64List.Type())
	Equals(int64(22), int64List.Values()[0])

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "Barry").
		Field("age", 40).
		FieldWithType("emblongs", int64List, embType)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	Assert(cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))
	catFromQuery = docs[0]
	Equals(40, toInt(catFromQuery.GetField("age").Value))
	emblongsFieldFromQuery := catFromQuery.GetField("emblongs")

	Equals("emblongs", emblongsFieldFromQuery.Name)
	Equals(embType, emblongsFieldFromQuery.Type)
	embListFromQuery, ok = emblongsFieldFromQuery.Value.([]interface{})
	Assert(ok, "Cast to oschema.[]interface{} failed")

	Equals(3, len(embListFromQuery))
	Equals(int64(22), embListFromQuery[0])
	Equals(int64(4444), embListFromQuery[1])
	Equals(int64(constants.MaxInt64-12), embListFromQuery[2])

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

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	Assert(cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))
	catFromQuery = docs[0]
	Equals(3, toInt(catFromQuery.GetField("age").Value))
	embcatsFieldFromQuery := catFromQuery.GetField("embcats")

	Equals("embcats", embcatsFieldFromQuery.Name)
	Equals(embType, embcatsFieldFromQuery.Type)
	embListFromQuery, ok = embcatsFieldFromQuery.Value.([]interface{})
	Assert(ok, "Cast to oschema.[]interface{} failed")

	Equals(2, len(embListFromQuery))
	sort.Sort(ByEmbeddedCatName(embListFromQuery))

	embCatDoc0, ok := embListFromQuery[0].(*oschema.ODocument)
	Assert(ok, "Cast to *oschema.ODocument failed")
	embCatDoc1, ok := embListFromQuery[1].(*oschema.ODocument)
	Assert(ok, "Cast to *oschema.ODocument failed")

	Equals("Gordo", embCatDoc0.GetField("name").Value)
	Equals(40, toInt(embCatDoc0.GetField("age").Value))
	Equals("Joan", embCatDoc1.GetField("name").Value)
	Equals("Marcia", embCatDoc1.GetField("caretaker").Value)

}

func createAndUpdateRecordsWithEmbeddedRecords(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.embcat EMBEDDED"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer removeProperty(dbc, "Cat", "embcat")

	ridsToDelete := make([]string, 0, 10)
	defer func() {
		for _, delrid := range ridsToDelete {
			_, _, err = obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+delrid)
			Ok(err)
		}
	}()

	/* ---[ FieldWithType ]--- */

	embcat := oschema.NewDocument("Cat")
	embcat.Field("name", "MaryLulu").
		Field("age", 47)

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "Willard").
		Field("age", 4).
		FieldWithType("embcat", embcat, oschema.EMBEDDED)

	// err = obinary.ReloadSchema(dbc) // TMP => LEFT OFF: try without this => does it work if write name and type, rather than id?
	// Ok(err)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)

	Assert(int(embcat.RID.ClusterID) < int(0), "embedded RID should be NOT filled in")
	Assert(cat.RID.ClusterID >= 0, "RID should be filled in")

	ridsToDelete = append(ridsToDelete, cat.RID.String())

	docs, err := obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))

	catFromQuery := docs[0]
	Equals("Willard", catFromQuery.GetField("name").Value.(string))
	Equals(4, toInt(catFromQuery.GetField("age").Value))
	Equals(oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	embCatFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Assert(embCatFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	Assert(embCatFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	Assert(embCatFromQuery.Version < 0, "Version should be unset")
	Equals(2, len(embCatFromQuery.FieldNames()))
	Equals(47, toInt(embCatFromQuery.GetField("age").Value))
	Equals("MaryLulu", embCatFromQuery.GetField("name").Value.(string))

	/* ---[ Field No Type Specified ]--- */

	embcat = oschema.NewDocument("Cat")
	embcat.Field("name", "Tsunami").
		Field("age", 33).
		Field("purebreed", false)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "Cara").
		Field("age", 3).
		Field("embcat", embcat)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	Assert(int(embcat.RID.ClusterID) < int(0), "embedded RID should be NOT filled in")
	Assert(cat.RID.ClusterID >= 0, "RID should be filled in")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))

	catFromQuery = docs[0]
	Equals("Cara", catFromQuery.GetField("name").Value.(string))
	Equals(3, toInt(catFromQuery.GetField("age").Value))
	Equals(oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	embCatFromQuery = catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Assert(embCatFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	Assert(embCatFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	Assert(embCatFromQuery.Version < 0, "Version should be unset")
	Equals("Cat", embCatFromQuery.Classname)
	Equals(3, len(embCatFromQuery.FieldNames()))
	Equals(33, toInt(embCatFromQuery.GetField("age").Value))
	Equals("Tsunami", embCatFromQuery.GetField("name").Value.(string))
	Equals(false, embCatFromQuery.GetField("purebreed").Value.(bool))

	/* ---[ Embedded with New Classname (not in DB) ]--- */

	moonpie := oschema.NewDocument("Moonpie")
	moonpie.Field("sku", "AB425827ACX3").
		Field("allnatural", false).
		FieldWithType("oz", 6.5, oschema.FLOAT)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "LeCara").
		Field("age", 7).
		Field("embcat", moonpie)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))

	catFromQuery = docs[0]
	Equals("LeCara", catFromQuery.GetField("name").Value.(string))
	Equals(7, toInt(catFromQuery.GetField("age").Value))
	Equals(oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	moonpieFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Assert(moonpieFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	Assert(moonpieFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	Assert(moonpieFromQuery.Version < 0, "Version should be unset")
	Equals("", moonpieFromQuery.Classname) // it throws out the classname => TODO: check serialized binary on this
	Equals(3, len(moonpieFromQuery.FieldNames()))
	Equals("AB425827ACX3", moonpieFromQuery.GetField("sku").Value)
	Equals(float32(6.5), moonpieFromQuery.GetField("oz").Value.(float32))
	Equals(false, moonpieFromQuery.GetField("allnatural").Value.(bool))

	noclass := oschema.NewDocument("")
	noclass.Field("sku", "AB425827ACX3222").
		Field("allnatural", true).
		FieldWithType("oz", 6.5, oschema.DOUBLE)

	cat = oschema.NewDocument("Cat")
	cat.Field("name", "LeCarre").
		Field("age", 87).
		Field("embcat", noclass)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	ridsToDelete = append(ridsToDelete, cat.RID.String())

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))

	catFromQuery = docs[0]
	Equals("LeCarre", catFromQuery.GetField("name").Value.(string))
	Equals(87, toInt(catFromQuery.GetField("age").Value))
	Equals(oschema.EMBEDDED, catFromQuery.GetField("embcat").Type)

	noclassFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Equals("", noclassFromQuery.Classname) // it throws out the classname
	Equals(3, len(noclassFromQuery.FieldNames()))
	Equals("AB425827ACX3222", noclassFromQuery.GetField("sku").Value)
	Equals(float64(6.5), noclassFromQuery.GetField("oz").Value.(float64))
	Equals(true, noclassFromQuery.GetField("allnatural").Value.(bool))

	/* ---[ update ]--- */

	versionBefore := cat.Version

	moonshine := oschema.NewDocument("")
	moonshine.Field("sku", "123").
		Field("allnatural", true).
		FieldWithType("oz", 99.092, oschema.FLOAT)

	cat.FieldWithType("embcat", moonshine, oschema.EMBEDDED) // updates the field locally
	Equals(true, cat.IsDirty())

	err = obinary.UpdateRecord(dbc, cat) // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < cat.Version, "version should have incremented")
	Equals(false, cat.IsDirty())

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	catFromQuery = docs[0]

	mshineFromQuery := catFromQuery.GetField("embcat").Value.(*oschema.ODocument)
	Equals("123", mshineFromQuery.GetField("sku").Value)
	Equals(true, mshineFromQuery.GetField("allnatural").Value)
	Equals(float32(99.092), mshineFromQuery.GetField("oz").Value)
}

func createAndUpdateRecordsWithBINARYType(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.bin BINARY"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.bin")
	}()

	/* ---[ FieldWithType ]--- */
	str := "four, five, six, pick up sticks"
	bindata := []byte(str)

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "little-jimmy").
		Field("age", 1).
		FieldWithType("bin", bindata, oschema.BINARY)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	Assert(cat.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+cat.RID.String())
	}()

	docs, err := obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat.RID.String())
	Ok(err)
	Equals(1, len(docs))

	catFromQuery := docs[0]

	Equals(cat.GetField("bin").Value, catFromQuery.GetField("bin").Value)
	Equals(str, string(catFromQuery.GetField("bin").Value.([]byte)))

	/* ---[ Field No Type Specified ]--- */
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

	Assert(cat2.RID.ClusterID <= 0, "RID should NOT be filled in")

	err = obinary.CreateRecord(dbc, cat2)
	Ok(err)
	Assert(cat2.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+cat2.RID.String())
	}()

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat2.RID.String())
	Ok(err)
	Equals(1, len(docs))
	cat2FromQuery := docs[0]

	Equals(bindata2, cat2FromQuery.GetField("bin").Value.([]byte))

	/* ---[ update ]--- */

	versionBefore := cat.Version

	newbindata := []byte("Now Gluten Free!")
	cat.FieldWithType("bin", newbindata, oschema.BINARY)
	err = obinary.UpdateRecord(dbc, cat) // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < cat.Version, "version should have incremented")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	catFromQuery = docs[0]
	Equals(newbindata, catFromQuery.GetField("bin").Value)
}

func createAndUpdateRecordsWithIntLongFloatAndDouble(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.ii INTEGER"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.ii")
	}()

	sql = "CREATE PROPERTY Cat.lg LONG"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.lg")
	}()

	sql = "CREATE PROPERTY Cat.ff FLOAT"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.ff")
	}()

	sql = "CREATE PROPERTY Cat.dd DOUBLE"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.dd")
	}()

	floatval := float32(constants.MaxInt32) + 0.5834
	// doubleval := float64(7976931348623157 + e308) // Double.MIN_VALUE in Java => too big for Go
	// doubleval := float64(9E-324) // Double.MIN_VALUE in Java  => too big for Go
	doubleval := float64(7.976931348623157E+222)

	/* ---[ FieldWithType ]--- */
	cat := oschema.NewDocument("Cat")
	cat.Field("name", "sourpuss").
		Field("age", 15).
		FieldWithType("ii", constants.MaxInt32, oschema.INTEGER).
		FieldWithType("lg", constants.MaxInt64, oschema.LONG).
		FieldWithType("ff", floatval, oschema.FLOAT).
		FieldWithType("dd", doubleval, oschema.DOUBLE)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	Assert(cat.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+cat.RID.String())
	}()

	docs, err := obinary.SQLQuery(dbc, "select from Cat where ii = ?", "", strconv.Itoa(int(constants.MaxInt32)))
	Ok(err)
	Equals(1, len(docs))

	catFromQuery := docs[0]

	Equals(toInt(cat.GetField("ii").Value), toInt(catFromQuery.GetField("ii").Value))
	Equals(toInt(cat.GetField("lg").Value), toInt(catFromQuery.GetField("lg").Value))
	Equals(cat.GetField("ff").Value, catFromQuery.GetField("ff").Value)
	Equals(cat.GetField("dd").Value, catFromQuery.GetField("dd").Value)

	/* ---[ Field ]--- */

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

	err = obinary.CreateRecord(dbc, cat2)
	Ok(err)

	Assert(cat2.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+cat2.RID.String())
	}()

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid = ?", "", cat2.RID.String())
	Ok(err)
	Equals(1, len(docs))
	cat2FromQuery := docs[0]

	Equals(toInt(cat2.GetField("ii").Value), toInt(cat2FromQuery.GetField("ii").Value))
	Equals(toInt(cat2.GetField("lg").Value), toInt(cat2FromQuery.GetField("lg").Value))
	Equals(cat2.GetField("ff").Value, cat2FromQuery.GetField("ff").Value)
	Equals(cat2.GetField("dd").Value, cat2FromQuery.GetField("dd").Value)

	/* ---[ update ]--- */

	cat2.Field("ii", int32(1)).
		Field("lg", int64(2)).
		Field("ff", float32(3.3)).
		Field("dd", float64(4.444))

	versionBefore := cat2.Version

	err = obinary.UpdateRecord(dbc, cat2) // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < cat2.Version, "version should have incremented")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid="+cat2.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	cat2FromQuery = docs[0]
	Equals(int32(1), cat2FromQuery.GetField("ii").Value)
	Equals(int64(2), cat2FromQuery.GetField("lg").Value)
	Equals(float32(3.3), cat2FromQuery.GetField("ff").Value)
	Equals(float64(4.444), cat2FromQuery.GetField("dd").Value)
}

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

func createAndUpdateRecordsWithBooleanByteAndShort(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.x BOOLEAN"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.x")
	}()

	sql = "CREATE PROPERTY Cat.y BYTE"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.y")
	}()

	sql = "CREATE PROPERTY Cat.z SHORT"
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.z")
	}()

	cat := oschema.NewDocument("Cat")
	cat.Field("name", "kitteh").
		Field("age", 4).
		Field("x", false).
		Field("y", byte(55)).
		Field("z", int16(5123))

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
	Assert(cat.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+cat.RID.String())
	}()

	docs, err := obinary.SQLQuery(dbc, "select from Cat where y = 55", "")
	Ok(err)
	Equals(1, len(docs))

	catFromQuery := docs[0]
	Equals(cat.GetField("x").Value.(bool), catFromQuery.GetField("x").Value.(bool))
	Equals(cat.GetField("y").Value.(byte), catFromQuery.GetField("y").Value.(byte))
	Equals(cat.GetField("z").Value.(int16), catFromQuery.GetField("z").Value.(int16))

	// try with explicit types
	cat2 := oschema.NewDocument("Cat")
	cat2.Field("name", "cat2").
		Field("age", 14).
		FieldWithType("x", true, oschema.BOOLEAN).
		FieldWithType("y", byte(44), oschema.BYTE).
		FieldWithType("z", int16(16000), oschema.SHORT)

	err = obinary.CreateRecord(dbc, cat2)
	Ok(err)
	Assert(cat2.RID.ClusterID > 0, "RID should be filled in")

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM Cat WHERE @rid="+cat2.RID.String())
	}()

	docs, err = obinary.SQLQuery(dbc, "select from Cat where x = true", "")
	Ok(err)
	Equals(1, len(docs))

	cat2FromQuery := docs[0]
	Equals(cat2.GetField("x").Value.(bool), cat2FromQuery.GetField("x").Value.(bool))
	Equals(cat2.GetField("y").Value.(byte), cat2FromQuery.GetField("y").Value.(byte))
	Equals(cat2.GetField("z").Value.(int16), cat2FromQuery.GetField("z").Value.(int16))

	/* ---[ update ]--- */

	versionBefore := cat.Version

	cat.Field("x", true)
	cat.Field("y", byte(19))
	cat.Field("z", int16(6789))

	err = obinary.UpdateRecord(dbc, cat) // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < cat.Version, "version should have incremented")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	catFromQuery = docs[0]
	Equals(true, catFromQuery.GetField("x").Value)
	Equals(byte(19), catFromQuery.GetField("y").Value)
	Equals(int16(6789), catFromQuery.GetField("z").Value)

}

func testCreationOfMismatchedTypesAndValues(dbc *obinary.DBClient) {
	c1 := oschema.NewDocument("Cat")
	c1.Field("name", "fluffy1").
		Field("age", 22).
		FieldWithType("ddd", "not a datetime", oschema.DATETIME)
	err := obinary.CreateRecord(dbc, c1)
	Assert(err != nil, "Should have returned error")
	_, ok := oerror.ExtractCause(err).(oerror.ErrDataTypeMismatch)
	Assert(ok, "should be DataTypeMismatch error")

	c2 := oschema.NewDocument("Cat")
	c2.Field("name", "fluffy1").
		Field("age", 22).
		FieldWithType("ddd", float32(33244.2), oschema.DATE)
	err = obinary.CreateRecord(dbc, c2)
	Assert(err != nil, "Should have returned error")
	_, ok = oerror.ExtractCause(err).(oerror.ErrDataTypeMismatch)
	Assert(ok, "should be DataTypeMismatch error")

	// no fluffy1 should be in the database
	docs, err := obinary.SQLQuery(dbc, "select from Cat where name = 'fluffy1'", "")
	Ok(err)
	Equals(0, len(docs))
}

func createAndUpdateRecordsWithDateTime(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.ddd DATETIME"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.ddd")
	}()

	/* ---[ creation ]--- */

	now := time.Now()
	simba := oschema.NewDocument("Cat")
	simba.Field("name", "Simba").
		Field("age", 11).
		FieldWithType("ddd", now, oschema.DATETIME)
	err = obinary.CreateRecord(dbc, simba)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM "+simba.RID.String())
	}()

	Assert(simba.RID.ClusterID > 0, "ClusterID should be set")
	Assert(simba.RID.ClusterPos >= 0, "ClusterID should be set")

	docs, err := obinary.SQLQuery(dbc, "select from Cat where @rid="+simba.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	simbaFromQuery := docs[0]
	Equals(simba.RID, simbaFromQuery.RID)
	Equals(simba.GetField("ddd").Value, simbaFromQuery.GetField("ddd").Value)

	/* ---[ update ]--- */

	versionBefore := simba.Version

	twoDaysAgo := now.AddDate(0, 0, -2)

	simba.FieldWithType("ddd", twoDaysAgo, oschema.DATETIME) // updates the field locally
	err = obinary.UpdateRecord(dbc, simba)                   // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < simba.Version, "version should have incremented")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid="+simba.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	simbaFromQuery = docs[0]
	Equals(twoDaysAgo.Unix(), simbaFromQuery.GetField("ddd").Value.(time.Time).Unix())
}

func createAndUpdateRecordsWithDate(dbc *obinary.DBClient) {
	sql := "CREATE PROPERTY Cat.bday DATE"
	_, _, err := obinary.SQLCommand(dbc, sql)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DROP PROPERTY Cat.bday")
	}()

	const dtTemplate = "Jan 2, 2006 at 3:04pm (MST)"
	bdayTm, err := time.Parse(dtTemplate, "Feb 3, 1932 at 7:54pm (EST)")
	Ok(err)

	jj := oschema.NewDocument("Cat")
	jj.Field("name", "JJ").
		Field("age", 2).
		FieldWithType("bday", bdayTm, oschema.DATE)
	err = obinary.CreateRecord(dbc, jj)
	Ok(err)

	defer func() {
		obinary.SQLCommand(dbc, "DELETE FROM "+jj.RID.String())
	}()

	Assert(jj.RID.ClusterID > 0, "ClusterID should be set")
	Assert(jj.RID.ClusterPos >= 0, "ClusterID should be set")
	jjbdayAfterCreate := jj.GetField("bday").Value.(time.Time)
	Equals(0, jjbdayAfterCreate.Hour())
	Equals(0, jjbdayAfterCreate.Minute())
	Equals(0, jjbdayAfterCreate.Second())

	docs, err := obinary.SQLQuery(dbc, "select from Cat where @rid="+jj.RID.String(), "")
	Equals(1, len(docs))
	jjFromQuery := docs[0]
	Equals(jj.RID, jjFromQuery.RID)
	Equals(1932, jjFromQuery.GetField("bday").Value.(time.Time).Year())

	/* ---[ update ]--- */

	versionBefore := jj.Version

	oneYearLater := bdayTm.AddDate(1, 0, 0)

	jj.FieldWithType("bday", oneYearLater, oschema.DATE) // updates the field locally
	err = obinary.UpdateRecord(dbc, jj)                  // update the field in the remote DB
	Ok(err)
	Assert(versionBefore < jj.Version, "version should have incremented")

	docs, err = obinary.SQLQuery(dbc, "select from Cat where @rid="+jj.RID.String(), "")
	Ok(err)
	Equals(1, len(docs))
	jjFromQuery = docs[0]
	Equals(1933, jjFromQuery.GetField("bday").Value.(time.Time).Year())
}

// ------

func removeProperty(dbc *obinary.DBClient, class, property string) {
	sql := fmt.Sprintf("UPDATE %s REMOVE %s", class, property)
	_, _, err := obinary.SQLCommand(dbc, sql)
	if err != nil {
		ogl.Warnf("WARN: clean up error: %v\n", err)
	}
	sql = fmt.Sprintf("DROP PROPERTY %s.%s", class, property)
	_, _, err = obinary.SQLCommand(dbc, sql)
	if err != nil {
		ogl.Warnf("WARN: clean up error: %v\n", err)
	}
}

// ------
// Sort OLinks by RID

type ByRID []*oschema.OLink

func (slnk ByRID) Len() int {
	return len(slnk)
}

func (slnk ByRID) Swap(i, j int) {
	slnk[i], slnk[j] = slnk[j], slnk[i]
}

func (slnk ByRID) Less(i, j int) bool {
	return slnk[i].RID.String() < slnk[j].RID.String()
}

// ------
// sort ODocuments by name field

type ByEmbeddedCatName []interface{}

func (a ByEmbeddedCatName) Len() int {
	return len(a)
}

func (a ByEmbeddedCatName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByEmbeddedCatName) Less(i, j int) bool {
	return a[i].(*oschema.ODocument).GetField("name").Value.(string) < a[j].(*oschema.ODocument).GetField("name").Value.(string)
}

// ------

type ByStringVal []interface{}

func (sv ByStringVal) Len() int {
	return len(sv)
}

func (sv ByStringVal) Swap(i, j int) {
	sv[i], sv[j] = sv[j], sv[i]
}

func (sv ByStringVal) Less(i, j int) bool {
	return sv[i].(string) < sv[j].(string)
}

// ------

func ogonoriTestAgainstOrientDBServer() {
	var (
		dbc *obinary.DBClient
		err error
	)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	/* ---[ set ogl log level ]--- */
	ogl.SetLevel(ogl.WARN)

	testType := "dataOnly"

	if len(os.Args) > 1 {
		if os.Args[1] == "full" || os.Args[1] == "create" {
			testType = os.Args[1]
		}
	}

	dbc, err = obinary.NewDBClient(obinary.ClientOptions{})
	Ok(err)
	defer dbc.Close()

	/* ---[ run clean up in case of panics ]--- */
	defer func() {
		if r := recover(); r != nil {
			lvl := ogl.GetLevel()
			ogl.SetLevel(ogl.NORMAL)
			switch r {
			case "Equals fail", "Assert fail", "Ok fail":
				// do not print stack trace
			default:
				ogl.Printf("panic recovery: %v\nTrace:\n%s\n", r, debug.Stack())
			}
			ogl.SetLevel(lvl)
			cleanUp(dbc, testType == "full")
			os.Exit(1)
		}
	}()

	/* ---[ Use "native" API ]--- */
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

	/* ---[ Use Go database/sql API on Document DB ]--- */
	ogl.SetLevel(ogl.WARN)
	conxStr := "admin@admin:localhost/" + dbDocumentName
	databaseSQLAPI(conxStr)
	databaseSQLPreparedStmtAPI(conxStr)

	/* ---[ Graph DB ]--- */
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
	// ogl.Println("-------- JSON ---------")
	// fld := oschema.OField{int32(44), "foo", oschema.LONG, int64(33341234)}
	// bsjson, err := fld.ToJSON()
	// Ok(err)
	// ogl.Printf("%v\n", string(bsjson))

	// doc := oschema.NewDocument("Coolio")
	// doc.AddField("foo", &fld)
	// bsjson, err = doc.ToJSON()
	// Ok(err)
	// ogl.Printf("%v\n", string(bsjson))

	ogl.Println("DONE")
}

func explore() {
	dbc, err := obinary.NewDBClient(obinary.ClientOptions{})
	Ok(err)
	defer dbc.Close()

	err = obinary.OpenDatabase(dbc, "ogonoriTest", constants.DocumentDB, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	_, _, err = obinary.SQLCommand(dbc, "Create class Dalek")
	Ok(err)

	// err = obinary.ReloadSchema(dbc) // TMP => LEFT OFF: do the Dalek example with ogonori in explore
	// Ok(err)

	dingo := oschema.NewDocument("Dingo")
	dingo.FieldWithType("foo", "bar", oschema.STRING).
		FieldWithType("salad", 44, oschema.INTEGER)

	cat := oschema.NewDocument("Dalek")
	cat.Field("name", "dalek3").
		FieldWithType("embeddedDingo", dingo, oschema.EMBEDDED)

	// ogl.SetLevel(ogl.DEBUG)

	err = obinary.CreateRecord(dbc, cat)
	Ok(err)
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
		TestConcurrentClients()
	} else {
		ogonoriTestAgainstOrientDBServer()
	}
}
