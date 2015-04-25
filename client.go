package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"

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
// I'm using while developing the ogonori OrientDB Go client.
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

// EDIT THESE to match your setup
const (
	ogonoriDocDB   = "ogonoriTest"
	ogonoriGraphDB = "ogonoriGraphTest"
	adminUser      = "root"
	adminPassw     = "jiffylube"
)

func Equals(exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		ogl.SetLevel(ogl.WARN)
		panic("Equals fail")
	}
}

func Ok(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line})...)
		ogl.SetLevel(ogl.WARN)
		panic("Ok fail")
	}
}

func Assert(b bool, msg string) {
	if !b {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31mFAIL: %s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line})...)
		ogl.SetLevel(ogl.WARN)
		panic("Assert fail")
	}
}

func Pause(msg string) {
	fmt.Print(msg, "[Press Enter to Continue]: ")
	var s string
	_, err := fmt.Scan(&s)
	if err != nil {
		panic(err)
	}
}

func Fatal(err error) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
	ogl.SetLevel(ogl.WARN)
	panic(err)
}

func createOgonoriTestDB(dbc *obinary.DBClient, adminUser, adminPassw string, fullTest bool) {
	ogl.Println("-------- Create OgonoriTest DB --------\n")

	err := obinary.ConnectToServer(dbc, adminUser, adminPassw)
	Ok(err)

	Assert(dbc.GetSessionId() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	mapDBs, err := obinary.RequestDBList(dbc)
	Ok(err)
	ogl.Debugf("mapDBs: %v\n", mapDBs)
	gratefulTestPath, ok := mapDBs["GratefulDeadConcerts"]
	Assert(ok, "GratefulDeadConcerts not in DB list")
	Assert(strings.HasPrefix(gratefulTestPath, "plocal"), "plocal prefix for db path")

	// first check if ogonoriTest db exists and if so, drop it
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDocDB, constants.Persistent)
	Ok(err)

	if dbexists {
		if !fullTest {
			return
		}

		err = obinary.DropDatabase(dbc, ogonoriDocDB, constants.DocumentDb)
		Ok(err)
	}

	// err = obinary.CreateDatabase(dbc, ogonoriDocDB, constants.DocumentDbType, constants.Volatile)
	err = obinary.CreateDatabase(dbc, ogonoriDocDB, constants.DocumentDb, constants.Persistent)
	Ok(err)
	dbexists, err = obinary.DatabaseExists(dbc, ogonoriDocDB, constants.Persistent)
	Ok(err)
	Assert(dbexists, ogonoriDocDB+" should now exists after creating it")

	seedInitialData(dbc)

	// bug in OrientDB 2.0.1? :
	//  ERROR: com.orientechnologies.orient.core.exception.ODatabaseException Database 'plocal:/home/midpeter444/apps/orientdb-community-2.0.1/databases/ogonoriTest' is closed}
	// mapDBs, err = obinary.RequestDBList(dbc)
	// if err != nil {
	// 	Fatal(err)
	// }
	// fmt.Printf("%v\n", mapDBs)
	// ogonoriTestPath, ok := mapDBs[ogonoriDocDB]
	// Assert(ok, ogonoriDocDB+" not in DB list")
	// Assert(strings.HasPrefix(ogonoriTestPath, "plocal"), "plocal prefix for db path")
}

func seedInitialData(dbc *obinary.DBClient) {
	fmt.Println("OpenDatabase (seed round)")
	err := obinary.OpenDatabase(dbc, ogonoriDocDB, constants.DocumentDb, "admin", "admin")
	Ok(err)

	defer obinary.CloseDatabase(dbc)

	// seed initial data
	var sqlCmd string
	sqlCmd = "CREATE CLASS Animal"
	fmt.Println(sqlCmd)
	retval, docs, err := obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sqlCmd = "CREATE property Animal.name string"
	fmt.Println(sqlCmd)
	retval, docs, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sqlCmd = "CREATE property Animal.age integer"
	fmt.Println(sqlCmd)
	retval, docs, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sqlCmd = "CREATE CLASS Cat extends Animal"
	fmt.Println(sqlCmd)
	retval, docs, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sqlCmd = "CREATE property Cat.caretaker string"
	fmt.Println(sqlCmd)
	retval, docs, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sqlCmd = `INSERT INTO Cat (name, age, caretaker) VALUES ("Linus", 15, "Michael"), ("Keiko", 10, "Anna")`
	fmt.Println(sqlCmd)
	retval, docs, err = obinary.SQLCommand(dbc, sqlCmd)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)
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
	_, _, err := obinary.SQLCommand(dbc, "DELETE VERTEX Person")
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
	_, _, err = obinary.SQLCommand(dbc, "DROP CLASS Person")
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
	err := obinary.ConnectToServer(dbc, adminUser, adminPassw)
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
		dropDatabase(dbc, ogonoriDocDB, constants.DocumentDb)

	} else {
		_ = obinary.CloseDatabase(dbc)
		err := obinary.OpenDatabase(dbc, ogonoriDocDB, constants.DocumentDb, "admin", "admin")
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
		dropDatabase(dbc, ogonoriGraphDB, constants.GraphDb)

	} else {
		_ = obinary.CloseDatabase(dbc)
		err := obinary.OpenDatabase(dbc, ogonoriGraphDB, constants.GraphDb, "admin", "admin")
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

func databaseSqlAPI(conxStr string) {
	fmt.Println("\n-------- Using database/sql API --------\n")

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
	lastId, _ := res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastId)
	Equals(int64(1), nrows)
	Assert(lastId > int64(0), fmt.Sprintf("LastInsertId: %v", lastId))

	/* ---[ INSERT #2 ]--- */
	// insert with no params
	insertSQL = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	ogl.Println(insertSQL, "=> 'Filo', 4, 'Greek'")
	res, err = db.Exec(insertSQL, "Filo", 4, "Greek")
	Ok(err)
	nrows, _ = res.RowsAffected()
	ogl.Printf("nrows: %v\n", nrows)
	lastId, _ = res.LastInsertId()
	ogl.Printf("last insert id: %v\n", lastId)
	Equals(int64(1), nrows)
	Assert(lastId > int64(0), fmt.Sprintf("LastInsertId: %v", lastId))

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

func databaseSqlPreparedStmtAPI(conxStr string) {
	ogl.Println("\n-------- Using database/sql PreparedStatement API --------\n")

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
	insertId, err := result.LastInsertId()
	Ok(err)
	Assert(int(insertId) >= 0, "insertId was: "+strconv.Itoa(int(insertId)))

	// use again
	result, err = cmdStmt.Exec(2, "Jimmy", "John")
	Ok(err)
	nrows, err = result.RowsAffected()
	Ok(err)
	Equals(1, int(nrows))
	insertId2, err := result.LastInsertId()
	Ok(err)
	Assert(insertId != insertId2, "insertId was: "+strconv.Itoa(int(insertId)))

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
	insertId3, err := result.LastInsertId()
	Ok(err)
	Assert(int(insertId3) < 0, "should have negative insertId for a DELETE")

}

func dbClusterCommandsNativeAPI(dbc *obinary.DBClient) {
	ogl.Debugln("\n-------- CLUSTER commands --------\n")

	err := obinary.OpenDatabase(dbc, ogonoriDocDB, constants.DocumentDb, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	cnt1, err := obinary.GetClusterCountIncludingDeleted(dbc, "default", "index", "ouser")
	Ok(err)
	Assert(cnt1 > 0, "should be clusters")

	cnt2, err := obinary.GetClusterCount(dbc, "default", "index", "ouser")
	Ok(err)
	Assert(cnt1 >= cnt2, "counts should match or have more deleted")
	ogl.Debugf("Cluster count: %d\n", cnt2)

	begin, end, err := obinary.GetClusterDataRange(dbc, "ouser")
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
	clusterId, err := obinary.AddCluster(dbc, "bigapple")
	if err != nil {
		Fatal(err)
	}
	Assert(clusterId > 0, "clusterId should be bigger than zero")

	cnt, err := obinary.GetClusterCount(dbc, "bigapple")
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

	err := obinary.ConnectToServer(dbc, adminUser, adminPassw)
	Ok(err)

	Assert(dbc.GetSessionId() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	dbexists, err := obinary.DatabaseExists(dbc, ogonoriGraphDB, constants.Persistent)
	Ok(err)
	if dbexists {
		dropDatabase(dbc, ogonoriGraphDB, constants.GraphDb)
	}

	err = obinary.CreateDatabase(dbc, ogonoriGraphDB, constants.GraphDb, constants.Persistent)
	Ok(err)
	dbexists, err = obinary.DatabaseExists(dbc, ogonoriGraphDB, constants.Persistent)
	Ok(err)
	Assert(dbexists, ogonoriGraphDB+" should now exists after creating it")
}

func graphCommandsNativeAPI(dbc *obinary.DBClient, fullTest bool) {
	var (
		sql    string
		retval string
		docs   []*oschema.ODocument
		err    error
	)

	if fullTest {
		createOgonoriGraphDb(dbc)
	}

	ogl.Println("- - - - - - GRAPH COMMANDS - - - - - - -")

	err = obinary.OpenDatabase(dbc, ogonoriGraphDB, constants.GraphDb, "admin", "admin")
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

	// sql = `DELETE VERTEX #24:434` // need to get the @rid of Bob
	// sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	// sql = `DELETE VERTEX Person WHERE in.@Class = 'MembershipExpired'`

}

func dbCommandsNativeAPI(dbc *obinary.DBClient, fullTest bool) {
	ogl.Println("\n-------- database-level commands --------\n")

	var sql string
	var retval string

	err := obinary.OpenDatabase(dbc, ogonoriDocDB, constants.DocumentDb, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	/* ---[ query from the ogonoriTest database ]--- */

	sql = "select from Cat where name = 'Linus'"
	fetchPlan := ""
	docs, err := obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)

	linusDocRID := docs[0].Rid

	Assert(linusDocRID != "", "linusDocRID should not be nil")
	Assert(docs[0].Version > 0, fmt.Sprintf("Version is: %d", docs[0].Version))
	Equals(3, len(docs[0].FieldNames()))
	Equals("Cat", docs[0].Classname)

	nameField := docs[0].GetField("name")
	Assert(nameField != nil, "should be a 'name' field")

	ageField := docs[0].GetField("age")
	Assert(ageField != nil, "should be a 'age' field")

	caretakerField := docs[0].GetField("caretaker")
	Assert(caretakerField != nil, "should be a 'caretaker' field")

	Assert(nameField.Id != caretakerField.Id, "Ids should not match")
	Equals(byte(oschema.STRING), nameField.Typ)
	Equals(byte(oschema.STRING), caretakerField.Typ)
	Equals(byte(oschema.INTEGER), ageField.Typ)
	Equals("Linus", nameField.Value)
	Equals(int32(15), ageField.Value)
	Equals("Michael", caretakerField.Value)

	/* ---[ get by RID ]--- */
	docs, err = obinary.GetRecordByRID(dbc, linusDocRID, "")
	Ok(err)
	Equals(1, len(docs))
	docByRID := docs[0]
	Equals(linusDocRID, docByRID.Rid)
	Assert(docByRID.Version > 0, fmt.Sprintf("Version is: %d", docByRID.Version))
	Equals(3, len(docByRID.FieldNames()))
	Equals("Cat", docByRID.Classname)

	nameField = docByRID.GetField("name")
	Assert(nameField != nil, "should be a 'name' field")

	ageField = docByRID.GetField("age")
	Assert(ageField != nil, "should be a 'age' field")

	caretakerField = docByRID.GetField("caretaker")
	Assert(caretakerField != nil, "should be a 'caretaker' field")

	Assert(nameField.Id != caretakerField.Id, "Ids should not match")
	Equals(byte(oschema.STRING), nameField.Typ)
	Equals(byte(oschema.INTEGER), ageField.Typ)
	Equals(byte(oschema.STRING), caretakerField.Typ)
	Equals("Linus", nameField.Value)
	Equals(int32(15), ageField.Value)
	Equals("Michael", caretakerField.Value)

	ogl.Printf("docs returned by RID: %v\n", *(docs[0]))

	/* ---[ cluster data range ]--- */
	begin, end, err := obinary.GetClusterDataRange(dbc, "cat")
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
	Equals(byte(oschema.STRING), keiko.GetField("caretaker").Typ)
	Assert(keiko.Version > int32(0), "Version should be greater than zero")
	Assert(keiko.Rid != "", "RID should not be empty")

	linus := docs[1]
	Equals("Linus", linus.GetField("name").Value)
	Equals(int32(15), linus.GetField("age").Value)
	Equals("Michael", linus.GetField("caretaker").Value)

	zed := docs[2]
	Equals("Zed", zed.GetField("name").Value)
	Equals(int32(3), zed.GetField("age").Value)
	Equals("Shaw", zed.GetField("caretaker").Value)
	Equals(byte(oschema.STRING), zed.GetField("caretaker").Typ)
	Equals(byte(oschema.INTEGER), zed.GetField("age").Typ)
	Assert(zed.Version > int32(0), "Version should be greater than zero")
	Assert(zed.Rid != "", "RID should not be empty")

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
	ogl.Println("Deleting (sync) record #" + zed.Rid)
	err = obinary.DeleteRecordByRID(dbc, zed.Rid, zed.Version)
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

	sql = "CREATE CLASS Patient"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))
	ncls, err := strconv.ParseInt(retval, 10, 64)
	Ok(err)
	Assert(ncls > 10, "classnum should be greater than 10 but was: "+retval)

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
	linusRID := docs[0].Rid
	keikoRID := docs[1].Rid

	sql = `CREATE PROPERTY Cat.buddy LINK`
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
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
	Equals(linusRID, docs[0].GetField("buddy").Value)

	tildeRID := docs[0].Rid

	/* ---[ Try LINKLIST ]--- */

	sql = `CREATE PROPERTY Cat.buddies LINKLIST`
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
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
	buddies := docs[0].GetField("buddies").Value.([]string)
	Equals(2, len(buddies))
	Equals(linusRID, buddies[0])
	Equals(keikoRID, buddies[1])

	felixRID := docs[0].Rid

	/* ---[ Try LINKMAP ]--- */
	sql = `CREATE PROPERTY Cat.notes LINKMAP`
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY creation should be a positive number")
	Equals(0, len(docs))

	sql = fmt.Sprintf(`INSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": #%s, 30: #%s}`,
		linusRID, keikoRID)
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(1, len(docs))
	Equals(4, len(docs[0].FieldNames()))
	Equals("Anna", docs[0].GetField("caretaker").Value)
	Equals(linusRID, docs[0].GetField("notes").Value.(map[string]string)["bff"])
	Equals(keikoRID, docs[0].GetField("notes").Value.(map[string]string)["30"])

	charlieRID := docs[0].Rid

	sql = fmt.Sprintf("DELETE from [%s,%s,%s]", felixRID, tildeRID, charlieRID)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals("3", retval)
	Equals(0, len(docs))

	sql = "DROP PROPERTY Cat.buddy"
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))

	sql = "DROP PROPERTY Cat.buddies"
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))

	// this also removes the notes from all data entries, but not from the schema
	sql = "UPDATE Cat REMOVE notes"
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))
	numval, err = strconv.ParseInt(retval, 10, 32)
	Ok(err)
	Assert(int(numval) >= 0, "retval from PROPERTY removal should be a positive number")

	// remove notes property from the schema
	sql = "DROP PROPERTY Cat.notes"
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))

	sql = "DROP CLASS Patient"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals("true", retval)
	Equals(0, len(docs))

	// TRUNCATE after drop should return an OServerException type

	sql = "TRUNCATE CLASS Patient"
	ogl.Debugln(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Assert(err != nil, "Error from TRUNCATE should not be null")
	ogl.Println(oerror.GetFullTrace(err))

	err = oerror.ExtractCause(err)
	switch err.(type) {
	case oerror.OServerException:
		ogl.Debugln("type == oerror.OServerException")
	default:
		Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
	}
}

//
// client.go acts as a functional test for the ogonori client
//
func main() {
	var (
		dbc *obinary.DBClient
		err error
	)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	/* ---[ set ogl log level ]--- */
	ogl.SetLevel(ogl.NORMAL)

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
			ogl.Warn(">> >> >> >> PANIC CAUGHT ----> cleanup called") // DEBUG
			cleanUp(dbc, testType == "full")
			os.Exit(1)
		}
	}()

	/* ---[ Use "native" API ]--- */
	createOgonoriTestDB(dbc, adminUser, adminPassw, testType != "dataOnly")
	defer cleanUp(dbc, testType == "full")

	// document database tests
	ogl.SetLevel(ogl.NORMAL)
	dbCommandsNativeAPI(dbc, testType != "dataOnly")
	if testType == "full" {
		ogl.SetLevel(ogl.WARN)
		dbClusterCommandsNativeAPI(dbc)
	}

	/* ---[ Use Go database/sql API on Document DB ]--- */
	ogl.SetLevel(ogl.WARN)
	conxStr := "admin@admin:localhost/ogonoriTest"
	databaseSqlAPI(conxStr)
	databaseSqlPreparedStmtAPI(conxStr)

	/* ---[ Graph DB ]--- */
	// graph database tests
	ogl.SetLevel(ogl.NORMAL)
	graphCommandsNativeAPI(dbc, testType != "dataOnly")

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
