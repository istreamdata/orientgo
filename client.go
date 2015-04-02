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
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
	_ "github.com/quux00/ogonori/osql"
)

//
// This is a "functional" tester class against a live OrientDB 2.x I'm using
// while developing the ogonori OrientDB Go client.
//
// Before running this test, you need to run the scripts/ogonori-setup.sql
// with the `console.sh` program of OrientDB:
//   ./console.sh ogonori-setup.sql
//

var ogonoriDBName string = "ogonoriTest"

func Equals(exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		panic("Equals fail")
	}
}

func Ok(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line})...)
		panic("Ok fail")
	}
}

func Assert(b bool, msg string) {
	if !b {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31mFAIL: %s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line})...)
		panic("Assert fail")
	}
}

func Pause(msg string) {
	fmt.Print(msg, "[Press Enter to Continue]: ")
	var s string
	_, err := fmt.Scan(&s)
	if err != nil {
		ogl.Fatale(err)
	}
}

// func Fatal(err error) {
// 	_, file, line, _ := runtime.Caller(1)
// 	fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
// 		append([]interface{}{filepath.Base(file), line})...)
// 	os.Exit(1)
// }

func createOgonoriTestDB(dbc *obinary.DBClient, adminUser, adminPassw string, outf *os.File, fullTest bool) {
	outf.WriteString("-------- Create OgonoriTest DB --------\n")

	err := obinary.ConnectToServer(dbc, adminUser, adminPassw)
	Ok(err)

	fmt.Fprintf(outf, "ConnectToServer: sessionId: %v\n", dbc.GetSessionId())
	Assert(dbc.GetSessionId() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	mapDBs, err := obinary.RequestDBList(dbc)
	Ok(err)
	gratefulTestPath, ok := mapDBs["GratefulDeadConcerts"]
	Assert(ok, "GratefulDeadConcerts not in DB list")
	Assert(strings.HasPrefix(gratefulTestPath, "plocal"), "plocal prefix for db path")

	// first check if ogonoriTest db exists and if so, drop it
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, constants.Persistent)
	Ok(err)

	if dbexists {
		if !fullTest {
			return
		}

		err = obinary.DropDatabase(dbc, ogonoriDBName, constants.DocumentDb)
		Ok(err)
	}

	// // err = obinary.CreateDatabase(dbc, ogonoriDBName, constants.DocumentDbType, constants.Volatile)
	err = obinary.CreateDatabase(dbc, ogonoriDBName, constants.DocumentDb, constants.Persistent)
	Ok(err)
	dbexists, err = obinary.DatabaseExists(dbc, ogonoriDBName, constants.Persistent)
	Ok(err)
	Assert(dbexists, ogonoriDBName+" should now exists after creating it")

	// BUG in OrientDB 2.0.1? :
	//  ERROR: com.orientechnologies.orient.core.exception.ODatabaseException Database 'plocal:/home/midpeter444/apps/orientdb-community-2.0.1/databases/ogonoriTest' is closed}
	// mapDBs, err = obinary.RequestDBList(dbc)
	// if err != nil {
	// 	Fatal(err)
	// }
	// fmt.Printf("%v\n", mapDBs)
	// ogonoriTestPath, ok := mapDBs[ogonoriDBName]
	// Assert(ok, ogonoriDBName+" not in DB list")
	// Assert(strings.HasPrefix(ogonoriTestPath, "plocal"), "plocal prefix for db path")
	// fmt.Fprintf(outf, "DB list: ogonoriTest: %v\n", ogonoriTestPath)
}

func deleteNewRecords(dbc *obinary.DBClient) {
	err := obinary.OpenDatabase(dbc, ogonoriDBName, constants.DocumentDb, "admin", "admin")
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
	_, _, err = obinary.SQLCommand(dbc, "delete from Cat where name <> 'Linus' AND name <> 'Keiko'")
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
	err = obinary.CloseDatabase(dbc)
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
}

func cleanUp(dbc *obinary.DBClient, fullTest bool) {
	if !fullTest {
		deleteNewRecords(dbc)
		return
	}

	// err = obinary.DropDatabase(dbc, ogonoriDBName, constants.Persistent)
	err := obinary.DropDatabase(dbc, ogonoriDBName, constants.DocumentDb)
	Ok(err)
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, constants.Persistent)
	if err != nil {
		ogl.Warn(err.Error())
		return
	}
	Assert(!dbexists, ogonoriDBName+" should not exists after deleting it")
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
		ogl.Fatale(err)
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
	fmt.Println("\n-------- Using database/sql PreparedStatement API --------\n")

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
		ogl.Fatale(err)
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
		ogl.Fatale(err)
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
		ogl.Fatale(err)
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

func dbCommandsNativeAPI(dbc *obinary.DBClient, outf *os.File, fullTest bool) {
	outf.WriteString("\n-------- database-level commands --------\n")

	var sql string
	var retval string

	fmt.Println("OpenDatabase")
	err := obinary.OpenDatabase(dbc, ogonoriDBName, constants.DocumentDb, "admin", "admin")
	Ok(err)
	// fmt.Printf("%v\n", dbc) // DEBUG

	// clusterId, err := obinary.AddCluster(dbc, "bigapple")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("bigapple cluster added => clusterId in `cars`: %v\n", clusterId)

	// cnt, err := obinary.GetClusterCount(dbc, "bigapple")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("bigapple cluster count = %d\n", cnt)

	// for _, name := range []string{"bigApple"} {
	// 	err = obinary.DropCluster(dbc, name)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Printf("cluster %v dropped successfully\n", name)
	// }

	cnt1, err := obinary.GetClusterCountIncludingDeleted(dbc, "default", "index", "ouser")
	Ok(err)

	cnt2, err := obinary.GetClusterCount(dbc, "default", "index", "ouser")
	Ok(err)
	Assert(cnt1 > 0, "should be clusters")
	Assert(cnt1 >= cnt2, "counts should match or have more deleted")

	begin, end, err := obinary.GetClusterDataRange(dbc, "ouser")
	Ok(err)
	Assert(end >= begin, "begin and end of ClusterDataRange")

	/* ---[ query from the ogonoriTest database ]--- */

	// REDO
	docs, err := obinary.GetRecordByRID(dbc, "12:0", "")
	Ok(err)
	Equals(1, len(docs))
	doc12_0 := docs[0]
	Equals("12:0", doc12_0.Rid)
	Assert(doc12_0.Version > 0, fmt.Sprintf("Version is: %d", doc12_0.Version))
	Equals(3, len(doc12_0.FieldNames()))
	Equals("Cat", doc12_0.Classname)

	nameField := doc12_0.GetField("name")
	Assert(nameField != nil, "should be a 'name' field")

	ageField := doc12_0.GetField("age")
	Assert(ageField != nil, "should be a 'age' field")

	caretakerField := doc12_0.GetField("caretaker")
	Assert(caretakerField != nil, "should be a 'caretaker' field")

	Assert(nameField.Id != caretakerField.Id, "Ids should not match")
	Equals(byte(oschema.STRING), nameField.Typ)
	Equals(byte(oschema.INTEGER), ageField.Typ)
	Equals(byte(oschema.STRING), caretakerField.Typ)
	Equals("Linus", nameField.Value)
	Equals(int32(15), ageField.Value)
	Equals("Michael", caretakerField.Value)

	fmt.Printf("docs returned by RID: %v\n", *(docs[0]))

	sql = "select from Cat where name = 'Linus'"
	fetchPlan := ""
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	Ok(err)

	Equals("12:0", docs[0].Rid)
	Assert(docs[0].Version > 0, fmt.Sprintf("Version is: %d", docs[0].Version))
	Equals(3, len(docs[0].FieldNames()))
	Equals("Cat", docs[0].Classname)

	nameField = docs[0].GetField("name")
	Assert(nameField != nil, "should be a 'name' field")

	ageField = doc12_0.GetField("age")
	Assert(ageField != nil, "should be a 'age' field")

	caretakerField = docs[0].GetField("caretaker")
	Assert(caretakerField != nil, "should be a 'caretaker' field")

	Assert(nameField.Id != caretakerField.Id, "Ids should not match")
	Equals(byte(oschema.STRING), nameField.Typ)
	Equals(byte(oschema.STRING), caretakerField.Typ)
	Equals(byte(oschema.INTEGER), ageField.Typ)
	Equals("Linus", nameField.Value)
	Equals(int32(15), ageField.Value)
	Equals("Michael", caretakerField.Value)

	/* ---[ cluster data range ]--- */
	begin, end, err = obinary.GetClusterDataRange(dbc, "cat")
	Ok(err)
	outf.WriteString(fmt.Sprintf("ClusterDataRange for cat: %d-%d\n", begin, end))

	fmt.Println("\n\n=+++++++++ START: SQL COMMAND ++++++++++++===")

	sql = "insert into Cat (name, age, caretaker) values(\"Zed\", 3, \"Shaw\")"
	nrows, docs, err := obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("nrows: %v\n", nrows)
	fmt.Printf("docs: %v\n", docs)
	fmt.Println("+++++++++ END: SQL COMMAND ++++++++++++===")

	/* ---[ query after inserting record(s) ]--- */

	sql = "select * from Cat order by name asc"
	fmt.Println("Issuing command query: " + sql)
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
	fmt.Println("Deleting (sync) record #" + zed.Rid)
	err = obinary.DeleteRecordByRID(dbc, zed.Rid, zed.Version)
	Ok(err)

	// fmt.Println("Deleting (Async) record #11:4")
	// err = obinary.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println("\n\n=+++++++++ START: SQL COMMAND w/ PARAMS ++++++++++++===")

	sql = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	fmt.Println(sql, "=> June", "8", "Cleaver")
	nrows, docs, err = obinary.SQLCommand(dbc, sql, "June", "8", "Cleaver") // TODO: check if numeric types are passed as strings in the Java client
	Ok(err)
	fmt.Printf("nrows: %v\n", nrows)
	fmt.Printf("docs: %v\n", docs)

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
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)
	fmt.Println("+++++++++ END: SQL COMMAND w/ PARAMS ++++++++++++===")

	fmt.Println("+++++++++ START: Basic DDL ++++++++++++===")

	sql = "DROP CLASS Patient"
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))
	if retval != "" {
		Equals("true", retval)
	}

	sql = "CREATE CLASS Patient"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(0, len(docs))
	ncls, err := strconv.ParseInt(retval, 10, 64)
	Ok(err)
	Assert(ncls > 10, "classnum should be greater than 10 but was: "+retval)

	sql = "Create property Patient.name string"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", nrows)
	fmt.Printf("docs: %v\n", docs)

	sql = "alter property Patient.name min 3"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sql = "Create property Patient.married boolean"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true')"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sql = "TRUNCATE CLASS Patient"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true'), ('Martha', 'false')"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	sql = "SELECT count(*) from Patient"
	fmt.Println(sql)
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(1, len(docs))
	fldCount := docs[0].GetField("count")
	Equals(int64(2), fldCount.Value)

	sql = "DROP CLASS Patient"
	fmt.Println(sql)
	retval, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	fmt.Printf("retval: %v\n", retval)
	fmt.Printf("docs: %v\n", docs)

	// TODO: check that this one returns a server exception
	// sql = "TRUNCATE CLASS Patient"
	// fmt.Println(sql)
	// retval, docs, err = obinary.SQLCommand(dbc, sql)
	// Ok(err)
	// fmt.Printf("retval: %v\n", retval)
	// fmt.Printf("docs: %v\n", docs)

	obinary.CloseDatabase(dbc)
}

//
// client.go acts as a functional test for the ogonori client
//
func main() {
	var (
		dbc  *obinary.DBClient
		err  error
		outf *os.File
	)
	outf, err = os.Create("./ftest.out")
	Ok(err)
	defer outf.Close()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	/* ---[ set ogl log level ]--- */
	ogl.SetLevel(ogl.NORMAL)

	adminUser := "root"
	adminPassw := "jiffylube"
	fullTest := false

	dbc, err = obinary.NewDBClient(obinary.ClientOptions{})
	Ok(err)
	defer dbc.Close()

	/* ---[ set up clean up in case of panics ]--- */
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(">> >> >> >> PANIC CAUGHT ----> cleanup called") // DEBUG
			cleanUp(dbc, fullTest)
			os.Exit(1)
		}
	}()

	/* ---[ Use "native" API ]--- */
	createOgonoriTestDB(dbc, adminUser, adminPassw, outf, fullTest)
	defer cleanUp(dbc, fullTest)

	dbCommandsNativeAPI(dbc, outf, fullTest)

	/* ---[ Use Go database/sql API ]--- */
	conxStr := "admin@admin:localhost/ogonoriTest"
	databaseSqlAPI(conxStr)
	databaseSqlPreparedStmtAPI(conxStr)

	//
	// Experimenting with JSON functionality
	//
	fmt.Println("-------- JSON ---------")
	fld := oschema.OField{int32(44), "foo", oschema.LONG, int64(33341234)}
	bsjson, err := fld.ToJSON()
	Ok(err)
	fmt.Printf("%v\n", string(bsjson))

	doc := oschema.NewDocument("Coolio")
	doc.AddField("foo", &fld)
	bsjson, err = doc.ToJSON()
	Ok(err)
	fmt.Printf("%v\n", string(bsjson))

	fmt.Println("DONE")
}
