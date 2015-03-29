package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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
		os.Exit(1)
	}
}

func Assert(b bool, msg string) {
	if !b {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31mFAIL: %s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line})...)
		os.Exit(1)
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

func Fatal(err error) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
	os.Exit(1)
}

func createOgonoriTestDB(dbc *obinary.DBClient, adminUser, adminPassw string, outf *os.File, fullTest bool) {
	outf.WriteString("-------- Create OgonoriTest DB --------\n")

	err := obinary.ConnectToServer(dbc, adminUser, adminPassw)
	if err != nil {
		Fatal(err)
	}
	fmt.Fprintf(outf, "ConnectToServer: sessionId: %v\n", dbc.GetSessionId())
	Assert(dbc.GetSessionId() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	mapDBs, err := obinary.RequestDBList(dbc)
	if err != nil {
		Fatal(err)
	}
	gratefulTestPath, ok := mapDBs["GratefulDeadConcerts"]
	Assert(ok, "GratefulDeadConcerts not in DB list")
	Assert(strings.HasPrefix(gratefulTestPath, "plocal"), "plocal prefix for db path")

	// first check if ogonoriTest db exists and if so, drop it
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, constants.Persistent)
	if err != nil {
		Fatal(err)
	}

	if dbexists {
		if !fullTest {
			return
		}

		err = obinary.DropDatabase(dbc, ogonoriDBName, constants.DocumentDb)
		if err != nil {
			Fatal(err)
		}
	}

	// // err = obinary.CreateDatabase(dbc, ogonoriDBName, constants.DocumentDbType, constants.Volatile)
	err = obinary.CreateDatabase(dbc, ogonoriDBName, constants.DocumentDb, constants.Persistent)
	if err != nil {
		Fatal(err)
	}
	dbexists, err = obinary.DatabaseExists(dbc, ogonoriDBName, constants.Persistent)
	if err != nil {
		Fatal(err)
	}
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

func dropOgonoriTestDB(dbc *obinary.DBClient, fullTest bool) {
	if !fullTest {
		return
	}

	// err = obinary.DropDatabase(dbc, ogonoriDBName, constants.Persistent)
	err := obinary.DropDatabase(dbc, ogonoriDBName, constants.DocumentDb)
	if err != nil {
		Fatal(err)
	}
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, constants.Persistent)
	if err != nil {
		Fatal(err)
	}
	Assert(!dbexists, ogonoriDBName+" should not exists after deleting it")
}

func databaseSqlAPI() {
	fmt.Println("\n-------- Using database/sql API --------\n")

	/* ---[ OPEN ]--- */
	db, err := sql.Open("ogonori", "admin@admin:localhost/ogonoriTest")
	if err != nil {
		ogl.Fatale(err)
	}
	defer db.Close()

	/* ---[ DELETE #1 ]--- */
	// should not delete any rows
	delcmd := "delete from Cat where name ='Jared'"
	res, err := db.Exec(delcmd)
	if err != nil {
		ogl.Fatale(err)
	}
	nrows, _ := res.RowsAffected()
	ogl.Printf(">> RES num rows affected: %v\n", nrows)
	Equals(int64(0), nrows)

	/* ---[ INSERT #1 ]--- */
	// insert with no params
	insertSQL := "insert into Cat (name, age, caretaker) values('Jared', 11, 'The Subway Guy')"
	ogl.Println(insertSQL, "=> 'Jared', 11, 'The Subway Guy'")
	res, err = db.Exec(insertSQL)
	if err != nil {
		ogl.Fatale(err)
	}
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
	if err != nil {
		ogl.Fatale(err)
	}
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
	if err != nil {
		ogl.Fatale(err)
	}
	Equals("Filo", retname)
	Equals(int64(4), retage)

	/* ---[ QUERY #2: Query (multiple rows returned) ]--- */

	// NOTE: this fails sporadically because order of fields in the document
	//       is variable due to the unordered map: doc.Fields
	//       we need an ordered data structure do that the order a document
	//       is constructed in is the order of retrieval
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
	if err != nil {
		ogl.Fatale(err)
	}

	Equals(4, len(names))
	Equals(4, len(ctakers))
	Equals(4, len(ages))

	Equals([]string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals([]string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(int64(4), ages[0])
	Equals(int64(10), ages[1])
	Equals(int64(11), ages[2])
	Equals(int64(15), ages[3])

	/* ---[ DELETE #2 ]--- */
	res, err = db.Exec(delcmd)
	if err != nil {
		ogl.Fatale(err)
	}
	nrows, _ = res.RowsAffected()
	ogl.Printf(">> DEL2 RES num rows affected: %v\n", nrows)
	Equals(int64(1), nrows)

	/* ---[ DELETE #3 ]--- */
	res, err = db.Exec(delcmd)
	if err != nil {
		ogl.Fatale(err)
	}
	nrows, _ = res.RowsAffected()
	ogl.Printf(">> DEL3 RES num rows affected: %v\n", nrows)
	Equals(int64(0), nrows)

	/* ---[ DELETE #4 ]--- */
	delcmd = "delete from Cat where name <> 'Linus' AND name <> 'Keiko'"
	res, err = db.Exec(delcmd)
	if err != nil {
		ogl.Fatale(err)
	}
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
	if err != nil {
		ogl.Fatale(err)
	}
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
	if err != nil {
		ogl.Fatale(err)
	}

	Equals(2, len(rowdocs))
	Equals("Cat", rowdocs[0].Classname)
	Equals("Linus", rowdocs[0].GetField("name").Value)
	Equals("Keiko", rowdocs[1].GetField("name").Value)
	Equals("Anna", rowdocs[1].GetField("caretaker").Value)
}

func dbCommandsNativeAPI(dbc *obinary.DBClient, outf *os.File, fullTest bool) {
	outf.WriteString("\n-------- database-level commands --------\n")

	// var sql string

	fmt.Println("OpenDatabase")
	err := obinary.OpenDatabase(dbc, ogonoriDBName, constants.DocumentDb, "admin", "admin")
	if err != nil {
		Fatal(err)
	}
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
	if err != nil {
		Fatal(err)
	}

	cnt2, err := obinary.GetClusterCount(dbc, "default", "index", "ouser")
	if err != nil {
		Fatal(err)
	}
	Assert(cnt1 > 0, "should be clusters")
	Assert(cnt1 >= cnt2, "counts should match or have more deleted")

	begin, end, err := obinary.GetClusterDataRange(dbc, "ouser")
	if err != nil {
		Fatal(err)
	}
	Assert(end >= begin, "begin and end of ClusterDataRange")

	/* ---[ query from the ogonoriTest database ]--- */

	// REDO
	docs, err := obinary.GetRecordByRID(dbc, "12:0", "")
	if err != nil {
		Fatal(err)
	}
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

	sql := "select from Cat where name = 'Linus'"
	fetchPlan := ""
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	if err != nil {
		Fatal(err)
	}

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
	if err != nil {
		Fatal(err)
	}
	outf.WriteString(fmt.Sprintf("ClusterDataRange for cat: %d-%d\n", begin, end))

	fmt.Println("\n\n=+++++++++ START: SQL COMMAND ++++++++++++===")

	sql = "insert into Cat (name, age, caretaker) values(\"Zed\", 3, \"Shaw\")"
	nrows, docs, err := obinary.SQLCommand(dbc, sql)
	if err != nil {
		Fatal(err)
	}
	fmt.Printf("nrows: %v\n", nrows)
	fmt.Printf("docs: %v\n", docs)
	fmt.Println("+++++++++ END: SQL COMMAND ++++++++++++===")

	/* ---[ query after inserting record(s) ]--- */

	sql = "select * from Cat order by name asc"
	fmt.Println("Issuing command query: " + sql)
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	if err != nil {
		Fatal(err)
	}
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
	if err != nil {
		Fatal(err)
	}
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
	if err != nil {
		ogl.Fatale(err)
	}

	// fmt.Println("Deleting (Async) record #11:4")
	// err = obinary.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println("\n\n=+++++++++ START: SQL COMMAND w/ PARAMS ++++++++++++===")

	sql = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	fmt.Println(sql, "=> June", "8", "Cleaver")
	nrows, docs, err = obinary.SQLCommand(dbc, sql, "June", "8", "Cleaver") // TODO: check if numeric types are passed as strings in the Java client
	if err != nil {
		ogl.Fatale(err)
	}
	fmt.Printf("nrows: %v\n", nrows)
	fmt.Printf("docs: %v\n", docs)

	origLevel := ogl.GetLevel()
	ogl.SetLevel(ogl.DEBUG)
	sql = "delete from Cat where name ='June'"
	fmt.Println(sql)
	nrows, docs, err = obinary.SQLCommand(dbc, sql)
	if err != nil {
		Fatal(err)
	}
	fmt.Printf("nrows: %v\n", nrows)
	fmt.Printf("docs: %v\n", docs)
	ogl.SetLevel(origLevel)
	fmt.Println("+++++++++ END: SQL COMMAND w/ PARAMS ++++++++++++===")

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
	if err != nil {
		Fatal(err)
	}
	defer outf.Close()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	/* ---[ set ogl log level ]--- */
	ogl.SetLevel(ogl.NORMAL)

	dbc, err = obinary.NewDBClient(obinary.ClientOptions{})
	if err != nil {
		Fatal(err)
	}
	defer dbc.Close()

	adminUser := "root"
	adminPassw := "jiffylube"
	fullTest := false

	/* ---[ Use "native" API ]--- */
	createOgonoriTestDB(dbc, adminUser, adminPassw, outf, fullTest)
	dbCommandsNativeAPI(dbc, outf, fullTest)

	/* ---[ Use Go database/sql API ]--- */
	databaseSqlAPI()

	dropOgonoriTestDB(dbc, fullTest)

	//
	// Experimenting with JSON functionality
	//
	fmt.Println("-------- JSON ---------")
	fld := oschema.OField{int32(44), "foo", oschema.LONG, int64(33341234)}
	bsjson, err := fld.ToJSON()
	if err != nil {
		ogl.Fatale(err)
	}
	fmt.Printf("%v\n", string(bsjson))

	doc := oschema.NewDocument("Coolio")
	doc.AddField("foo", &fld)
	bsjson, err = doc.ToJSON()
	if err != nil {
		ogl.Fatale(err)
	}
	fmt.Printf("%v\n", string(bsjson))

	fmt.Println("DONE")
}
