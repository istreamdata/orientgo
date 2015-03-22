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
	db, err := sql.Open("ogonori", "admin@admin:localhost/ogonoriTest")
	if err != nil {
		Fatal(err)
	}
	defer db.Close()

	sqlcmd := "delete from Cat where name ='Jared'"
	res, err := db.Exec(sqlcmd)
	if err != nil {
		Fatal(err)
	}
	nrows, _ := res.RowsAffected()
	fmt.Printf(">> RES num rows affected: %v\n", nrows)
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
	doc12_0 := docs[0]
	Equals("12:0", doc12_0.Rid)
	Assert(doc12_0.Version > 0, fmt.Sprintf("Version is: %d", doc12_0.Version))
	Equals(3, len(doc12_0.Fields))
	Equals("Cat", doc12_0.Classname)

	nameField, ok := doc12_0.Fields["name"]
	Assert(ok, "should be a 'name' field")

	ageField, ok := doc12_0.Fields["age"]
	Assert(ok, "should be a 'age' field")

	caretakerField, ok := doc12_0.Fields["caretaker"]
	Assert(ok, "should be a 'caretaker' field")

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
	Equals(3, len(docs[0].Fields))
	Equals("Cat", docs[0].Classname)

	nameField, ok = docs[0].Fields["name"]
	Assert(ok, "should be a 'name' field")

	ageField, ok = doc12_0.Fields["age"]
	Assert(ok, "should be a 'age' field")

	caretakerField, ok = docs[0].Fields["caretaker"]
	Assert(ok, "should be a 'caretaker' field")

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
	err = obinary.SQLCommand(dbc, sql)
	if err != nil {
		Fatal(err)
	}
	fmt.Println("+++++++++ END: SQL COMMAND ++++++++++++===")

	/* ---[ query after inserting record(s) ]--- */

	sql = "select * from Cat order by name asc"
	fmt.Println("Issuing command query: " + sql)
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	if err != nil {
		Fatal(err)
	}
	Equals(3, len(docs))
	Equals(3, len(docs[0].Fields))
	Equals("Cat", docs[0].Classname)
	Equals(3, len(docs[1].Fields))
	Equals("Cat", docs[1].Classname)
	Equals(3, len(docs[2].Fields))
	Equals("Cat", docs[2].Classname)

	keiko := docs[0]
	Equals("Keiko", keiko.Fields["name"].Value)
	Equals(int32(10), keiko.Fields["age"].Value)
	Equals("Anna", keiko.Fields["caretaker"].Value)
	Equals(byte(oschema.STRING), keiko.Fields["caretaker"].Typ)
	Assert(keiko.Version > int32(0), "Version should be greater than zero")
	Assert(keiko.Rid != "", "RID should not be empty")

	linus := docs[1]
	Equals("Linus", linus.Fields["name"].Value)
	Equals(int32(15), linus.Fields["age"].Value)
	Equals("Michael", linus.Fields["caretaker"].Value)

	zed := docs[2]
	Equals("Zed", zed.Fields["name"].Value)
	Equals(int32(3), zed.Fields["age"].Value)
	Equals("Shaw", zed.Fields["caretaker"].Value)
	Equals(byte(oschema.STRING), zed.Fields["caretaker"].Typ)
	Equals(byte(oschema.INTEGER), zed.Fields["age"].Typ)
	Assert(zed.Version > int32(0), "Version should be greater than zero")
	Assert(zed.Rid != "", "RID should not be empty")

	sql = "select name, caretaker from Cat order by caretaker"
	docs, err = obinary.SQLQuery(dbc, sql, fetchPlan)
	if err != nil {
		Fatal(err)
	}
	Equals(3, len(docs))
	Equals(2, len(docs[0].Fields))
	Equals("", docs[0].Classname) // property queries do not come back with Classname set
	Equals(2, len(docs[1].Fields))
	Equals("", docs[1].Classname)
	Equals(2, len(docs[2].Fields))

	Equals("Anna", docs[0].Fields["caretaker"].Value)
	Equals("Michael", docs[1].Fields["caretaker"].Value)
	Equals("Shaw", docs[2].Fields["caretaker"].Value)

	Equals("Keiko", docs[0].Fields["name"].Value)
	Equals("Linus", docs[1].Fields["name"].Value)
	Equals("Zed", docs[2].Fields["name"].Value)

	Equals("name", docs[0].Fields["name"].Name)

	/* ---[ delete newly added record(s) ]--- */
	fmt.Println("Deleting (sync) record #" + zed.Rid)
	err = obinary.DeleteRecordByRID(dbc, zed.Rid, zed.Version)
	if err != nil {
		Fatal(err)
	}

	// fmt.Println("Deleting (Async) record #11:4")
	// err = obinary.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println("\n\n=+++++++++ START: SQL COMMAND w/ PARAMS ++++++++++++===")

	sql = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	err = obinary.SQLCommand(dbc, sql, "June", "8", "Cleaver") // TODO: check if numeric types are passed as strings in the Java client
	if err != nil {
		Fatal(err)
	}
	fmt.Println("+++++++++ END: SQL COMMAND w/ PARAMS ++++++++++++===")

	// sql = "select from  Cat where name = ? and caretaker = ?"
	// err = obinary.SQLQuery(dbc, sql, "June", "Cleaver")
	// if err != nil {
	// 	Fatal(err)
	// }
	// fmt.Println("+++++++++ END: SQL QUERY w/ PARAMS ++++++++++++===")

	sql = "delete from Cat where name ='June'"
	err = obinary.SQLCommand(dbc, sql)
	if err != nil {
		Fatal(err)
	}

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

	fmt.Println("DONE")
}
