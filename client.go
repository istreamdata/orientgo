package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/quux00/ogonori/obinary"
	"github.com/quux00/ogonori/oschema"
)

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
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, obinary.PersistentStorageType)
	if err != nil {
		Fatal(err)
	}

	if dbexists {
		if !fullTest {
			return
		}

		err = obinary.DropDatabase(dbc, ogonoriDBName, obinary.DocumentDbType)
		if err != nil {
			Fatal(err)
		}
	}

	// // err = obinary.CreateDatabase(dbc, ogonoriDBName, obinary.DocumentDbType, obinary.VolatileStorageType)
	err = obinary.CreateDatabase(dbc, ogonoriDBName, obinary.DocumentDbType, obinary.PersistentStorageType)
	if err != nil {
		Fatal(err)
	}
	dbexists, err = obinary.DatabaseExists(dbc, ogonoriDBName, obinary.PersistentStorageType)
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

	// err = obinary.DropDatabase(dbc, ogonoriDBName, obinary.PersistentStorageType)
	err := obinary.DropDatabase(dbc, ogonoriDBName, obinary.DocumentDbType)
	if err != nil {
		Fatal(err)
	}
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, obinary.PersistentStorageType)
	if err != nil {
		Fatal(err)
	}
	Assert(!dbexists, ogonoriDBName+" should not exists after deleting it")
}

func dbCommands(dbc *obinary.DBClient, outf *os.File, fullTest bool) {
	outf.WriteString("\n-------- database-level commands --------\n")

	// var sql string

	fmt.Println("OpenDatabase")
	err := obinary.OpenDatabase(dbc, ogonoriDBName, obinary.DocumentDbType, "admin", "admin")
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
	Equals(2, len(doc12_0.Fields))
	Equals("Cat", doc12_0.Classname)

	nameField, ok := doc12_0.Fields["name"]
	Assert(ok, "should be a 'name' field")

	caretakerField, ok := doc12_0.Fields["caretaker"]
	Assert(ok, "should be a 'caretaker' field")

	Assert(nameField.Id != caretakerField.Id, "Ids should not match")
	Equals(byte(oschema.STRING), nameField.Typ)
	Equals(byte(oschema.STRING), caretakerField.Typ)
	Equals("Linus", nameField.Value)
	Equals("Michael", caretakerField.Value)

	fmt.Printf("docs returned by RID: %v\n", *(docs[0]))

	// fmt.Println("Deleting (sync) record #11:3")
	// err = obinary.DeleteRecordByRID(dbc, "11:3", 3)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("Deleting (Async) record #11:4")
	// err = obinary.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	sql := "select * from Cat where name = 'Linus'"
	docs, err = obinary.SQLQuery(dbc, sql)
	if err != nil {
		Fatal(err)
	}

	Equals("12:0", docs[0].Rid)
	Assert(docs[0].Version > 0, fmt.Sprintf("Version is: %d", docs[0].Version))
	Equals(2, len(docs[0].Fields))
	Equals("Cat", docs[0].Classname)

	nameField, ok = docs[0].Fields["name"]
	Assert(ok, "should be a 'name' field")

	caretakerField, ok = docs[0].Fields["caretaker"]
	Assert(ok, "should be a 'caretaker' field")

	Assert(nameField.Id != caretakerField.Id, "Ids should not match")
	Equals(byte(oschema.STRING), nameField.Typ)
	Equals(byte(oschema.STRING), caretakerField.Typ)
	Equals("Linus", nameField.Value)
	Equals("Michael", caretakerField.Value)

	sql = "select * from Cat order by name desc"
	fmt.Println("Issuing command query: " + sql)
	docs, err = obinary.SQLQuery(dbc, sql)
	if err != nil {
		Fatal(err)
	}
	Equals(2, len(docs))
	Equals(2, len(docs[0].Fields))
	Equals("Cat", docs[0].Classname)
	Equals(2, len(docs[1].Fields))
	Equals("Cat", docs[1].Classname)

	linus := docs[0]
	Equals("Linus", linus.Fields["name"].Value)
	Equals("Michael", linus.Fields["caretaker"].Value)

	keiko := docs[1]
	Equals("Keiko", keiko.Fields["name"].Value)
	Equals("Anna", keiko.Fields["caretaker"].Value)
	Equals(byte(oschema.STRING), keiko.Fields["caretaker"].Typ)
	Assert(keiko.Version > int32(0), "Version should be greater than zero")
	Assert(keiko.Rid != "", "RID should not be empty")

	sql = "select name, caretaker from Cat order by caretaker"
	docs, err = obinary.SQLQuery(dbc, sql)
	if err != nil {
		Fatal(err)
	}
	Equals(2, len(docs))
	Equals(2, len(docs[0].Fields))
	Equals("", docs[0].Classname) // property queries do not come back with Classname set
	Equals(2, len(docs[1].Fields))
	Equals("", docs[1].Classname)

	Equals("Anna", docs[0].Fields["caretaker"].Value)
	Equals("Michael", docs[1].Fields["caretaker"].Value)

	Equals("Keiko", docs[0].Fields["name"].Value)
	Equals("Linus", docs[1].Fields["name"].Value)

	Equals("name", docs[0].Fields["name"].Name)

	fmt.Println("\n\n=+++++++++++++++++++++===")

	/* ---[ cluster data range ]--- */
	begin, end, err = obinary.GetClusterDataRange(dbc, "cat")
	if err != nil {
		Fatal(err)
	}
	outf.WriteString(fmt.Sprintf("ClusterDataRange for cat: %d-%d\n", begin, end))

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
		log.Fatalf("ERROR: %v\n", err)
	}
	defer outf.Close()

	dbc, err = obinary.NewDBClient(obinary.ClientOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer dbc.Close()

	adminUser := "root"
	adminPassw := "jiffylube"
	fullTest := false

	createOgonoriTestDB(dbc, adminUser, adminPassw, outf, fullTest)
	dbCommands(dbc, outf, fullTest)
	dropOgonoriTestDB(dbc, fullTest)

	fmt.Println("DONE")
}
