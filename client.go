package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/quux00/ogonori/obinary"
)

var ogonoriDBName string = "ogonoriTest"

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

func createOgonoriTestDB(dbc *obinary.DBClient, adminUser, adminPassw string, outf *os.File) {
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
	fmt.Printf("%v\n", mapDBs)

	// first check if ogonoriTest db exists and if so, drop it
	dbexists, err := obinary.DatabaseExists(dbc, ogonoriDBName, obinary.PersistentStorageType)
	if err != nil {
		Fatal(err)
	}

	if dbexists {
		fmt.Println("ogonoriTest already existed - so dropping")
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

func dropOgonoriTestDB(dbc *obinary.DBClient) {
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

func dbCommands(dbc *obinary.DBClient) {
	fmt.Println("\n-------- database-level commands --------")

	// var sql string

	fmt.Println("OpenDatabase")
	err := obinary.OpenDatabase(dbc, "cars", obinary.DocumentDbType, "admin", "admin")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", dbc) // DEBUG

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

	// cnt, err = obinary.GetClusterCountIncludingDeleted(dbc, "person", "v", "ouser")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("ClusterCount w/ deleted: %v\n", cnt)

	// cnt, err = obinary.GetClusterCount(dbc, "person", "v", "ouser")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("ClusterCount w/o deleted: %v\n", cnt)

	// begin, end, err := obinary.GetClusterDataRange(dbc, "ouser")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("ClusterDataRange for ouser: %d-%d\n", begin, end)

	// fmt.Printf("\n+++ Attempting to fetch record now +++\n cmd num = %v\n", obinary.REQUEST_RECORD_LOAD)
	// docs, err := obinary.GetRecordByRID(dbc, "11:0", "")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("docs returned by RID: %v\n", *(docs[0]))

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

	// sql := "select * from Person where name = 'Luke'"
	// fmt.Println("Issuing command query: " + sql)
	// err = obinary.SQLQuery(dbc, sql)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "PERSON: WARN: %v\n", err)
	// }

	// begin, end, err := obinary.GetClusterDataRange(dbc, "ouser")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("ClusterDataRange for ouser: %d-%d\n", begin, end)

	// fmt.Println("=+++++++++++++++++++++===")

	// sql = "select * from Carz"
	// fmt.Println("Issuing command query: " + sql)
	// err = obinary.SQLQuery(dbc, sql)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "FOO: WARN: %v\n", err)
	// }

	// fmt.Println("\n\n=+++++++++++++++++++++===")

	// sql = "select model, make from Carz"
	// fmt.Println("Issuing command query: " + sql)
	// err = obinary.SQLQuery(dbc, sql)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "MK: WARN: %v\n", err)
	// }

	fmt.Println("\n\n=+++++++++++++++++++++===")
	// GetRecordByRID(dbc *DBClient, rid string, fetchPlan string) ([]*oschema.ODocument, error) {

	// sql = "#0:1"

	// docs, err := obinary.GetRecordByRID(dbc, sql, "")
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
	// }
	// fmt.Println("=======================================\n=======================================\n=======================================")
	// fmt.Printf("len(docs):: %v\n", len(docs))
	// doc0 := docs[0]
	// fmt.Printf("len(doc0.Fields):: %v\n", len(doc0.Fields))
	// fmt.Println("Field names:")
	// for k, _ := range doc0.Fields {
	// 	fmt.Printf("  %v\n", k)
	// }
	// schemaVersion := doc0.Fields["schemaVersion"]
	// fmt.Printf("%v\n", schemaVersion)

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
	outf, err = os.Create("./ogonori.ftest.out")
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
	createOgonoriTestDB(dbc, adminUser, adminPassw, outf)
	// dbCommands(dbc)
	dropOgonoriTestDB(dbc)

	fmt.Println("DONE")
}
