package main

import (
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"testing"

	"github.com/quux00/ogonori/obinary"
	"github.com/quux00/ogonori/ogl"
)

func TestAgainstOrientDBServer(t *testing.T) {
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
	createRecordsViaNativeAPI(dbc)

	/* ---[ Use Go database/sql API on Document DB ]--- */
	ogl.SetLevel(ogl.WARN)
	conxStr := "admin@admin:localhost/" + dbDocumentName
	databaseSqlAPI(conxStr)
	databaseSqlPreparedStmtAPI(conxStr)

	/* ---[ Graph DB ]--- */
	// graph database tests
	ogl.SetLevel(ogl.WARN)
	graphCommandsNativeAPI(dbc, testType != "dataOnly")
	graphConxStr := "admin@admin:localhost/" + dbGraphName
	ogl.SetLevel(ogl.NORMAL)
	graphCommandsSqlAPI(graphConxStr)

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
