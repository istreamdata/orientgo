package obinary_test

import (
	"github.com/golang/glog"
	"runtime/debug"
	"testing"
)

func catch() {
	if r := recover(); r != nil {
		glog.Errorf("panic recovery: %v\nTrace:\n%s\n", r, debug.Stack())
	}
}
func notShort(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
}

func TestInitialize(t *testing.T) {
	notShort(t)
	dbc, closer := SpinOrient(t)
	defer closer()
	defer catch()
	createOgonoriTestDB(t, dbc, dbUser, dbPass)
}

func TestCommandsNativeAPI(t *testing.T) {
	notShort(t)
	dbc, closer := SpinOrient(t)
	defer closer()
	defer catch()

	createOgonoriTestDB(t, dbc, dbUser, dbPass)
	dbCommandsNativeAPI(t, dbc)

	// document database tests
	//	dbCommandsNativeAPI(dbc, testType != "dataOnly")
	//	if testType == "full" {
	//		dbClusterCommandsNativeAPI(dbc)
	//	}

	// create new records from low-level create API (not SQL)
	//	createAndUpdateRecordsViaNativeAPI(dbc)

	/* ---[ Use Go database/sql API on Document DB ]--- */
	//	conxStr := "admin@admin:localhost/" + dbDocumentName
	//	databaseSQLAPI(conxStr)
	//	databaseSQLPreparedStmtAPI(conxStr)

	/* ---[ Graph DB ]--- */
	// graph database tests
	//	graphCommandsNativeAPI(dbc, testType != "dataOnly")
	//	graphConxStr := "admin@admin:localhost/" + dbGraphName
	//	graphCommandsSQLAPI(graphConxStr)

	// ------

	//
	// experimenting with JSON functionality
	//
	// ogl.Println("-------- JSON ---------")
	// fld := oschema.OField{int32(44), "foo", oschema.LONG, int64(33341234)}
	// bsjson, err := fld.ToJSON()
	// assert.Nil(t, err)
	// ogl.Printf("%v\n", string(bsjson))

	// doc := oschema.NewDocument("Coolio")
	// doc.AddField("foo", &fld)
	// bsjson, err = doc.ToJSON()
	// assert.Nil(t, err)
	// ogl.Printf("%v\n", string(bsjson))
}
