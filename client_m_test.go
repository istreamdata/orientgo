package orient_test

import (
	//	"database/sql"
	//	"log"
	//	"os"
	"path/filepath"
	"reflect"
	"runtime"
	//	"runtime/debug"
	//	"runtime/pprof"
	//	"strconv"
	"fmt"

	//	"net/http"
	_ "net/http/pprof"

	"github.com/golang/glog"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/oschema"
	"github.com/stretchr/testify/assert"
	"runtime/debug"
	"testing"
)

// Flags - specify these on the cmd line to change from the defaults
const (
	dbDocumentName = "ogonoriTest"
	dbGraphName    = "ogonoriGraphTest"
)

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
		t.Fatalf("%s:%d\n\n\texp: %+v\n\n\tgot: %+v\n\n", filepath.Base(file), line, exp, act)
	}
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
	sess, err := dbc.Auth(srvUser, srvPass)
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

func removeProperty(db *orient.Database, class, property string) {
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
