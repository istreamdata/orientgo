package orient_test

import (
	//	"database/sql"
	//	"os"
	"path/filepath"
	"reflect"
	"runtime"
	//	"strconv"
	"fmt"
	"log"
	_ "net/http/pprof"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/istreamdata/orientgo.v2"
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

/*
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
*/
func dropDatabase(t *testing.T, dbc orient.Client, dbname string, dbtype orient.StorageType) {
	//_ = dbc.Close()
	sess, err := dbc.Auth(srvUser, srvPass)
	Nil(t, err)

	err = sess.DropDatabase(dbname, dbtype)
	Nil(t, err)
	dbexists, err := sess.DatabaseExists(dbname, orient.Persistent)
	if err != nil {
		log.Println(err.Error())
		return
	}
	if dbexists {
		log.Printf("ERROR: Deletion of database %s failed\n", dbname)
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

	cat1 := orient.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 1).
		Field("caretaker", "Jackie")

	err = dbc.CreateRecord(dbc, cat1)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	linkToCat1 := &orient.OLink{RID: cat1.RID, Record: cat1}
	linkmap := map[string]*orient.OLink{"bff": linkToCat1}

	cat2 := orient.NewDocument("Cat")
	cat2.Field("name", "A2").
		Field("age", 2).
		Field("caretaker", "Ben").
		FieldWithType("notes", linkmap, orient.LINKMAP)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	linkmap["7th-best-friend"] = &orient.OLink{RID: cat2.RID}

	cat3 := orient.NewDocument("Cat")
	cat3.Field("name", "A3").
		Field("age", 3).
		Field("caretaker", "Konrad").
		FieldWithType("notes", linkmap, orient.LINKMAP)

	err = dbc.CreateRecord(dbc, cat3)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat3.RID.String())

	docs, err := db.SQLQuery(dbc, "select * from Cat where name='A2' OR name='A3' ORDER BY name", "")
	Nil(t, err)
	Equals(t, 2, len(docs))

	cat2FromQuery := docs[0]
	Equals(t, "A2", cat2FromQuery.GetField("name").Value)
	Equals(t, 2, toInt(cat2FromQuery.GetField("age").Value))
	notesFromQuery := cat2FromQuery.GetField("notes").Value.(map[string]*orient.OLink)
	Equals(t, 1, len(notesFromQuery))
	Equals(t, notesFromQuery["bff"].RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 3, toInt(cat3FromQuery.GetField("age").Value))
	notesFromQuery = cat3FromQuery.GetField("notes").Value.(map[string]*orient.OLink)
	Equals(t, 2, len(notesFromQuery))
	Equals(t, notesFromQuery["bff"].RID, cat1.RID)
	Equals(t, notesFromQuery["7th-best-friend"].RID, cat2.RID)

	///////////////////////

	// ---[ update ]---

	versionBefore := cat3.Version

	// add to cat3's linkmap

	cat3map := cat3.GetField("notes").Value.(map[string]*orient.OLink)
	cat3map["new1"] = &orient.OLink{RID: cat2.RID}
	cat3map["new2"] = &orient.OLink{RID: cat2.RID}

	err = dbc.UpdateRecord(dbc, cat3) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat3.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat3.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat3FromQuery = docs[0]

	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	cat3MapFromQuery := cat3FromQuery.GetField("notes").Value.(map[string]*orient.OLink)
	Equals(t, 4, len(cat3MapFromQuery))
	Equals(t, cat3MapFromQuery["bff"].RID, cat1.RID)
	Equals(t, cat3MapFromQuery["7th-best-friend"].RID, cat2.RID)
	Equals(t, cat3MapFromQuery["new1"].RID, cat2.RID)
	Equals(t, cat3MapFromQuery["new2"].RID, cat2.RID)
}

func createAndUpdateRecordsWithLinkLists(dbc orient.Client, collType orient.OType) {
	sql := "CREATE PROPERTY Cat.catfriends " + orient.ODataTypeNameFor(collType) + " Cat"
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

	cat1 := orient.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 1).
		Field("caretaker", "Jackie")

	err = dbc.CreateRecord(dbc, cat1)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	linkToCat1 := &orient.OLink{RID: cat1.RID, Record: cat1}

	cat2 := orient.NewDocument("Cat")
	cat2.Field("name", "A2").
		Field("age", 2).
		Field("caretaker", "Ben").
		FieldWithType("catfriends", []*orient.OLink{linkToCat1}, collType)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	linkToCat2 := &orient.OLink{RID: cat2.RID}
	twoCatLinks := []*orient.OLink{linkToCat1, linkToCat2}

	cat3 := orient.NewDocument("Cat")
	cat3.Field("name", "A3")

	if collType == orient.LINKSET {
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
	catFriendsFromQuery := cat2FromQuery.GetField("catfriends").Value.([]*orient.OLink)
	Equals(t, 1, len(catFriendsFromQuery))
	Equals(t, catFriendsFromQuery[0].RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 3, toInt(cat3FromQuery.GetField("age").Value))
	catFriendsFromQuery = cat3FromQuery.GetField("catfriends").Value.([]*orient.OLink)
	Equals(t, 2, len(catFriendsFromQuery))
	sort.Sort(byRID(catFriendsFromQuery))
	Equals(t, catFriendsFromQuery[0].RID, cat1.RID)
	Equals(t, catFriendsFromQuery[1].RID, cat2.RID)

	// ---[ update ]---

	versionBefore := cat3.Version

	// cat2 ("A2") currently has linklist to cat1 ("A2")
	// -> change this to a linklist to cat1 and cat3

	linkToCat3 := &orient.OLink{RID: cat3.RID}
	linksCat1and3 := []*orient.OLink{linkToCat1, linkToCat3}

	cat2.Field("catfriends", linksCat1and3) // updates the field locally

	err = dbc.UpdateRecord(dbc, cat2) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat2.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat2.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat2FromQuery = docs[0]

	Equals(t, "A2", cat2FromQuery.GetField("name").Value)
	catFriendsFromQuery = cat2FromQuery.GetField("catfriends").Value.([]*orient.OLink)
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

	cat1 := orient.NewDocument("Cat")
	cat1.Field("name", "A1").
		Field("age", 2).
		Field("caretaker", "Jackie")

	err = dbc.CreateRecord(dbc, cat1)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat1.RID.String())

	cat2 := orient.NewDocument("Cat")
	linkToCat1 := &orient.OLink{RID: cat1.RID, Record: cat1}
	cat2.Field("name", "A2").
		Field("age", 3).
		Field("caretaker", "Jimmy").
		FieldWithType("catlink", linkToCat1, orient.LINK)

	err = dbc.CreateRecord(dbc, cat2)
	Nil(t, err)
	ridsToDelete = append(ridsToDelete, cat2.RID.String())

	// ---[ try without FieldWithType ]---

	cat3 := orient.NewDocument("Cat")
	linkToCat2 := &orient.OLink{RID: cat2.RID, Record: cat2} // also, only use RID, not record
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
	linkToCat1FromQuery := cat2FromQuery.GetField("catlink").Value.(*orient.OLink)
	Equals(t, linkToCat1FromQuery.RID, cat1.RID)

	cat3FromQuery := docs[1]
	Equals(t, "A3", cat3FromQuery.GetField("name").Value)
	Equals(t, 4, toInt(cat3FromQuery.GetField("age").Value))
	linkToCat2FromQuery := cat3FromQuery.GetField("catlink").Value.(*orient.OLink)
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
	linkToCat1FromQuery = cat3FromQuery.GetField("catlink").Value.(*orient.OLink)
	Equals(t, linkToCat1FromQuery.RID, cat1.RID)
}

func createAndUpdateRecordsWithEmbeddedLists(dbc orient.Client, embType orient.OType) {
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
	stringList := orient.NewEmbeddedSlice(embStrings, orient.STRING)

	Equals(t, orient.STRING, stringList.Type())
	Equals(t, "two", stringList.Values()[1])

	cat := orient.NewDocument("Cat")
	cat.Field("name", "Yugo").
		Field("age", 33)

	if embType == orient.EMBEDDEDLIST {
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
	True(t, ok, "Cast to orient.[]interface{} failed")

	sort.Sort(byStringVal(embListFromQuery))
	Equals(t, 3, len(embListFromQuery))
	Equals(t, "one", embListFromQuery[0])
	Equals(t, "three", embListFromQuery[1])
	Equals(t, "two", embListFromQuery[2])

	// ------

	embLongs := []interface{}{int64(22), int64(4444), int64(constants.MaxInt64 - 12)}
	int64List := orient.NewEmbeddedSlice(embLongs, orient.LONG)

	Equals(t, orient.LONG, int64List.Type())
	Equals(t, int64(22), int64List.Values()[0])

	cat = orient.NewDocument("Cat")
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
	True(t, ok, "Cast to orient.[]interface{} failed")

	sort.Sort(byLongVal(embListFromQuery))
	Equals(t, 3, len(embListFromQuery))
	Equals(t, int64(22), embListFromQuery[0])
	Equals(t, int64(4444), embListFromQuery[1])
	Equals(t, int64(constants.MaxInt64-12), embListFromQuery[2])

	// ------

	// how to insert into embcats from the OrientDB console:
	// insert into Cat set name="Draydon", age=223, embcats=[{"@class":"Cat", "name": "geary", "age":33}, {"@class":"Cat", "name": "joan", "age": 44}]

	embCat0 := orient.NewDocument("Cat")
	embCat0.Field("name", "Gordo").Field("age", 40)

	embCat1 := orient.NewDocument("Cat")
	embCat1.Field("name", "Joan").Field("age", 14).Field("caretaker", "Marcia")

	embCats := []interface{}{embCat0, embCat1}
	embcatList := orient.NewEmbeddedSlice(embCats, orient.EMBEDDED)

	cat = orient.NewDocument("Cat")
	cat.Field("name", "Draydon").
		Field("age", 3)

	if embType == orient.EMBEDDEDLIST {
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
	True(t, ok, "Cast to orient.[]interface{} failed")

	Equals(t, 2, len(embListFromQuery))
	sort.Sort(byEmbeddedCatName(embListFromQuery))

	embCatDoc0, ok := embListFromQuery[0].(*orient.Document)
	True(t, ok, "Cast to *orient.Document failed")
	embCatDoc1, ok := embListFromQuery[1].(*orient.Document)
	True(t, ok, "Cast to *orient.Document failed")

	Equals(t, "Gordo", embCatDoc0.GetField("name").Value)
	Equals(t, 40, toInt(embCatDoc0.GetField("age").Value))
	Equals(t, "Joan", embCatDoc1.GetField("name").Value)
	Equals(t, "Marcia", embCatDoc1.GetField("caretaker").Value)

	// ---[ update ]---

	// update embedded string list
	versionBefore := cat.Version

	newEmbStrings := []interface{}{"A", "BB", "CCCC"}
	newStringList := orient.NewEmbeddedSlice(newEmbStrings, orient.STRING)
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
	True(t, ok, "Cast to orient.[]interface{} failed")

	sort.Sort(byStringVal(embListFromQuery))
	Equals(t, 3, len(embListFromQuery))
	Equals(t, "A", embListFromQuery[0])
	Equals(t, "BB", embListFromQuery[1])
	Equals(t, "CCCC", embListFromQuery[2])

	// update embedded long list + embedded Cats

	newEmbLongs := []interface{}{int64(18), int64(1234567890)}
	newInt64List := orient.NewEmbeddedSlice(newEmbLongs, orient.LONG)

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
	True(t, ok, "Cast to orient.[]interface{} failed")

	sort.Sort(byLongVal(embListFromQuery))
	Equals(t, 2, len(embListFromQuery))
	Equals(t, int64(18), embListFromQuery[0])
	Equals(t, int64(1234567890), embListFromQuery[1])

	// add another cat to the embedded cat list
	embCat2 := orient.NewDocument("Cat")
	embCat2.Field("name", "Mickey").Field("age", 1)

	cat.GetField("embcats").Value.(orient.OEmbeddedList).Add(embCat2)

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
	True(t, ok, "Cast to orient.[]interface{} failed")

	Equals(t, 3, len(embListFromQuery))
	sort.Sort(byEmbeddedCatName(embListFromQuery))

	embCatDoc0, ok = embListFromQuery[0].(*orient.Document)
	True(t, ok, "Cast to *orient.Document failed")
	embCatDoc1, ok = embListFromQuery[1].(*orient.Document)
	True(t, ok, "Cast to *orient.Document failed")
	embCatDoc2, ok := embListFromQuery[2].(*orient.Document)
	True(t, ok, "Cast to *orient.Document failed")

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

	embcat := orient.NewDocument("Cat")
	embcat.Field("name", "MaryLulu").
		Field("age", 47)

	cat := orient.NewDocument("Cat")
	cat.Field("name", "Willard").
		Field("age", 4).
		FieldWithType("embcat", embcat, orient.EMBEDDED)

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
	Equals(t, orient.EMBEDDED, catFromQuery.GetField("embcat").Type)

	embCatFromQuery := catFromQuery.GetField("embcat").Value.(*orient.Document)
	True(t, embCatFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	True(t, embCatFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	True(t, embCatFromQuery.Version < 0, "Version should be unset")
	Equals(t, 2, len(embCatFromQuery.FieldNames()))
	Equals(t, 47, toInt(embCatFromQuery.GetField("age").Value))
	Equals(t, "MaryLulu", embCatFromQuery.GetField("name").Value.(string))

	// ---[ Field No Type Specified ]---

	embcat = orient.NewDocument("Cat")
	embcat.Field("name", "Tsunami").
		Field("age", 33).
		Field("purebreed", false)

	cat = orient.NewDocument("Cat")
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
	Equals(t, orient.EMBEDDED, catFromQuery.GetField("embcat").Type)

	embCatFromQuery = catFromQuery.GetField("embcat").Value.(*orient.Document)
	True(t, embCatFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	True(t, embCatFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	True(t, embCatFromQuery.Version < 0, "Version should be unset")
	Equals(t, "Cat", embCatFromQuery.Classname)
	Equals(t, 3, len(embCatFromQuery.FieldNames()))
	Equals(t, 33, toInt(embCatFromQuery.GetField("age").Value))
	Equals(t, "Tsunami", embCatFromQuery.GetField("name").Value.(string))
	Equals(t, false, embCatFromQuery.GetField("purebreed").Value.(bool))

	// ---[ Embedded with New Classname (not in DB) ]---

	moonpie := orient.NewDocument("Moonpie")
	moonpie.Field("sku", "AB425827ACX3").
		Field("allnatural", false).
		FieldWithType("oz", 6.5, orient.FLOAT)

	cat = orient.NewDocument("Cat")
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
	Equals(t, orient.EMBEDDED, catFromQuery.GetField("embcat").Type)

	moonpieFromQuery := catFromQuery.GetField("embcat").Value.(*orient.Document)
	True(t, moonpieFromQuery.RID.ClusterPos < 0, "RID (pos) should be unset")
	True(t, moonpieFromQuery.RID.ClusterID < 0, "RID (ID) should be unset")
	True(t, moonpieFromQuery.Version < 0, "Version should be unset")
	Equals(t, "", moonpieFromQuery.Classname) // it throws out the classname => TODO: check serialized binary on this
	Equals(t, 3, len(moonpieFromQuery.FieldNames()))
	Equals(t, "AB425827ACX3", moonpieFromQuery.GetField("sku").Value)
	Equals(t, float32(6.5), moonpieFromQuery.GetField("oz").Value.(float32))
	Equals(t, false, moonpieFromQuery.GetField("allnatural").Value.(bool))

	noclass := orient.NewDocument("")
	noclass.Field("sku", "AB425827ACX3222").
		Field("allnatural", true).
		FieldWithType("oz", 6.5, orient.DOUBLE)

	cat = orient.NewDocument("Cat")
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
	Equals(t, orient.EMBEDDED, catFromQuery.GetField("embcat").Type)

	noclassFromQuery := catFromQuery.GetField("embcat").Value.(*orient.Document)
	Equals(t, "", noclassFromQuery.Classname) // it throws out the classname
	Equals(t, 3, len(noclassFromQuery.FieldNames()))
	Equals(t, "AB425827ACX3222", noclassFromQuery.GetField("sku").Value)
	Equals(t, float64(6.5), noclassFromQuery.GetField("oz").Value.(float64))
	Equals(t, true, noclassFromQuery.GetField("allnatural").Value.(bool))

	// ---[ update ]---

	versionBefore := cat.Version

	moonshine := orient.NewDocument("")
	moonshine.Field("sku", "123").
		Field("allnatural", true).
		FieldWithType("oz", 99.092, orient.FLOAT)

	cat.FieldWithType("embcat", moonshine, orient.EMBEDDED) // updates the field locally

	err = dbc.UpdateRecord(dbc, cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Version, "version should have incremented")

	docs, err = db.SQLQuery(dbc, "select from Cat where @rid="+cat.RID.String(), "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]

	mshineFromQuery := catFromQuery.GetField("embcat").Value.(*orient.Document)
	Equals(t, "123", mshineFromQuery.GetField("sku").Value)
	Equals(t, true, mshineFromQuery.GetField("allnatural").Value)
	Equals(t, float32(99.092), mshineFromQuery.GetField("oz").Value)
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
	cat := orient.NewDocument("Cat")
	cat.Field("name", "sourpuss").
		Field("age", 15).
		FieldWithType("ii", constants.MaxInt32, orient.INTEGER).
		FieldWithType("lg", constants.MaxInt64, orient.LONG).
		FieldWithType("ff", floatval, orient.FLOAT).
		FieldWithType("dd", doubleval, orient.DOUBLE)

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

	cat2 := orient.NewDocument("Cat")
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

func removeProperty(db *orient.Database, class, property string) {
	sql := fmt.Sprintf("UPDATE %s REMOVE %s", class, property)
	err := db.Command(orient.NewSQLCommand(sql)).Err()
	if err != nil {
		log.Printf("WARN: clean up error: %v\n", err)
	}
	sql = fmt.Sprintf("DROP PROPERTY %s.%s", class, property)
	err = db.Command(orient.NewSQLCommand(sql)).Err()
	if err != nil {
		log.Printf("WARN: clean up error: %v\n", err)
	}
}

// Sort OLinks by RID
type byRID []orient.OIdentifiable

func (slnk byRID) Len() int {
	return len(slnk)
}

func (slnk byRID) Swap(i, j int) {
	slnk[i], slnk[j] = slnk[j], slnk[i]
}

func (slnk byRID) Less(i, j int) bool {
	return slnk[i].GetIdentity().String() < slnk[j].GetIdentity().String()
}

// sort Documents by name field
type byEmbeddedCatName []interface{}

func (a byEmbeddedCatName) Len() int {
	return len(a)
}

func (a byEmbeddedCatName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byEmbeddedCatName) Less(i, j int) bool {
	return a[i].(*orient.Document).GetField("name").Value.(string) < a[j].(*orient.Document).GetField("name").Value.(string)
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
	// fld := orient.OField{int32(44), "foo", orient.LONG, int64(33341234)}
	// bsjson, err := fld.ToJSON()
	// Nil(t, err)
	// glog.Infof("%v\n", string(bsjson))

	// doc := orient.NewDocument("Coolio")
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

	dingo := orient.NewDocument("Dingo")
	dingo.FieldWithType("foo", "bar", orient.STRING).
		FieldWithType("salad", 44, orient.INTEGER)

	cat := orient.NewDocument("Dalek")
	cat.Field("name", "dalek3").
		FieldWithType("embeddedDingo", dingo, orient.EMBEDDED)

	// ogl.SetLevel(ogl.DEBUG)

	err = dbc.CreateRecord(dbc, cat)
	Nil(t, err)
}

type testrange struct {
	start int64
	end   int64
}

*/
