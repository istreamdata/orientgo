package orient_test

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/oerror"
	"github.com/istreamdata/orientgo/oschema"
	"sort"
	"strings"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

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

	sess, err := dbc.Auth(srvUser, srvPass)
	Nil(t, err)
	//	True(t, dbc.GetSessionId() >= int32(0), "sessionid")
	//	True(t, dbc.GetCurrDB() == nil, "currDB should be nil")

	mapDBs, err := sess.ListDatabases()
	Nil(t, err)
	gratefulTestPath, ok := mapDBs["GratefulDeadConcerts"]
	True(t, ok, "GratefulDeadConcerts not in DB list")
	True(t, strings.HasPrefix(gratefulTestPath, "plocal"), "plocal prefix for db path")

	dbexists, err := sess.DatabaseExists(dbDocumentName, orient.Persistent)
	Nil(t, err)
	True(t, !dbexists)

	// err = dbc.CreateDatabase(dbc, dbDocumentName, constants.DocumentDbType, constants.Volatile)
	err = sess.CreateDatabase(dbDocumentName, orient.DocumentDB, orient.Persistent)
	Nil(t, err)
	dbexists, err = sess.DatabaseExists(dbDocumentName, orient.Persistent)
	Nil(t, err)
	True(t, dbexists, dbDocumentName+" should now exists after creating it")

	db, err := dbc.Open(dbDocumentName, orient.DocumentDB, "admin", "admin")
	Nil(t, err)
	SeedDB(t, db)

	mapDBs, err = sess.ListDatabases()
	Nil(t, err)
	ogonoriTestPath, ok := mapDBs[dbDocumentName]
	True(t, ok, dbDocumentName+" not in DB list")
	True(t, strings.HasPrefix(ogonoriTestPath, "plocal"), "plocal prefix for db path")
}

/*
// ---[ Use Go database/sql API on Document DB ]---
//	conxStr := "admin@admin:localhost/" + dbDocumentName
//	databaseSQLAPI(conxStr)
//	databaseSQLPreparedStmtAPI(conxStr)

// ---[ Graph DB ]---
// graph database tests
//	graphCommandsNativeAPI(dbc, testType != "dataOnly")
//	graphConxStr := "admin@admin:localhost/" + dbGraphName
//	graphCommandsSQLAPI(graphConxStr)
*/

func testRecordsNativeAPI(t *testing.T) { // TODO: disabled due to serialization issues
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	// ---[ creation ]---

	winston := oschema.NewDocument("Cat")
	winston.Field("name", "Winston").
		Field("caretaker", "Churchill").
		FieldWithType("age", 7, oschema.INTEGER)
	Equals(t, -1, int(winston.RID.ClusterID))
	Equals(t, -1, int(winston.RID.ClusterPos))
	Equals(t, -1, int(winston.Version))
	err := db.CreateRecord(winston)
	Nil(t, err)
	True(t, int(winston.RID.ClusterID) > -1, "RID should be filled in")
	True(t, int(winston.RID.ClusterPos) > -1, "RID should be filled in")
	True(t, int(winston.Version) > -1, "Version should be filled in")

	// ---[ update STRING and INTEGER field ]---

	versionBefore := winston.Version

	winston.Field("caretaker", "Lolly") // this updates the field locally
	winston.Field("age", 8)             // this updates the field locally
	err = db.UpdateRecord(winston)      // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < winston.Version, "version should have incremented")

	var docs []*oschema.ODocument
	_, err = db.SQLQuery(&docs, nil, "select * from Cat where @rid="+winston.RID.String())
	Nil(t, err)
	Equals(t, 1, len(docs))

	winstonFromQuery := docs[0]
	Equals(t, "Winston", winstonFromQuery.GetField("name").Value)
	Equals(t, 8, toInt(winstonFromQuery.GetField("age").Value))
	Equals(t, "Lolly", winstonFromQuery.GetField("caretaker").Value)

	// ---[ next creation ]---

	daemon := oschema.NewDocument("Cat")
	daemon.Field("name", "Daemon").Field("caretaker", "Matt").Field("age", 4)
	err = db.CreateRecord(daemon)
	Nil(t, err)

	indy := oschema.NewDocument("Cat")
	indy.Field("name", "Indy").Field("age", 6)
	err = db.CreateRecord(indy)
	Nil(t, err)

	sql := fmt.Sprintf("select from Cat where @rid=%s or @rid=%s or @rid=%s ORDER BY name",
		winston.RID, daemon.RID, indy.RID)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 3, len(docs))
	Equals(t, daemon.RID, docs[0].RID)
	Equals(t, indy.RID, docs[1].RID)
	Equals(t, winston.RID, docs[2].RID)

	Equals(t, indy.Version, docs[1].Version)
	Equals(t, "Matt", docs[0].GetField("caretaker").Value)

	sql = fmt.Sprintf("DELETE FROM [%s, %s, %s]", winston.RID, daemon.RID, indy.RID)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ---[ Test DATE Serialization ]---
	//createAndUpdateRecordsWithDate(dbc)

	// ---[ Test DATETIME Serialization ]---
	//createAndUpdateRecordsWithDateTime(dbc)

	// test inserting wrong values for date and datetime
	//testCreationOfMismatchedTypesAndValues(dbc)

	// ---[ Test Boolean, Byte and Short Serialization ]---
	//createAndUpdateRecordsWithBooleanByteAndShort(dbc)

	// ---[ Test Int, Long, Float and Double Serialization ]---
	//createAndUpdateRecordsWithIntLongFloatAndDouble(dbc)

	// ---[ Test BINARY Serialization ]---
	//createAndUpdateRecordsWithBINARYType(dbc)

	// ---[ Test EMBEDDEDRECORD Serialization ]---
	//createAndUpdateRecordsWithEmbeddedRecords(dbc)

	// ---[ Test EMBEDDEDLIST, EMBEDDEDSET Serialization ]---
	//createAndUpdateRecordsWithEmbeddedLists(dbc, oschema.EMBEDDEDLIST)
	//createAndUpdateRecordsWithEmbeddedLists(dbc, oschema.EMBEDDEDSET)

	// ---[ Test Link Serialization ]---
	//createAndUpdateRecordsWithLinks(dbc)

	// ---[ Test LinkList/LinkSet Serialization ]---
	//createAndUpdateRecordsWithLinkLists(dbc, oschema.LINKLIST)
	// createAndUpdateRecordsWithLinkLists(dbc, oschema.LINKSET)  // TODO: get this working

	// ---[ Test LinkMap Serialization ]---
	//createAndUpdateRecordsWithLinkMap(dbc)
}

func TestCommandsNativeAPI(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	var (
		sql  string
		recs orient.Records
	)

	// ---[ query from the ogonoriTest database ]---

	sql = "select from Cat where name = 'Linus'"

	var docs []*oschema.ODocument
	_, err := db.SQLQuery(&docs, nil, sql)
	Nil(t, err)

	linusDocRID := docs[0].RID

	True(t, linusDocRID.ClusterID != oschema.ClusterIDInvalid, "linusDocRID should not be nil")
	True(t, docs[0].Version > 0, fmt.Sprintf("Version is: %d", docs[0].Version))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Cat", docs[0].Classname)

	nameField := docs[0].GetField("name")
	True(t, nameField != nil, "should be a 'name' field")

	ageField := docs[0].GetField("age")
	True(t, ageField != nil, "should be a 'age' field")

	caretakerField := docs[0].GetField("caretaker")
	True(t, caretakerField != nil, "should be a 'caretaker' field")

	True(t, nameField.Id != caretakerField.Id, "IDs should not match")
	Equals(t, oschema.STRING, nameField.Type)
	Equals(t, oschema.STRING, caretakerField.Type)
	Equals(t, oschema.INTEGER, ageField.Type)
	Equals(t, "Linus", nameField.Value)
	Equals(t, int32(15), ageField.Value)
	Equals(t, "Michael", caretakerField.Value)

	// ---[ get by RID ]---
	docs, err = db.GetRecordByRID(linusDocRID, "")
	Nil(t, err)
	Equals(t, 1, len(docs))
	docByRID := docs[0]
	Equals(t, linusDocRID, docByRID.RID)
	True(t, docByRID.Version > 0, fmt.Sprintf("Version is: %d", docByRID.Version))
	Equals(t, 3, len(docByRID.FieldNames()))
	Equals(t, "Cat", docByRID.Classname)

	nameField = docByRID.GetField("name")
	True(t, nameField != nil, "should be a 'name' field")

	ageField = docByRID.GetField("age")
	True(t, ageField != nil, "should be a 'age' field")

	caretakerField = docByRID.GetField("caretaker")
	True(t, caretakerField != nil, "should be a 'caretaker' field")

	True(t, nameField.Id != caretakerField.Id, "IDs should not match")
	Equals(t, oschema.STRING, nameField.Type)
	Equals(t, oschema.INTEGER, ageField.Type)
	Equals(t, oschema.STRING, caretakerField.Type)
	Equals(t, "Linus", nameField.Value)
	Equals(t, int32(15), ageField.Value)
	Equals(t, "Michael", caretakerField.Value)

	glog.Infof("docs returned by RID: %v\n", *(docs[0]))

	// ---[ cluster data range ]---
	//	begin, end, err := db.FetchClusterDataRange("cat")
	//	Nil(t, err)
	//	glog.Infof("begin = %v; end = %v\n", begin, end)

	sql = "insert into Cat (name, age, caretaker) values(\"Zed\", 3, \"Shaw\")"
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ---[ query after inserting record(s) ]---

	sql = "select * from Cat order by name asc"
	glog.Infoln("Issuing command query: " + sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 3, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Cat", docs[0].Classname)
	Equals(t, 3, len(docs[1].FieldNames()))
	Equals(t, "Cat", docs[1].Classname)
	Equals(t, 3, len(docs[2].FieldNames()))
	Equals(t, "Cat", docs[2].Classname)

	keiko := docs[0]
	Equals(t, "Keiko", keiko.GetField("name").Value)
	Equals(t, int32(10), keiko.GetField("age").Value)
	Equals(t, "Anna", keiko.GetField("caretaker").Value)
	Equals(t, oschema.STRING, keiko.GetField("caretaker").Type)
	True(t, keiko.Version > int32(0), "Version should be greater than zero")
	True(t, keiko.RID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	linus := docs[1]
	Equals(t, "Linus", linus.GetField("name").Value)
	Equals(t, int32(15), linus.GetField("age").Value)
	Equals(t, "Michael", linus.GetField("caretaker").Value)

	zed := docs[2]
	Equals(t, "Zed", zed.GetField("name").Value)
	Equals(t, int32(3), zed.GetField("age").Value)
	Equals(t, "Shaw", zed.GetField("caretaker").Value)
	Equals(t, oschema.STRING, zed.GetField("caretaker").Type)
	Equals(t, oschema.INTEGER, zed.GetField("age").Type)
	True(t, zed.Version > int32(0), "Version should be greater than zero")
	True(t, zed.RID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	sql = "select name, caretaker from Cat order by caretaker"
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 3, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].Classname) // property queries do not come back with Classname set
	Equals(t, 2, len(docs[1].FieldNames()))
	Equals(t, "", docs[1].Classname)
	Equals(t, 2, len(docs[2].FieldNames()))

	Equals(t, "Anna", docs[0].GetField("caretaker").Value)
	Equals(t, "Michael", docs[1].GetField("caretaker").Value)
	Equals(t, "Shaw", docs[2].GetField("caretaker").Value)

	Equals(t, "Keiko", docs[0].GetField("name").Value)
	Equals(t, "Linus", docs[1].GetField("name").Value)
	Equals(t, "Zed", docs[2].GetField("name").Value)

	Equals(t, "name", docs[0].GetField("name").Name)

	// ---[ delete newly added record(s) ]---
	glog.Infoln("Deleting (sync) record #" + zed.RID.String())
	err = db.DeleteRecordByRID(zed.RID.String(), zed.Version)
	Nil(t, err)

	// glog.Infoln("Deleting (Async) record #11:4")
	// err = dbc.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	Fatal(err)
	// }

	sql = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	_, err = db.SQLCommand(nil, sql, "June", "8", "Cleaver") // TODO: check if numeric types are passed as strings in the Java client
	Nil(t, err)

	sql = "select name, age from Cat where caretaker = ?"
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql, "Cleaver")
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].Classname) // property queries do not come back with Classname set
	Equals(t, "June", docs[0].GetField("name").Value)
	Equals(t, int32(8), docs[0].GetField("age").Value)

	sql = "select caretaker, name, age from Cat where age > ? order by age desc"
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql, "9")
	Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].Classname) // property queries do not come back with Classname set
	Equals(t, "Linus", docs[0].GetField("name").Value)
	Equals(t, int32(15), docs[0].GetField("age").Value)
	Equals(t, "Keiko", docs[1].GetField("name").Value)
	Equals(t, int32(10), docs[1].GetField("age").Value)
	Equals(t, "Anna", docs[1].GetField("caretaker").Value)

	sql = "delete from Cat where name ='June'" // TODO: can we use a param here too ?
	glog.Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	glog.Infoln("+++++++++ END: SQL COMMAND w/ PARAMS ++++++++++++===")

	glog.Infoln("+++++++++ START: Basic DDL ++++++++++++===")

	sql = "DROP CLASS Patient"
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	if retval != "" {
	//		Equals(t, "true", retval)
	//	}

	// ------

	sql = "CREATE CLASS Patient"
	glog.V(10).Infoln(sql)
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	defer func() {
		sql = "DROP CLASS Patient"
		_, err = db.SQLCommand(nil, sql)
		if err != nil {
			glog.Warningf("WARN: clean up error: %v\n", err)
			return
		}

		// TRUNCATE after drop should return an OServerException type
		sql = "TRUNCATE CLASS Patient"
		_, err = db.SQLCommand(nil, sql)
		True(t, err != nil, "Error from TRUNCATE should not be null")
		glog.V(10).Infoln(oerror.GetFullTrace(err))

		err = oerror.ExtractCause(err)
		switch err.(type) {
		case oerror.OServerException:
			glog.V(10).Infoln("type == oerror.OServerException")
		default:
			t.Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
		}
	}()

	ncls, err := recs.AsInt()
	Nil(t, err)
	True(t, ncls > 10, "classnum should be greater than 10 but was: ") //+retval)

	// ------

	sql = "Create property Patient.name string"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "alter property Patient.name min 3"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "Create property Patient.married boolean"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	db.ReloadSchema()
	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true')"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "TRUNCATE CLASS Patient"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married) VALUES ('Hank', 'true'), ('Martha', 'false')"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "SELECT count(*) from Patient"
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	fldCount := docs[0].GetField("count")
	Equals(t, int64(2), fldCount.Value)

	sql = "CREATE PROPERTY Patient.gender STRING"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "ALTER PROPERTY Patient.gender REGEXP [M|F]"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married, gender) VALUES ('Larry', 'true', 'M'), ('Shirley', 'false', 'F')"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: %v\n", retval)
	//	glog.V(10).Infof("docs: %v\n", docs)

	sql = "INSERT INTO Patient (name, married, gender) VALUES ('Lt. Dan', 'true', 'T'), ('Sally', 'false', 'F')"
	glog.Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	True(t, err != nil, "should be error - T is not an allowed gender")
	err = oerror.ExtractCause(err)
	switch err.(type) {
	case oerror.OServerException:
		glog.V(10).Infoln("type == oerror.OServerException")
	default:
		t.Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
	}

	sql = "SELECT FROM Patient ORDER BY @rid desc"
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 4, len(docs))
	Equals(t, "Shirley", docs[0].GetField("name").Value)

	sql = "ALTER PROPERTY Patient.gender NAME sex"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	err = db.ReloadSchema()
	Nil(t, err)

	sql = "DROP PROPERTY Patient.sex"
	glog.V(10).Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	sql = "select from Patient order by RID"
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 4, len(docs))
	Equals(t, 2, len(docs[0].Fields)) // has name and married
	Equals(t, "Hank", docs[0].Fields["name"].Value)

	Equals(t, 4, len(docs[3].Fields)) // has name, married, sex and for some reason still has `gender`
	Equals(t, "Shirley", docs[3].Fields["name"].Value)
	Equals(t, "F", docs[3].Fields["gender"].Value)

	sql = "TRUNCATE CLASS Patient"
	glog.Infoln(sql)
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	// ---[ Attempt to create, insert and read back EMBEDDEDLIST types ]---

	sql = "CREATE PROPERTY Patient.tags EMBEDDEDLIST STRING"
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)

	numval, err := recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `insert into Patient (name, married, tags) values ("George", "false", ["diabetic", "osteoarthritis"])`
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))

	sql = `SELECT from Patient where name = 'George'`
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	glog.V(10).Infof("docs: %v\n", docs)
	Equals(t, 1, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	embListTagsField := docs[0].GetField("tags")

	embListTags := embListTagsField.Value.([]interface{})
	Equals(t, 2, len(embListTags))
	Equals(t, "diabetic", embListTags[0].(string))
	Equals(t, "osteoarthritis", embListTags[1].(string))

	// ---[ try JSON content insertion notation ]---

	sql = `insert into Patient content {"name": "Freddy", "married":false}`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Freddy", docs[0].GetField("name").Value)
	Equals(t, false, docs[0].GetField("married").Value)

	// ---[ Try LINKs ! ]---

	sql = `select from Cat WHERE name = 'Linus' OR name='Keiko' ORDER BY @rid`
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Equals(t, 2, len(docs))
	linusRID := docs[0].RID
	keikoRID := docs[1].RID

	sql = `CREATE PROPERTY Cat.buddy LINK`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "buddy")

	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `insert into Cat SET name='Tilde', age=8, caretaker='Earl', buddy=(SELECT FROM Cat WHERE name = 'Linus')`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	//	glog.V(10).Infof("retval: >>%v<<\n", retval)
	//	glog.V(10).Infof("docs: >>%v<<\n", docs)
	Equals(t, 1, len(docs))
	Equals(t, "Tilde", docs[0].GetField("name").Value)
	Equals(t, 8, int(docs[0].GetField("age").Value.(int32)))
	Equals(t, linusRID, docs[0].GetField("buddy").Value.(*oschema.OLink).RID)

	tildeRID := docs[0].RID

	// ---[ Test EMBEDDED ]---

	sql = `CREATE PROPERTY Cat.embeddedCat EMBEDDED`
	_, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "embeddedCat")

	emb := `{"name": "Spotty", "age": 2, emb: {"@type": "d", "@class":"Cat", "name": "yowler", "age":13}}`
	docs = nil
	_, err = db.SQLCommand(&docs, "insert into Cat content "+emb)
	Nil(t, err)

	Equals(t, 1, len(docs))
	Equals(t, "Spotty", docs[0].GetField("name").Value)
	Equals(t, 2, int(docs[0].GetField("age").Value.(int32)))
	Equals(t, oschema.EMBEDDED, docs[0].GetField("emb").Type)

	embCat := docs[0].GetField("emb").Value.(*oschema.ODocument)
	Equals(t, "Cat", embCat.Classname)
	True(t, embCat.Version < 0, "Version should be unset")
	True(t, embCat.RID.ClusterID < 0, "RID.ClusterID should be unset")
	True(t, embCat.RID.ClusterPos < 0, "RID.ClusterPos should be unset")
	Equals(t, "yowler", embCat.GetField("name").Value.(string))
	Equals(t, int(13), toInt(embCat.GetField("age").Value))

	_, err = db.SQLCommand(nil, "delete from Cat where name = 'Spotty'")
	Nil(t, err)

	// ---[ Test LINKLIST ]---

	sql = `CREATE PROPERTY Cat.buddies LINKLIST`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "buddies")
	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `insert into Cat SET name='Felix', age=6, caretaker='Ed', buddies=(SELECT FROM Cat WHERE name = 'Linus' OR name='Keiko')`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Felix", docs[0].GetField("name").Value)
	Equals(t, 6, int(docs[0].GetField("age").Value.(int32)))
	buddies := docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddies))
	Equals(t, 2, len(buddies))
	Equals(t, linusRID, buddies[0].RID)
	Equals(t, keikoRID, buddies[1].RID)

	felixRID := docs[0].RID

	// ---[ Try LINKMAP ]---
	sql = `CREATE PROPERTY Cat.notes LINKMAP`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "notes")

	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = fmt.Sprintf(`INSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": %s, '30': %s}`,
		linusRID, keikoRID)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 4, len(docs[0].FieldNames()))
	Equals(t, "Anna", docs[0].GetField("caretaker").Value)
	Equals(t, linusRID, docs[0].GetField("notes").Value.(map[string]*oschema.OLink)["bff"].RID)
	Equals(t, keikoRID, docs[0].GetField("notes").Value.(map[string]*oschema.OLink)["30"].RID)

	charlieRID := docs[0].RID

	// query with a fetchPlan that does NOT follow all the links
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL`
	docs = nil
	_, err = db.SQLQuery(&docs, nil, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	doc := docs[0]
	Equals(t, "Charlie", doc.GetField("name").Value)
	notesField := doc.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(notesField))

	bffNote := notesField["bff"]
	True(t, bffNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, bffNote.Record == nil, "RID should be nil")

	thirtyNote := notesField["30"]
	True(t, thirtyNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, thirtyNote.Record == nil, "RID should be nil")

	// query with a fetchPlan that does follow all the links

	sql = `SELECT FROM Cat WHERE notes IS NOT NULL`
	docs = nil
	recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	True(t, len(docs) > 0)
	doc = docs[0]
	Equals(t, "Charlie", doc.GetField("name").Value)
	notesField = doc.GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(notesField))

	bffNote = notesField["bff"]
	True(t, bffNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, bffNote.Record != nil, "Record should be filled in")
	Equals(t, "Linus", bffNote.Record.GetField("name").Value)

	thirtyNote = notesField["30"]
	True(t, thirtyNote.RID.ClusterID != -1, "RID should be filled in")
	True(t, thirtyNote.Record != nil, "Record should be filled in")
	Equals(t, "Keiko", thirtyNote.Record.GetField("name").Value)

	// ---[ Try LINKSET ]---

	sql = `CREATE PROPERTY Cat.buddySet LINKSET`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "buddySet")

	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	db.ReloadSchema() // good thing to do after modifying the schema

	// insert record with all the LINK types
	sql = `insert into Cat SET name='Germaine', age=2, caretaker='Minnie', ` +
		`buddies=(SELECT FROM Cat WHERE name = 'Linus' OR name='Keiko'), ` +
		`buddySet=(SELECT FROM Cat WHERE name = 'Linus' OR name='Felix'), ` +
		fmt.Sprintf(`notes = {"bff": %s, "30": %s}`, keikoRID, linusRID)

	// status of Cat at this point in time
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     #   |@RID |@CLASS|name    |age |caretaker|buddy|buddies        |notes                |buddySet
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     0   |#10:0|Cat   |Linus   |15  |Michael  |null |null           |null                 |null
	//     1   |#10:1|Cat   |Keiko   |10  |Anna     |null |null           |null                 |null
	//     2   |#10:4|Cat   |Tilde   |8   |Earl     |#10:0|null           |null                 |null
	//     3   |#10:5|Cat   |Felix   |6   |Ed       |null |[#10:0, #10:1] |null                 |null
	//     4   |#10:6|Cat   |Charlie |5   |Anna     |null |null           |{bff:#10:0, 30:#10:1}|null
	//     5   |#10:7|Cat   |Germaine|2   |Minnie   |null |[#10:0, #10:1] |{bff:#10:1, 30:#10:0}|[#10:0, #10:5]
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     Germaine references
	//     Felix references => Linus and Keiko as "buddies" (LINKLIST)

	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Germaine", docs[0].GetField("name").Value)
	Equals(t, 2, int(docs[0].GetField("age").Value.(int32)))

	germaineRID := docs[0].RID

	buddyList := docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddyList))
	Equals(t, 2, len(buddies))
	Equals(t, linusRID, buddyList[0].RID)
	Equals(t, keikoRID, buddyList[1].RID)

	buddySet := docs[0].GetField("buddySet").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddySet))
	Equals(t, 2, len(buddySet))
	Equals(t, linusRID, buddySet[0].RID)
	Equals(t, felixRID, buddySet[1].RID)

	notesMap := docs[0].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(buddies))
	Equals(t, keikoRID, notesMap["bff"].RID)
	Equals(t, linusRID, notesMap["30"].RID)

	// now query with fetchPlan that retrieves all links
	sql = `SELECT FROM Cat WHERE notes IS NOT NULL ORDER BY name`
	docs = nil
	recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, "Charlie", docs[0].GetField("name").Value)
	Equals(t, "Germaine", docs[1].GetField("name").Value)
	Equals(t, "Minnie", docs[1].GetField("caretaker").Value)

	charlieNotesField := docs[0].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(charlieNotesField))

	bffNote = charlieNotesField["bff"]
	Equals(t, "Linus", bffNote.Record.GetField("name").Value)

	thirtyNote = charlieNotesField["30"]
	Equals(t, "Keiko", thirtyNote.Record.GetField("name").Value)

	// test Germaine's notes (LINKMAP)
	germaineNotesField := docs[1].GetField("notes").Value.(map[string]*oschema.OLink)
	Equals(t, 2, len(germaineNotesField))

	bffNote = germaineNotesField["bff"]
	Equals(t, "Keiko", bffNote.Record.GetField("name").Value)

	thirtyNote = germaineNotesField["30"]
	Equals(t, "Linus", thirtyNote.Record.GetField("name").Value)

	// test Germaine's buddySet (LINKSET)
	germaineBuddySet := docs[1].GetField("buddySet").Value.([]*oschema.OLink)
	sort.Sort(byRID(germaineBuddySet))
	Equals(t, "Linus", germaineBuddySet[0].Record.GetField("name").Value)
	Equals(t, "Felix", germaineBuddySet[1].Record.GetField("name").Value)
	True(t, germaineBuddySet[1].RID.ClusterID != -1, "RID should be filled in")

	// Felix Document has references, so those should also be filled in
	felixDoc := germaineBuddySet[1].Record
	felixBuddiesList := felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, 2, len(felixBuddiesList))
	True(t, felixBuddiesList[0].Record != nil, "Felix links should be filled in")
	Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)

	// test Germaine's buddies (LINKLIST)
	germaineBuddyList := docs[1].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(germaineBuddyList))
	Equals(t, "Linus", germaineBuddyList[0].Record.GetField("name").Value)
	Equals(t, "Keiko", germaineBuddyList[1].Record.GetField("name").Value)
	True(t, germaineBuddyList[0].RID.ClusterID != -1, "RID should be filled in")

	// now make a circular reference -> give Linus to Germaine as buddy
	sql = `UPDATE Cat SET buddy = ` + germaineRID.String() + ` where name = 'Linus'`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	ret, err := recs.AsInt()
	Equals(t, 1, ret)

	// status of Cat at this point in time
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     #   |@RID |@CLASS|name    |age |caretaker|buddy|buddies        |notes                |buddySet
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------
	//     0   |#10:0|Cat   |Linus   |15  |Michael  |#10:7|null           |null                 |null
	//     1   |#10:1|Cat   |Keiko   |10  |Anna     |null |null           |null                 |null
	//     2   |#10:4|Cat   |Tilde   |8   |Earl     |#10:0|null           |null                 |null
	//     3   |#10:5|Cat   |Felix   |6   |Ed       |null |[#10:0, #10:1] |null                 |null
	//     4   |#10:6|Cat   |Charlie |5   |Anna     |null |null           |{bff:#10:0, 30:#10:1}|null
	//     5   |#10:7|Cat   |Germaine|2   |Minnie   |null |[#10:0, #10:1] |{bff:#10:1, 30:#10:0}|[#10:0, #10:5]
	//     ----+-----+------+--------+----+---------+-----+---------------+---------------------+--------

	// ---[ queries with extended fetchPlan (simple case) ]---
	sql = `select * from Cat where name = 'Tilde'`
	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	doc = docs[0]
	Equals(t, "Tilde", doc.GetField("name").Value)
	tildeBuddyField := doc.GetField("buddy").Value.(*oschema.OLink)
	Equals(t, linusRID, tildeBuddyField.RID)
	Equals(t, "Linus", tildeBuddyField.Record.GetField("name").Value)

	// now pull in both records with non-null buddy links
	//     Tilde and Linus are the primary docs
	//     Tilde.buddy -> Linus
	//     Linus.buddy -> Felix
	//     Felix.buddies -> Linus and Keiko
	//     so Tilde, Linus, Felix and Keiko should all be pulled in, but only
	//     Tilde and Linus returned directly from the query
	sql = `SELECT FROM Cat where buddy is not null ORDER BY name`

	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, "Linus", docs[0].GetField("name").Value)
	Equals(t, "Tilde", docs[1].GetField("name").Value)

	linusBuddy := docs[0].GetField("buddy").Value.(*oschema.OLink)
	True(t, linusBuddy.Record != nil, "Record should be filled in")
	Equals(t, "Germaine", linusBuddy.Record.GetField("name").Value)

	tildeBuddy := docs[1].GetField("buddy").Value.(*oschema.OLink)
	True(t, tildeBuddy.Record != nil, "Record should be filled in")
	Equals(t, "Linus", tildeBuddy.Record.GetField("name").Value)

	// now check that Felix buddies were pulled in too
	felixDoc = linusBuddy.Record
	felixBuddiesList = felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, 2, len(felixBuddiesList))
	Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)
	Equals(t, "Keiko", felixBuddiesList[1].Record.GetField("name").Value)

	// Linus.buddy links to Felix
	// Felix.buddies links Linux and Keiko
	sql = `SELECT FROM Cat WHERE name = 'Linus' OR name = 'Felix' ORDER BY name DESC`
	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	linusBuddy = docs[0].GetField("buddy").Value.(*oschema.OLink)
	True(t, linusBuddy.Record != nil, "Record should be filled in")
	Equals(t, "Germaine", linusBuddy.Record.GetField("name").Value)

	True(t, docs[1].GetField("buddy") == nil, "Felix should have no 'buddy'")
	felixBuddiesList = docs[1].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)
	Equals(t, "Keiko", felixBuddiesList[1].Record.GetField("name").Value)
	Equals(t, "Anna", felixBuddiesList[1].Record.GetField("caretaker").Value)

	// check that Felix's reference to Linus has Linus' link filled in
	Equals(t, "Germaine", felixBuddiesList[0].Record.GetField("buddy").Value.(*oschema.OLink).Record.GetField("name").Value)

	// ------

	sql = `select * from Cat where buddies is not null ORDER BY name`
	docs = nil
	_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	Nil(t, err)
	Equals(t, 2, len(docs))
	felixDoc = docs[0]
	Equals(t, "Felix", felixDoc.GetField("name").Value)
	felixBuddiesList = felixDoc.GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(felixBuddiesList))
	Equals(t, 2, len(felixBuddiesList))
	felixBuddy0 := felixBuddiesList[0]
	True(t, felixBuddy0.RID.ClusterID != -1, "RID should be filled in")
	Equals(t, "Linus", felixBuddy0.Record.GetField("name").Value)
	felixBuddy1 := felixBuddiesList[1]
	True(t, felixBuddy1.RID.ClusterID != -1, "RID should be filled in")
	Equals(t, "Keiko", felixBuddy1.Record.GetField("name").Value)

	// now test that the LINK docs had their LINKs filled in
	linusDocViaFelix := felixBuddy0.Record
	linusBuddyLink := linusDocViaFelix.GetField("buddy").Value.(*oschema.OLink)
	Equals(t, "Germaine", linusBuddyLink.Record.GetField("name").Value)

	// ------

	// Create two records that reference only each other (a.buddy = b and b.buddy = a)
	//  do:  SELECT FROM Cat where name = "a" OR name = "b" with *:-1 fetchPlan
	//  and make sure if the LINK fields are filled in
	//  with the *:-1 fetchPlan, OrientDB server will return all the link docs in the
	//  "supplementary section" even if they are already in the primary docs section

	sql = `INSERT INTO Cat SET name='Tom', age=3`
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	tomRID := docs[0].RID
	True(t, tomRID.ClusterID != oschema.ClusterIDInvalid, "RID should be filled in")

	sql = `INSERT INTO Cat SET name='Nick', age=4, buddy=?`
	docs = nil
	_, err = db.SQLCommand(&docs, sql, tomRID)
	Nil(t, err)
	Equals(t, 1, len(docs))
	nickRID := docs[0].RID

	sql = `UPDATE Cat SET buddy=? WHERE name='Tom' and age=3`
	_, err = db.SQLCommand(nil, sql, nickRID)
	Nil(t, err)

	db.ReloadSchema()

	// in this case the buddy links should be filled in with full Documents
	sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
	docs = nil
	recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql, "Tom", "Nick")
	Nil(t, err)
	Equals(t, 2, len(docs))
	tomDoc := docs[0]
	nickDoc := docs[1]
	Equals(t, "Tom", tomDoc.GetField("name").Value)
	Equals(t, "Nick", nickDoc.GetField("name").Value)

	// TODO: FIX

	//	// TODO: this section fails with orientdb-community-2.1-rc5
	//	tomsBuddy := tomDoc.GetField("buddy").Value.(*oschema.OLink)
	//	nicksBuddy := nickDoc.GetField("buddy").Value.(*oschema.OLink)
	//	// True(t, tomsBuddy.Record != nil, "should have retrieved the link record")
	//	// True(t, nicksBuddy.Record != nil, "should have retrieved the link record")
	//	// Equals(t, "Nick", tomsBuddy.Record.GetField("name").Value)
	//	// Equals(t, "Tom", nicksBuddy.Record.GetField("name").Value)
	//
	//	// in this case the buddy links should NOT be filled in with full Documents
	//	sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
	//	docs = nil
	//	_, err = db.SQLQuery(&docs, nil, sql, "Tom", "Nick")
	//	Nil(t, err)
	//	Equals(t, 2, len(docs))
	//	tomDoc = docs[0]
	//	nickDoc = docs[1]
	//	Equals(t, "Tom", tomDoc.GetField("name").Value)
	//	Equals(t, "Nick", nickDoc.GetField("name").Value)
	//
	//	tomsBuddy = tomDoc.GetField("buddy").Value.(*oschema.OLink)
	//	nicksBuddy = nickDoc.GetField("buddy").Value.(*oschema.OLink)
	//	True(t, tomsBuddy.RID.ClusterID != -1, "RID should be filled in")
	//	True(t, nicksBuddy.RID.ClusterID != -1, "RID should be filled in")
	//	True(t, tomsBuddy.Record == nil, "Record should NOT be filled in")
	//	True(t, nicksBuddy.Record == nil, "Record should NOT be filled in")

	// ------

	// ----+-----+------+--------+----+---------+-----+-------+---------------------+--------
	// #   |@RID |@CLASS|name    |age |caretaker|buddy|buddies|notes                |buddySet
	// ----+-----+------+--------+----+---------+-----+-------+---------------------+--------
	// 0   |#10:0|Cat   |Linus   |15  |Michael  |#10:7|null   |null                 |null
	// 1   |#10:1|Cat   |Keiko   |10  |Anna     |null |null   |null                 |null
	// 2   |#10:4|Cat   |Tilde   |8   |Earl     |#10:0|null   |null                 |null
	// 3   |#10:5|Cat   |Felix   |6   |Ed       |null |[2]    |null                 |null
	// 4   |#10:6|Cat   |Charlie |5   |Anna     |null |null   |{bff:#10:0, 30:#10:1}|null
	// 5   |#10:7|Cat   |Germaine|2   |Minnie   |null |[2]    |{bff:#10:1, 30:#10:0}|[2]
	// 6   |#10:8|Cat   |Tom     |3   |null     |#10:9|null   |null                 |null
	// 7   |#10:9|Cat   |Nick    |4   |null     |#10:8|null   |null                 |null
	// ----+-----+------+--------+----+---------+-----+-------+---------------------+--------

	//
	// Use a fetchPlan that only gets some of the LINKS, not all
	//
	sql = `SELECT from Cat where name = ?`
	docs = nil
	_, err = db.SQLQuery(&docs, &orient.FetchPlan{Plan: "buddy:0 buddies:1 buddySet:0 notes:0"}, sql, "Felix")
	// docs, err = db.SQLQuery(dbc, sql, FetchPlanFollowAllLinks, "Felix")
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Felix", docs[0].GetField("name").Value)
	buddies = docs[0].GetField("buddies").Value.([]*oschema.OLink)
	sort.Sort(byRID(buddies))
	Equals(t, 2, len(buddies))
	linusDoc := buddies[0].Record
	True(t, linusDoc != nil, "first level should be filled in")
	linusBuddy = linusDoc.GetField("buddy").Value.(*oschema.OLink)
	True(t, linusBuddy.RID.ClusterID != -1, "RID should be filled in")
	True(t, linusBuddy.Record == nil, "Record of second level should NOT be filled in")

	keikoDoc := buddies[1].Record
	True(t, keikoDoc != nil, "first level should be filled in")

	// ------

	// ---[ Try DATETIME ]---

	sql = `Create PROPERTY Cat.dt DATETIME`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "dt")
	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	sql = `Create PROPERTY Cat.birthday DATE`
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	defer removeProperty(db, "Cat", "birthday")
	numval, err = recs.AsInt()
	Nil(t, err)
	True(t, int(numval) >= 0, "retval from PROPERTY creation should be a positive number")

	// OrientDB DATETIME is precise to the millisecond
	sql = `INSERT into Cat SET name = 'Bruce', dt = '2014-11-25 09:14:54'`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Bruce", docs[0].GetField("name").Value)

	dt := docs[0].GetField("dt").Value.(time.Time)
	zone, zoneOffset := dt.Zone()
	zoneLocation := time.FixedZone(zone, zoneOffset)
	expectedTm, err := time.Parse("2006-01-02 03:04:05", "2014-11-25 09:14:54") //time.ParseInLocation("2006-01-02 03:04:05", "2014-11-25 09:14:54", zoneLocation)
	Nil(t, err)
	Equals(t, expectedTm.Local().String(), dt.String())

	bruceRID := docs[0].RID

	sql = `INSERT into Cat SET name = 'Tiger', birthday = '2014-11-25'`
	glog.V(10).Infoln(sql)
	docs = nil
	_, err = db.SQLCommand(&docs, sql)
	Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, "Tiger", docs[0].GetField("name").Value)

	birthdayTm := docs[0].GetField("birthday").Value.(time.Time)
	zone, zoneOffset = birthdayTm.Zone()
	zoneLocation = time.FixedZone(zone, zoneOffset)
	expectedTm, err = time.ParseInLocation("2006-01-02", "2014-11-25", zoneLocation)
	Nil(t, err)
	Equals(t, expectedTm.String(), birthdayTm.String())

	tigerRID := docs[0].RID

	// ---[ Clean up above expts ]---

	ridsToDelete := []interface{}{felixRID, tildeRID, charlieRID, bruceRID, tigerRID, germaineRID, tomRID, nickRID}
	sql = fmt.Sprintf("DELETE from [%s,%s,%s,%s,%s,%s,%s,%s]", ridsToDelete...)

	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	ret, err = recs.AsInt()
	Nil(t, err)
	Equals(t, len(ridsToDelete), ret)

	db.ReloadSchema()

	sql = "DROP CLASS Patient"
	glog.V(10).Infoln(sql)
	recs, err = db.SQLCommand(nil, sql)
	Nil(t, err)
	retb, err := recs.AsBool()
	Nil(t, err)
	Equals(t, true, retb)
}

func TestClusterNativeAPI(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	recint := func(recs orient.Records) int {
		val, err := recs.AsInt()
		Nil(t, err)
		return val
	}
	recbool := func(recs orient.Records) bool {
		val, err := recs.AsBool()
		Nil(t, err)
		return val
	}

	cnt1, err := db.CountClusters(true, "default", "index", "ouser")
	Nil(t, err)
	True(t, cnt1 > 0, "should be clusters")

	cnt2, err := db.CountClusters(false, "default", "index", "ouser")
	Nil(t, err)
	True(t, cnt1 >= cnt2, "counts should match or have more deleted")

	begin, end, err := db.GetClusterDataRange("ouser")
	Nil(t, err)
	True(t, end >= begin, "begin and end of ClusterDataRange")

	recs, err := db.SQLCommand(nil, "CREATE CLUSTER CatUSA")
	Nil(t, err)
	ival := recint(recs)
	True(t, ival > 5, fmt.Sprintf("Unexpected value of ival: %d", ival))

	recs, err = db.SQLCommand(nil, "ALTER CLUSTER CatUSA Name CatAmerica")
	Nil(t, err)

	recs, err = db.SQLCommand(nil, "DROP CLUSTER CatUSA")
	Nil(t, err)
	Equals(t, false, recbool(recs))

	recs, err = db.SQLCommand(nil, "DROP CLUSTER CatAmerica")
	Nil(t, err)
	Equals(t, true, recbool(recs))

	clusterID, err := db.AddCluster("bigapple")
	Nil(t, err)
	True(t, clusterID > 0, "clusterID should be bigger than zero")

	// TODO: this will potentially use another connection and internal cluster lookup will fail - FIX!
	//	cnt, err := db.CountClusters(false, "bigapple")
	//	Nil(t, err)
	//	Equals(t, 0, int(cnt)) // should be no records in bigapple cluster
	// TODO: same as above
	//	err = db.DropCluster("bigapple")
	//	Nil(t, err)

	// this time it should return an error
	err = db.DropCluster("bigapple")
	True(t, err != nil, "DropCluster should return error when cluster doesn't exist")
}

func TestConcurrentClients(t *testing.T) {
	const N = 5
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	SeedDB(t, db)

	// ---[ queries and insertions concurrently ]---

	var wg sync.WaitGroup

	sql := `select count(*) from Cat where caretaker like 'Eva%'`
	recs, err := db.SQLQuery(nil, nil, sql)
	Nil(t, err)
	docs, err := recs.AsDocuments()
	Nil(t, err)
	beforeCount := toInt(docs[0].GetField("count").Value)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doQueriesAndInsertions(t, db, i)
		}(i)
	}

	wg.Wait()

	sql = `select count(*) from Cat where caretaker like 'Eva%'`
	recs, err = db.SQLQuery(nil, nil, sql)
	Nil(t, err)
	docs, err = recs.AsDocuments()
	Nil(t, err)
	afterCount := toInt(docs[0].GetField("count").Value)
	Equals(t, beforeCount, afterCount)
}

func doQueriesAndInsertions(t *testing.T, db orient.Database, id int) {
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	nreps := 1000
	ridsToDelete := make([]string, 0, nreps)

	for i := 0; i < nreps; i++ {
		randInt := rnd.Intn(3)
		if randInt > 0 {
			time.Sleep(time.Duration(randInt) * time.Millisecond)
		}

		if (i+randInt)%2 == 0 {
			sql := fmt.Sprintf(`insert into Cat set name="Bar", age=%d, caretaker="Eva%d"`, 20+id, id)
			recs, err := db.SQLCommand(nil, sql)
			Nil(t, err)
			docs, err := recs.AsDocuments()
			Nil(t, err)
			Equals(t, 1, len(docs))
			ridsToDelete = append(ridsToDelete, docs[0].RID.String())
		} else {
			sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
			recs, err := db.SQLQuery(nil, nil, sql)
			Nil(t, err)
			docs, err := recs.AsDocuments()
			Nil(t, err)
			Equals(t, toInt(docs[0].GetField("count").Value), len(ridsToDelete))
		}
	}

	//t.Logf("records insert by goroutine %d: %v", id, len(ridsToDelete))

	// ---[ clean up ]---

	for _, rid := range ridsToDelete {
		_, err := db.SQLCommand(nil, `delete from Cat where @rid=`+rid)
		Nil(t, err)
	}
	sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
	recs, err := db.SQLQuery(nil, nil, sql)
	Nil(t, err)
	docs, err := recs.AsDocuments()
	Nil(t, err)
	Equals(t, toInt(docs[0].GetField("count").Value), 0)
}
