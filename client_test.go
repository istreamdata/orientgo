package orient_test

import (
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/istreamdata/orientgo.v2"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func catch() {
	if r := recover(); r != nil {
		log.Printf("panic recovery: %v\nTrace:\n%s\n", r, debug.Stack())
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
	gratefulTestPath, ok := mapDBs["default"]
	True(t, ok, "default not in DB list")
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

	if orientVersion >= "2.1" { // error: Database 'plocal:/opt/orient/databases/ogonoriTest' is closed
		mapDBs, err = sess.ListDatabases()
		Nil(t, err)
		ogonoriTestPath, ok := mapDBs[dbDocumentName]
		True(t, ok, dbDocumentName+" not in DB list")
		True(t, strings.HasPrefix(ogonoriTestPath, "plocal"), "plocal prefix for db path")
	}
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

func TestRecordsNativeAPIStructs(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	type Cat struct {
		Name      string `mapstructure:"name"`
		CareTaker string `mapstructure:"caretaker"`
		Age       int32  `mapstructure:"age"`
	}
	catWinston := Cat{
		Name:      "Winston",
		CareTaker: "Churchill",
		Age:       7,
	}

	// ---[ creation ]---

	winston := orient.NewDocument("Cat")
	err := winston.From(catWinston)
	Nil(t, err)
	Equals(t, -1, int(winston.RID.ClusterID))
	Equals(t, -1, int(winston.RID.ClusterPos))
	Equals(t, -1, int(winston.Vers))
	err = db.CreateRecord(winston)
	Nil(t, err)
	True(t, int(winston.RID.ClusterID) > -1, "RID should be filled in")
	True(t, int(winston.RID.ClusterPos) > -1, "RID should be filled in")
	True(t, int(winston.Vers) > -1, "Version should be filled in")

	// ---[ update STRING and INTEGER field ]---

	versionBefore := winston.Vers
	catWinston.CareTaker = "Lolly"
	catWinston.Age = 8
	err = winston.From(catWinston) // this updates the fields locally
	Nil(t, err)

	err = db.UpdateRecord(winston) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < winston.Vers, "version should have incremented")

	var cats []Cat
	err = db.Command(orient.NewSQLQuery("select * from Cat where @rid=" + winston.RID.String())).All(&cats)
	Nil(t, err)
	Equals(t, 1, len(cats))

	winstonFromQuery := cats[0]
	Equals(t, catWinston, winstonFromQuery)

	// ---[ next creation ]---
	catDaemon := Cat{Name: "Daemon", CareTaker: "Matt", Age: 4}

	daemon := orient.NewDocument("Cat")
	err = daemon.From(catDaemon)
	Nil(t, err)
	err = db.CreateRecord(daemon)
	Nil(t, err)

	catIndy := Cat{Name: "Indy", Age: 6}

	indy := orient.NewDocument("Cat")
	err = indy.From(catIndy)
	Nil(t, err)
	err = db.CreateRecord(indy)
	Nil(t, err)

	sql := fmt.Sprintf("select from Cat where @rid=%s or @rid=%s or @rid=%s ORDER BY name",
		winston.RID, daemon.RID, indy.RID)
	cats = nil
	err = db.Command(orient.NewSQLQuery(sql)).All(&cats)
	Nil(t, err)
	Equals(t, 3, len(cats))
	Equals(t, catDaemon, cats[0])
	Equals(t, catIndy, cats[1])
	Equals(t, catWinston, cats[2])

	sql = fmt.Sprintf("DELETE FROM [%s, %s, %s]", winston.RID, daemon.RID, indy.RID)
	err = db.Command(orient.NewSQLCommand(sql)).Err()
	Nil(t, err)
}

func TestRecordsNativeAPI(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	// ---[ creation ]---

	winston := orient.NewDocument("Cat")
	winston.SetField("name", "Winston").
		SetField("caretaker", "Churchill").
		SetFieldWithType("age", 7, orient.INTEGER)
	Equals(t, -1, int(winston.RID.ClusterID))
	Equals(t, -1, int(winston.RID.ClusterPos))
	Equals(t, -1, int(winston.Vers))
	err := db.CreateRecord(winston)
	Nil(t, err)
	True(t, int(winston.RID.ClusterID) > -1, "RID should be filled in")
	True(t, int(winston.RID.ClusterPos) > -1, "RID should be filled in")
	True(t, int(winston.Vers) > -1, "Version should be filled in")

	// ---[ update STRING and INTEGER field ]---

	versionBefore := winston.Vers

	winston.SetField("caretaker", "Lolly") // this updates the field locally
	winston.SetField("age", 8)             // this updates the field locally
	err = db.UpdateRecord(winston)         // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < winston.Vers, "version should have incremented")

	var docs []*orient.Document
	err = db.Command(orient.NewSQLQuery("select * from Cat where @rid=" + winston.RID.String())).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))

	winstonFromQuery := docs[0]
	Equals(t, "Winston", winstonFromQuery.GetField("name").Value)
	Equals(t, 8, toInt(winstonFromQuery.GetField("age").Value))
	Equals(t, "Lolly", winstonFromQuery.GetField("caretaker").Value)

	// ---[ next creation ]---

	daemon := orient.NewDocument("Cat")
	daemon.SetField("name", "Daemon").SetField("caretaker", "Matt").SetField("age", 4)
	err = db.CreateRecord(daemon)
	Nil(t, err)

	indy := orient.NewDocument("Cat")
	indy.SetField("name", "Indy").SetField("age", 6)
	err = db.CreateRecord(indy)
	Nil(t, err)

	sql := fmt.Sprintf("select from Cat where @rid=%s or @rid=%s or @rid=%s ORDER BY name",
		winston.RID, daemon.RID, indy.RID)
	docs = nil
	err = db.Command(orient.NewSQLQuery(sql)).All(&docs)
	Nil(t, err)
	Equals(t, 3, len(docs))
	Equals(t, daemon.RID, docs[0].RID)
	Equals(t, indy.RID, docs[1].RID)
	Equals(t, winston.RID, docs[2].RID)

	Equals(t, indy.Vers, docs[1].Vers)
	Equals(t, "Matt", docs[0].GetField("caretaker").Value)

	sql = fmt.Sprintf("DELETE FROM [%s, %s, %s]", winston.RID, daemon.RID, indy.RID)
	err = db.Command(orient.NewSQLCommand(sql)).Err()
	Nil(t, err)

	// ---[ Test Boolean, Byte and Short Serialization ]---
	//createAndUpdateRecordsWithBooleanByteAndShort(dbc)

	// ---[ Test Int, Long, Float and Double Serialization ]---
	//createAndUpdateRecordsWithIntLongFloatAndDouble(dbc)

	// ---[ Test BINARY Serialization ]---
	//createAndUpdateRecordsWithBINARYType(dbc)

	// ---[ Test EMBEDDEDRECORD Serialization ]---
	//createAndUpdateRecordsWithEmbeddedRecords(dbc)

	// ---[ Test EMBEDDEDLIST, EMBEDDEDSET Serialization ]---
	//createAndUpdateRecordsWithEmbeddedLists(dbc, orient.EMBEDDEDLIST)
	//createAndUpdateRecordsWithEmbeddedLists(dbc, orient.EMBEDDEDSET)

	// ---[ Test Link Serialization ]---
	//createAndUpdateRecordsWithLinks(dbc)

	// ---[ Test LinkList/LinkSet Serialization ]---
	//createAndUpdateRecordsWithLinkLists(dbc, orient.LINKLIST)
	// createAndUpdateRecordsWithLinkLists(dbc, orient.LINKSET)  // TODO: get this working

	// ---[ Test LinkMap Serialization ]---
	//createAndUpdateRecordsWithLinkMap(dbc)
}

func TestRecordsWithDate(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	err := db.Command(orient.NewSQLCommand("CREATE PROPERTY Cat.bday DATE")).Err()
	Nil(t, err)

	const dtTemplate = "Jan 2, 2006 at 3:04pm (MST)"
	bdayTm, err := time.Parse(dtTemplate, "Feb 3, 1932 at 7:54pm (EST)")
	Nil(t, err)

	jj := orient.NewDocument("Cat")
	jj.SetField("name", "JJ").
		SetField("age", 2).
		SetFieldWithType("bday", bdayTm, orient.DATE)
	err = db.CreateRecord(jj)
	Nil(t, err)

	True(t, jj.RID.ClusterID > 0, "ClusterID should be set")
	True(t, jj.RID.ClusterPos >= 0, "ClusterID should be set")
	jjbdayAfterCreate := jj.GetField("bday").Value.(time.Time)
	Equals(t, 0, jjbdayAfterCreate.Hour())
	Equals(t, 0, jjbdayAfterCreate.Minute())
	Equals(t, 0, jjbdayAfterCreate.Second())

	var docs []*orient.Document
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid=" + jj.RID.String())).All(&docs)
	Equals(t, 1, len(docs))
	jjFromQuery := docs[0]
	Equals(t, jj.RID, jjFromQuery.RID)
	Equals(t, 1932, jjFromQuery.GetField("bday").Value.(time.Time).Year())

	// ---[ update ]---
	versionBefore := jj.Vers
	oneYearLater := bdayTm.AddDate(1, 0, 0)

	jj.SetFieldWithType("bday", oneYearLater, orient.DATE) // updates the field locally
	err = db.UpdateRecord(jj)                              // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < jj.Vers, "version should have incremented")

	docs = nil
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid=" + jj.RID.String())).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))
	jjFromQuery = docs[0]
	Equals(t, 1933, jjFromQuery.GetField("bday").Value.(time.Time).Year())
}

func TestRecordsWithDatetime(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	err := db.Command(orient.NewSQLCommand("CREATE PROPERTY Cat.ddd DATETIME")).Err()
	Nil(t, err)

	// ---[ creation ]---

	now := time.Now()
	now = time.Unix(now.Unix(), int64((now.Nanosecond()/1e6))*1e6)
	simba := orient.NewDocument("Cat")
	simba.SetField("name", "Simba").
		SetField("age", 11).
		SetFieldWithType("ddd", now, orient.DATETIME)
	err = db.CreateRecord(simba)
	Nil(t, err)

	True(t, simba.RID.ClusterID > 0, "ClusterID should be set")
	True(t, simba.RID.ClusterPos >= 0, "ClusterID should be set")

	var docs []*orient.Document
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid=" + simba.RID.String())).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))
	simbaFromQuery := docs[0]
	Equals(t, simba.RID, simbaFromQuery.RID)
	Equals(t, simba.GetField("ddd").Value, simbaFromQuery.GetField("ddd").Value)

	// ---[ update ]---

	versionBefore := simba.Vers

	twoDaysAgo := now.AddDate(0, 0, -2)

	simba.SetFieldWithType("ddd", twoDaysAgo, orient.DATETIME) // updates the field locally
	err = db.UpdateRecord(simba)                               // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < simba.Vers, "version should have incremented")

	docs = nil
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid=" + simba.RID.String())).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))
	simbaFromQuery = docs[0]
	Equals(t, twoDaysAgo.Unix(), simbaFromQuery.GetField("ddd").Value.(time.Time).Unix())
}

func TestRecordsMismatchedTypes(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	c1 := orient.NewDocument("Cat")
	c1.SetField("name", "fluffy1").
		SetField("age", 22).
		SetFieldWithType("ddd", "not a datetime", orient.DATETIME)
	err := db.CreateRecord(c1)
	True(t, err != nil, "Should have returned error")
	//	_, ok := oerror.ExtractCause(err).(oerror.ErrDataTypeMismatch)
	//	True(t, ok, "should be DataTypeMismatch error")

	c2 := orient.NewDocument("Cat")
	c2.SetField("name", "fluffy1").
		SetField("age", 22).
		SetFieldWithType("ddd", float32(33244.2), orient.DATE)
	err = db.CreateRecord(c2)
	True(t, err != nil, "Should have returned error")
	//	_, ok = oerror.ExtractCause(err).(oerror.ErrDataTypeMismatch)
	//	True(t, ok, "should be DataTypeMismatch error")

	// no fluffy1 should be in the database
	var docs []*orient.Document
	err = db.Command(orient.NewSQLQuery("select from Cat where name = 'fluffy1'")).All(&docs)
	Nil(t, err)
	Equals(t, 0, len(docs))
}

func TestRecordsBasicTypes(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	for _, cmd := range []string{
		"CREATE PROPERTY Cat.x BOOLEAN",
		"CREATE PROPERTY Cat.y BYTE",
		"CREATE PROPERTY Cat.z SHORT",
	} {
		err := db.Command(orient.NewSQLCommand(cmd)).Err()
		Nil(t, err)
	}

	cat := orient.NewDocument("Cat")
	cat.SetField("name", "kitteh").
		SetField("age", 4).
		SetField("x", false).
		SetField("y", byte(55)).
		SetField("z", int16(5123))

	err := db.CreateRecord(cat)
	Nil(t, err)
	True(t, cat.RID.ClusterID > 0, "RID should be filled in")

	var docs []*orient.Document
	err = db.Command(orient.NewSQLQuery("select from Cat where y = 55")).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery := docs[0]
	Equals(t, cat.GetField("x").Value.(bool), catFromQuery.GetField("x").Value.(bool))
	Equals(t, cat.GetField("y").Value.(byte), catFromQuery.GetField("y").Value.(byte))
	Equals(t, cat.GetField("z").Value.(int16), catFromQuery.GetField("z").Value.(int16))

	// try with explicit types
	cat2 := orient.NewDocument("Cat")
	cat2.SetField("name", "cat2").
		SetField("age", 14).
		SetFieldWithType("x", true, orient.BOOLEAN).
		SetFieldWithType("y", byte(44), orient.BYTE).
		SetFieldWithType("z", int16(16000), orient.SHORT)

	err = db.CreateRecord(cat2)
	Nil(t, err)
	True(t, cat2.RID.ClusterID > 0, "RID should be filled in")

	docs = nil
	err = db.Command(orient.NewSQLQuery("select from Cat where x = true")).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))

	cat2FromQuery := docs[0]
	Equals(t, cat2.GetField("x").Value.(bool), cat2FromQuery.GetField("x").Value.(bool))
	Equals(t, cat2.GetField("y").Value.(byte), cat2FromQuery.GetField("y").Value.(byte))
	Equals(t, cat2.GetField("z").Value.(int16), cat2FromQuery.GetField("z").Value.(int16))

	// ---[ update ]---

	versionBefore := cat.Vers

	cat.SetField("x", true)
	cat.SetField("y", byte(19))
	cat.SetField("z", int16(6789))

	err = db.UpdateRecord(cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Vers, "version should have incremented")

	docs = nil
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid=" + cat.RID.String())).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]
	Equals(t, true, catFromQuery.GetField("x").Value)
	Equals(t, byte(19), catFromQuery.GetField("y").Value)
	Equals(t, int16(6789), catFromQuery.GetField("z").Value)
}

func TestRecordsBinaryField(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	err := db.Command(orient.NewSQLCommand("CREATE PROPERTY Cat.bin BINARY")).Err()
	Nil(t, err)

	// ---[ FieldWithType ]---
	str := "four, five, six, pick up sticks"
	bindata := []byte(str)

	cat := orient.NewDocument("Cat")
	cat.SetField("name", "little-jimmy").
		SetField("age", 1).
		SetFieldWithType("bin", bindata, orient.BINARY)

	err = db.CreateRecord(cat)
	Nil(t, err)
	True(t, cat.RID.ClusterID > 0, "RID should be filled in")

	var docs []*orient.Document
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid = ?", cat.RID)).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))

	catFromQuery := docs[0]

	Equals(t, cat.GetField("bin").Value, catFromQuery.GetField("bin").Value)
	Equals(t, str, string(catFromQuery.GetField("bin").Value.([]byte)))

	// ---[ Field No Type Specified ]---
	binN := 10 * 1024 * 1024
	bindata2 := make([]byte, binN)

	for i := 0; i < binN; i++ {
		bindata2[i] = byte(i)
	}

	cat2 := orient.NewDocument("Cat")
	cat2.SetField("name", "Sauron").
		SetField("age", 1111).
		SetField("bin", bindata2)

	True(t, cat2.RID.ClusterID <= 0, "RID should NOT be filled in")

	err = db.CreateRecord(cat2)
	Nil(t, err)
	True(t, cat2.RID.ClusterID > 0, "RID should be filled in")

	docs = nil
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid = ?", cat2.RID)).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))
	cat2FromQuery := docs[0]

	Equals(t, bindata2, cat2FromQuery.GetField("bin").Value.([]byte))

	// ---[ update ]---

	versionBefore := cat.Vers

	newbindata := []byte("Now Gluten Free!")
	cat.SetFieldWithType("bin", newbindata, orient.BINARY)
	err = db.UpdateRecord(cat) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < cat.Vers, "version should have incremented")

	docs = nil
	err = db.Command(orient.NewSQLQuery("select from Cat where @rid=" + cat.RID.String())).All(&docs)
	Nil(t, err)
	Equals(t, 1, len(docs))
	catFromQuery = docs[0]
	Equals(t, newbindata, catFromQuery.GetField("bin").Value)
}

func recordAsDocument(t *testing.T, rec orient.ORecord) *orient.Document {
	doc, ok := rec.(*orient.Document)
	if !ok {
		t.Fatalf("expected document, got: %T", rec)
	}
	return doc
}

func TestRecordBytes(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()

	rec := orient.NewBytesRecord()

	// ---[ FieldWithType ]---
	str := "four, five, six, pick up sticks"
	bindata := []byte(str)

	rec.Data = bindata

	err := db.CreateRecord(rec)
	Nil(t, err)
	True(t, rec.RID.ClusterID > 0, "RID should be filled in")

	rrec, err := db.GetRecordByRID(rec.RID, "", true)
	Nil(t, err)
	recFromQuery := rrec.(*orient.BytesRecord)

	Equals(t, rec.Data, recFromQuery.Data)
	Equals(t, str, string(recFromQuery.Data))

	// ---[ Field No Type Specified ]---
	binN := 10 * 1024 * 1024
	bindata2 := make([]byte, binN)

	for i := 0; i < binN; i++ {
		bindata2[i] = byte(i)
	}

	rec2 := orient.NewBytesRecord()
	rec2.Data = bindata2

	True(t, rec2.RID.ClusterID <= 0, "RID should NOT be filled in")

	err = db.CreateRecord(rec2)
	Nil(t, err)
	True(t, rec2.RID.ClusterID > 0, "RID should be filled in")

	rrec, err = db.GetRecordByRID(rec2.RID, "", true)
	Nil(t, err)
	rec2FromQuery := rrec.(*orient.BytesRecord)

	Equals(t, bindata2, rec2FromQuery.Data)

	// ---[ update ]---

	versionBefore := rec.Vers

	newbindata := []byte("Now Gluten Free!")
	rec.Data = newbindata
	err = db.UpdateRecord(rec) // update the field in the remote DB
	Nil(t, err)
	True(t, versionBefore < rec.Vers, "version should have incremented")

	rrec, err = db.GetRecordByRID(rec.RID, "", true)
	Nil(t, err)
	recFromQuery = rrec.(*orient.BytesRecord)
	Equals(t, newbindata, recFromQuery.Data)
}

func TestCommandsNativeAPI(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	var (
		rec orient.ORecord
		doc *orient.Document
		err error

		retint int
		docs   []*orient.Document
	)

	resetVars := func() {
		docs = nil
		retint = 0
	}

	sqlCommand := func(sql string, params ...interface{}) {
		err := db.Command(orient.NewSQLCommand(sql, params...)).Err()
		Nil(t, err)
	}

	sqlCommandAll := func(sql string, out interface{}, params ...interface{}) {
		resetVars()
		err := db.Command(orient.NewSQLCommand(sql, params...)).All(out)
		Nil(t, err)
	}

	sqlCommandErr := func(sql string, params ...interface{}) {
		err := db.Command(orient.NewSQLCommand(sql, params...)).Err()
		True(t, err != nil, "should be error: ", sql)
		switch err.(type) {
		case orient.OServerException:
		default:
			t.Fatal(fmt.Errorf("error should have been a oerror.OServerException but was: %T: %v", err, err))
		}
	}

	sqlQueryAll := func(sql string, out interface{}, params ...interface{}) {
		resetVars()
		err := db.Command(orient.NewSQLQuery(sql, params...)).All(out)
		Nil(t, err)
	}

	sqlQueryPlanAll := func(sql string, plan orient.FetchPlan, out interface{}, params ...interface{}) {
		resetVars()
		err := db.Command(orient.NewSQLQuery(sql, params...).FetchPlan(plan)).All(out)
		Nil(t, err)
	}

	// ---[ query from the ogonoriTest database ]---

	sqlQueryAll("select from Cat where name = ?", &docs, "Linus")

	linusDocRID := docs[0].RID

	True(t, linusDocRID.IsValid(), "linusDocRID should not be nil")
	True(t, docs[0].Vers > 0, fmt.Sprintf("Version is: %d", docs[0].Vers))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Cat", docs[0].ClassName())

	nameField := docs[0].GetField("name")
	True(t, nameField != nil, "should be a 'name' field")

	ageField := docs[0].GetField("age")
	True(t, ageField != nil, "should be a 'age' field")

	caretakerField := docs[0].GetField("caretaker")
	True(t, caretakerField != nil, "should be a 'caretaker' field")

	Equals(t, orient.STRING, nameField.Type)
	Equals(t, orient.STRING, caretakerField.Type)
	Equals(t, orient.INTEGER, ageField.Type)
	Equals(t, "Linus", nameField.Value)
	Equals(t, int32(15), ageField.Value)
	Equals(t, "Michael", caretakerField.Value)

	// ---[ get by RID ]---
	rec, err = db.GetRecordByRID(linusDocRID, "", true)
	Nil(t, err)
	doc = recordAsDocument(t, rec)
	docByRID := doc
	Equals(t, linusDocRID, docByRID.RID)
	True(t, docByRID.Vers > 0, fmt.Sprintf("Version is: %d", docByRID.Vers))
	Equals(t, 3, len(docByRID.FieldNames()))
	Equals(t, "Cat", docByRID.ClassName())

	nameField = docByRID.GetField("name")
	True(t, nameField != nil, "should be a 'name' field")

	ageField = docByRID.GetField("age")
	True(t, ageField != nil, "should be a 'age' field")

	caretakerField = docByRID.GetField("caretaker")
	True(t, caretakerField != nil, "should be a 'caretaker' field")

	Equals(t, orient.STRING, nameField.Type)
	Equals(t, orient.INTEGER, ageField.Type)
	Equals(t, orient.STRING, caretakerField.Type)
	Equals(t, "Linus", nameField.Value)
	Equals(t, int32(15), ageField.Value)
	Equals(t, "Michael", caretakerField.Value)

	// ---[ cluster data range ]---
	//	begin, end, err := db.FetchClusterDataRange("cat")
	//	Nil(t, err)
	//	glog.Infof("begin = %v; end = %v\n", begin, end)

	sqlCommand(`insert into Cat (name, age, caretaker) values("Zed", 3, "Shaw")`)

	// ---[ query after inserting record(s) ]---

	sqlQueryAll("select * from Cat order by name asc", &docs)
	Equals(t, 3, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Cat", docs[0].ClassName())
	Equals(t, 3, len(docs[1].FieldNames()))
	Equals(t, "Cat", docs[1].ClassName())
	Equals(t, 3, len(docs[2].FieldNames()))
	Equals(t, "Cat", docs[2].ClassName())

	keiko := docs[0]
	Equals(t, "Keiko", keiko.GetField("name").Value)
	Equals(t, int32(10), keiko.GetField("age").Value)
	Equals(t, "Anna", keiko.GetField("caretaker").Value)
	Equals(t, orient.STRING, keiko.GetField("caretaker").Type)
	True(t, keiko.Vers > 0, "Version should be greater than zero")
	True(t, keiko.RID.IsValid(), "RID should be filled in")

	linus := docs[1]
	Equals(t, "Linus", linus.GetField("name").Value)
	Equals(t, int32(15), linus.GetField("age").Value)
	Equals(t, "Michael", linus.GetField("caretaker").Value)

	zed := docs[2]
	Equals(t, "Zed", zed.GetField("name").Value)
	Equals(t, int32(3), zed.GetField("age").Value)
	Equals(t, "Shaw", zed.GetField("caretaker").Value)
	Equals(t, orient.STRING, zed.GetField("caretaker").Type)
	Equals(t, orient.INTEGER, zed.GetField("age").Type)
	True(t, zed.Vers > 0, "Version should be greater than zero")
	True(t, zed.RID.IsValid(), "RID should be filled in")

	sqlQueryAll("select name, caretaker from Cat order by caretaker", &docs)
	Equals(t, 3, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].ClassName()) // property queries do not come back with Classname set
	Equals(t, 2, len(docs[1].FieldNames()))
	Equals(t, "", docs[1].ClassName())
	Equals(t, 2, len(docs[2].FieldNames()))

	Equals(t, "Anna", docs[0].GetField("caretaker").Value)
	Equals(t, "Michael", docs[1].GetField("caretaker").Value)
	Equals(t, "Shaw", docs[2].GetField("caretaker").Value)

	Equals(t, "Keiko", docs[0].GetField("name").Value)
	Equals(t, "Linus", docs[1].GetField("name").Value)
	Equals(t, "Zed", docs[2].GetField("name").Value)

	Equals(t, "name", docs[0].GetField("name").Name)

	// ---[ delete newly added record(s) ]---
	err = db.DeleteRecordByRID(zed.RID, zed.Vers)
	Nil(t, err)

	// glog.Infoln("Deleting (Async) record #11:4")
	// err = dbc.DeleteRecordByRIDAsync(dbc, "11:4", 1)
	// if err != nil {
	// 	Fatal(err)
	// }

	sqlCommand("insert into Cat (name, age, caretaker) values(?, ?, ?)", "June", 8, "Cleaver")

	sqlQueryAll("select name, age from Cat where caretaker = ?", &docs, "Cleaver")
	Equals(t, 1, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].ClassName()) // property queries do not come back with Classname set
	Equals(t, "June", docs[0].GetField("name").Value)
	Equals(t, int32(8), docs[0].GetField("age").Value)

	sqlCommandAll("select caretaker, name, age from Cat where age > ? order by age desc", &docs, 9)
	Equals(t, 2, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "", docs[0].ClassName()) // property queries do not come back with Classname set
	Equals(t, "Linus", docs[0].GetField("name").Value)
	Equals(t, int32(15), docs[0].GetField("age").Value)
	Equals(t, "Keiko", docs[1].GetField("name").Value)
	Equals(t, int32(10), docs[1].GetField("age").Value)
	Equals(t, "Anna", docs[1].GetField("caretaker").Value)

	sqlCommand("delete from Cat where name = ?")

	// START: Basic DDL

	sqlCommand("DROP CLASS Patient")
	//Equals(t, true, retbool)

	// ------

	retint = 0
	sqlCommandAll("CREATE CLASS Patient", &retint)
	True(t, retint > 10, "classnum should be greater than 10 but was: ", retint)

	defer func() {
		err = db.Command(orient.NewSQLCommand("DROP CLASS Patient")).Err()
		if err != nil {
			log.Printf("WARN: clean up error: %v\n", err)
			return
		}

		// TRUNCATE after drop should return an OServerException type
		err = db.Command(orient.NewSQLCommand("TRUNCATE CLASS Patient")).Err()
		True(t, err != nil, "Error from TRUNCATE should not be null")

		switch err.(type) {
		case orient.OServerException:
		default:
			t.Fatal(fmt.Errorf("TRUNCATE error cause should have been a oerror.OServerException but was: %T: %v", err, err))
		}
	}()

	// ------

	sqlCommand("Create property Patient.name string")
	sqlCommand("alter property Patient.name min 3")
	sqlCommand("Create property Patient.married boolean")

	err = db.ReloadSchema()
	Nil(t, err)

	sqlCommand("INSERT INTO Patient (name, married) VALUES ('Hank', 'true')")
	sqlCommand("TRUNCATE CLASS Patient")
	sqlCommand("INSERT INTO Patient (name, married) VALUES ('Hank', 'true'), ('Martha', 'false')")

	sqlQueryAll("SELECT count(*) from Patient", &docs)
	Equals(t, 1, len(docs))
	fldCount := docs[0].GetField("count")
	Equals(t, int64(2), fldCount.Value)

	sqlCommand("CREATE PROPERTY Patient.gender STRING")
	sqlCommand("ALTER PROPERTY Patient.gender REGEXP [M|F]")
	sqlCommand("INSERT INTO Patient (name, married, gender) VALUES ('Larry', 'true', 'M'), ('Shirley', 'false', 'F')")
	sqlCommandErr("INSERT INTO Patient (name, married, gender) VALUES ('Lt. Dan', 'true', 'T'), ('Sally', 'false', 'F')")

	sqlQueryAll("SELECT FROM Patient ORDER BY @rid desc", &docs)
	Equals(t, 4, len(docs))
	Equals(t, "Shirley", docs[0].GetField("name").Value)

	sqlCommand("ALTER PROPERTY Patient.gender NAME sex")

	err = db.ReloadSchema()
	Nil(t, err)

	sqlCommand("DROP PROPERTY Patient.sex")

	sqlQueryAll("select from Patient order by RID", &docs)
	Equals(t, 4, len(docs))
	Equals(t, 2, len(docs[0].Fields())) // has name and married
	Equals(t, "Hank", docs[0].Fields()["name"].Value)

	Equals(t, 4, len(docs[3].Fields())) // has name, married, sex and for some reason still has `gender`
	Equals(t, "Shirley", docs[3].Fields()["name"].Value)
	Equals(t, "F", docs[3].Fields()["gender"].Value)

	sqlCommand("TRUNCATE CLASS Patient")

	// ---[ Attempt to create, insert and read back EMBEDDEDLIST types ]---

	sqlCommandAll("CREATE PROPERTY Patient.tags EMBEDDEDLIST STRING", &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")

	sqlCommandAll(`insert into Patient (name, married, tags) values ("George", "false", ["diabetic", "osteoarthritis"])`, &docs)
	Equals(t, 1, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))

	sqlQueryAll(`SELECT from Patient where name = ?`, &docs, "George")
	Equals(t, 1, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	embListTagsField := docs[0].GetField("tags")

	embListTags := embListTagsField.Value.([]interface{})
	Equals(t, 2, len(embListTags))
	Equals(t, "diabetic", embListTags[0].(string))
	Equals(t, "osteoarthritis", embListTags[1].(string))

	// ---[ try JSON content insertion notation ]---

	sqlCommandAll(`insert into Patient content {"name": "Freddy", "married":false}`, &docs)
	Equals(t, 1, len(docs))
	Equals(t, "Freddy", docs[0].GetField("name").Value)
	Equals(t, false, docs[0].GetField("married").Value)

	// ---[ Try LINKs ! ]---

	sqlQueryAll(`select from Cat WHERE name = 'Linus' OR name='Keiko' ORDER BY @rid`, &docs)
	Equals(t, 2, len(docs))
	linusRID := docs[0].RID
	keikoRID := docs[1].RID

	sqlCommandAll(`CREATE PROPERTY Cat.buddy LINK`, &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")
	defer removeProperty(db, "Cat", "buddy")

	sqlCommandAll(`insert into Cat SET name='Tilde', age=8, caretaker='Earl', buddy=(SELECT FROM Cat WHERE name = 'Linus')`, &docs)
	Equals(t, 1, len(docs))
	Equals(t, "Tilde", docs[0].GetField("name").Value)
	Equals(t, 8, int(docs[0].GetField("age").Value.(int32)))
	Equals(t, linusRID, docs[0].GetField("buddy").Value.(orient.RID))

	tildeRID := docs[0].RID

	// ---[ Test EMBEDDED ]---
	sqlCommand(`CREATE PROPERTY Cat.embeddedCat EMBEDDED`)
	defer removeProperty(db, "Cat", "embeddedCat")

	emb := `{"name": "Spotty", "age": 2, emb: {"@type": "d", "@class":"Cat", "name": "yowler", "age":13}}`
	sqlCommandAll("insert into Cat content "+emb, &docs)
	Equals(t, 1, len(docs))
	Equals(t, "Spotty", docs[0].GetField("name").Value)
	Equals(t, 2, int(docs[0].GetField("age").Value.(int32)))
	Equals(t, orient.EMBEDDED, docs[0].GetField("emb").Type)

	embCat := docs[0].GetField("emb").Value.(*orient.Document)
	Equals(t, "Cat", embCat.ClassName())
	True(t, embCat.Vers < 0, "Version should be unset")
	True(t, embCat.RID.ClusterID < 0, "RID.ClusterID should be unset")
	True(t, embCat.RID.ClusterPos < 0, "RID.ClusterPos should be unset")
	Equals(t, "yowler", embCat.GetField("name").Value.(string))
	Equals(t, int(13), toInt(embCat.GetField("age").Value))

	sqlCommand("delete from Cat where name = 'Spotty'")

	// ---[ Test LINKLIST ]---
	sqlCommandAll(`CREATE PROPERTY Cat.buddies LINKLIST`, &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")
	defer removeProperty(db, "Cat", "buddies")

	sqlCommandAll(`insert into Cat SET name='Felix', age=6, caretaker='Ed', buddies=(SELECT FROM Cat WHERE name = 'Linus' OR name='Keiko')`, &docs)
	Equals(t, 1, len(docs))
	Equals(t, "Felix", docs[0].GetField("name").Value)
	Equals(t, 6, int(docs[0].GetField("age").Value.(int32)))
	buddies := docs[0].GetField("buddies").Value.([]orient.OIdentifiable)
	sort.Sort(byRID(buddies))
	Equals(t, 2, len(buddies))
	Equals(t, linusRID, buddies[0].GetIdentity())
	Equals(t, keikoRID, buddies[1].GetIdentity())

	felixRID := docs[0].RID

	// ---[ Try LINKMAP ]---
	sqlCommandAll(`CREATE PROPERTY Cat.notes LINKMAP`, &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")
	defer removeProperty(db, "Cat", "notes")
	sqlCommandAll(fmt.Sprintf(`INSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": %s, '30': %s}`,
		linusRID, keikoRID), &docs)
	Equals(t, 1, len(docs))
	Equals(t, 4, len(docs[0].FieldNames()))
	Equals(t, "Anna", docs[0].GetField("caretaker").Value)
	Equals(t, linusRID, docs[0].GetField("notes").Value.(map[string]orient.OIdentifiable)["bff"].GetIdentity())
	Equals(t, keikoRID, docs[0].GetField("notes").Value.(map[string]orient.OIdentifiable)["30"].GetIdentity())

	//charlieRID := docs[0].RID

	// query with a fetchPlan that does NOT follow all the links
	sqlQueryPlanAll(`SELECT FROM Cat WHERE notes IS NOT NULL`, orient.NoFollow, &docs)
	Equals(t, 1, len(docs))
	doc = docs[0]
	Equals(t, "Charlie", doc.GetField("name").Value)
	notesField := doc.GetField("notes").Value.(map[string]orient.OIdentifiable)
	Equals(t, 2, len(notesField))

	bffNote := notesField["bff"]
	True(t, bffNote.GetIdentity().ClusterID != -1, "RID should be filled in")
	True(t, bffNote.GetRecord() == nil, "RID should be nil")

	thirtyNote := notesField["30"]
	True(t, thirtyNote.GetIdentity().ClusterID != -1, "RID should be filled in")
	True(t, thirtyNote.GetRecord() == nil, "RID should be nil")

	// query with a fetchPlan that does follow all the links
	// TODO: fix fetch plan
	/*
		sqlQueryPlanAll(`SELECT FROM Cat WHERE notes IS NOT NULL`, orient.FetchPlanFollowAll, &docs)
			True(t, len(docs) > 0)
			doc = docs[0]
			Equals(t, "Charlie", doc.GetField("name").Value)
			notesField = doc.GetField("notes").Value.(map[string]orient.OIdentifiable)
			Equals(t, 2, len(notesField))

			bffNote = notesField["bff"]
			True(t, bffNote.GetIdentity().ClusterID != -1, "RID should be filled in")
			True(t, bffNote.GetRecord() != nil, "Record should be filled in")
			Equals(t, "Linus", bffNote.GetRecord().(*orient.Document).GetField("name").Value)

			thirtyNote = notesField["30"]
			True(t, thirtyNote.GetIdentity().ClusterID != -1, "RID should be filled in")
			True(t, thirtyNote.GetRecord() != nil, "Record should be filled in")
			Equals(t, "Keiko", thirtyNote.GetRecord().(*orient.Document).GetField("name").Value)
	*/
	// ---[ Try LINKSET ]---
	sqlCommandAll(`CREATE PROPERTY Cat.buddySet LINKSET`, &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")
	defer removeProperty(db, "Cat", "buddySet")

	err = db.ReloadSchema() // good thing to do after modifying the schema
	Nil(t, err)

	// insert record with all the LINK types
	sql := `insert into Cat SET name='Germaine', age=2, caretaker='Minnie', ` +
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

	sqlCommandAll(sql, &docs)
	Equals(t, 1, len(docs))
	Equals(t, "Germaine", docs[0].GetField("name").Value)
	Equals(t, 2, int(docs[0].GetField("age").Value.(int32)))

	germaineRID := docs[0].RID

	buddyList := docs[0].GetField("buddies").Value.([]orient.OIdentifiable)
	sort.Sort(byRID(buddyList))
	Equals(t, 2, len(buddies))
	Equals(t, linusRID, buddyList[0].GetIdentity())
	Equals(t, keikoRID, buddyList[1].GetIdentity())

	buddySet := docs[0].GetField("buddySet").Value.([]orient.OIdentifiable)
	sort.Sort(byRID(buddySet))
	Equals(t, 2, len(buddySet))
	Equals(t, linusRID, buddySet[0].GetIdentity())
	Equals(t, felixRID, buddySet[1].GetIdentity())

	notesMap := docs[0].GetField("notes").Value.(map[string]orient.OIdentifiable)
	Equals(t, 2, len(buddies))
	Equals(t, keikoRID, notesMap["bff"].GetIdentity())
	Equals(t, linusRID, notesMap["30"].GetIdentity())

	// TODO: fix fetch plan
	/*
		// now query with fetchPlan that retrieves all links
		sql = `SELECT FROM Cat WHERE notes IS NOT NULL ORDER BY name`
		docs = nil
		recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAll, sql)
		Nil(t, err)
		Equals(t, 2, len(docs))
		Equals(t, "Charlie", docs[0].GetField("name").Value)
		Equals(t, "Germaine", docs[1].GetField("name").Value)
		Equals(t, "Minnie", docs[1].GetField("caretaker").Value)

		charlieNotesField := docs[0].GetField("notes").Value.(map[string]*orient.OLink)
		Equals(t, 2, len(charlieNotesField))

		bffNote = charlieNotesField["bff"]
		Equals(t, "Linus", bffNote.Record.GetField("name").Value)

		thirtyNote = charlieNotesField["30"]
		Equals(t, "Keiko", thirtyNote.Record.GetField("name").Value)

		// test Germaine's notes (LINKMAP)
		germaineNotesField := docs[1].GetField("notes").Value.(map[string]*orient.OLink)
		Equals(t, 2, len(germaineNotesField))

		bffNote = germaineNotesField["bff"]
		Equals(t, "Keiko", bffNote.Record.GetField("name").Value)

		thirtyNote = germaineNotesField["30"]
		Equals(t, "Linus", thirtyNote.Record.GetField("name").Value)

		// test Germaine's buddySet (LINKSET)
		germaineBuddySet := docs[1].GetField("buddySet").Value.([]*orient.OLink)
		sort.Sort(byRID(germaineBuddySet))
		Equals(t, "Linus", germaineBuddySet[0].Record.GetField("name").Value)
		Equals(t, "Felix", germaineBuddySet[1].Record.GetField("name").Value)
		True(t, germaineBuddySet[1].RID.ClusterID != -1, "RID should be filled in")

		// Felix Document has references, so those should also be filled in
		felixDoc := germaineBuddySet[1].Record
		felixBuddiesList := felixDoc.GetField("buddies").Value.([]*orient.OLink)
		sort.Sort(byRID(felixBuddiesList))
		Equals(t, 2, len(felixBuddiesList))
		True(t, felixBuddiesList[0].Record != nil, "Felix links should be filled in")
		Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)

		// test Germaine's buddies (LINKLIST)
		germaineBuddyList := docs[1].GetField("buddies").Value.([]*orient.OLink)
		sort.Sort(byRID(germaineBuddyList))
		Equals(t, "Linus", germaineBuddyList[0].Record.GetField("name").Value)
		Equals(t, "Keiko", germaineBuddyList[1].Record.GetField("name").Value)
		True(t, germaineBuddyList[0].RID.ClusterID != -1, "RID should be filled in")
	*/
	// now make a circular reference -> give Linus to Germaine as buddy
	sqlCommandAll(`UPDATE Cat SET buddy = `+germaineRID.String()+` where name = 'Linus'`, &retint)
	Equals(t, 1, retint)

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

	// TODO: fix fetch plan
	/*
		// ---[ queries with extended fetchPlan (simple case) ]---
		sql = `select * from Cat where name = 'Tilde'`
		docs = nil
		_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAll, sql)
		Nil(t, err)
		Equals(t, 1, len(docs))
		doc = docs[0]
		Equals(t, "Tilde", doc.GetField("name").Value)
		tildeBuddyField := doc.GetField("buddy").Value.(*orient.OLink)
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
		_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAll, sql)
		Nil(t, err)
		Equals(t, 2, len(docs))
		Equals(t, "Linus", docs[0].GetField("name").Value)
		Equals(t, "Tilde", docs[1].GetField("name").Value)

		linusBuddy := docs[0].GetField("buddy").Value.(*orient.OLink)
		True(t, linusBuddy.Record != nil, "Record should be filled in")
		Equals(t, "Germaine", linusBuddy.Record.GetField("name").Value)

		tildeBuddy := docs[1].GetField("buddy").Value.(*orient.OLink)
		True(t, tildeBuddy.Record != nil, "Record should be filled in")
		Equals(t, "Linus", tildeBuddy.Record.GetField("name").Value)

		// now check that Felix buddies were pulled in too
		felixDoc = linusBuddy.Record
		felixBuddiesList = felixDoc.GetField("buddies").Value.([]*orient.OLink)
		sort.Sort(byRID(felixBuddiesList))
		Equals(t, 2, len(felixBuddiesList))
		Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)
		Equals(t, "Keiko", felixBuddiesList[1].Record.GetField("name").Value)

		// Linus.buddy links to Felix
		// Felix.buddies links Linux and Keiko
		sql = `SELECT FROM Cat WHERE name = 'Linus' OR name = 'Felix' ORDER BY name DESC`
		docs = nil
		_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAll, sql)
		Nil(t, err)
		Equals(t, 2, len(docs))
		linusBuddy = docs[0].GetField("buddy").Value.(*orient.OLink)
		True(t, linusBuddy.Record != nil, "Record should be filled in")
		Equals(t, "Germaine", linusBuddy.Record.GetField("name").Value)

		True(t, docs[1].GetField("buddy") == nil, "Felix should have no 'buddy'")
		felixBuddiesList = docs[1].GetField("buddies").Value.([]*orient.OLink)
		sort.Sort(byRID(felixBuddiesList))
		Equals(t, "Linus", felixBuddiesList[0].Record.GetField("name").Value)
		Equals(t, "Keiko", felixBuddiesList[1].Record.GetField("name").Value)
		Equals(t, "Anna", felixBuddiesList[1].Record.GetField("caretaker").Value)

		// check that Felix's reference to Linus has Linus' link filled in
		Equals(t, "Germaine", felixBuddiesList[0].Record.GetField("buddy").Value.(*orient.OLink).Record.GetField("name").Value)

		// ------

		sql = `select * from Cat where buddies is not null ORDER BY name`
		docs = nil
		_, err = db.SQLQuery(&docs, orient.FetchPlanFollowAll, sql)
		Nil(t, err)
		Equals(t, 2, len(docs))
		felixDoc = docs[0]
		Equals(t, "Felix", felixDoc.GetField("name").Value)
		felixBuddiesList = felixDoc.GetField("buddies").Value.([]*orient.OLink)
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
		linusBuddyLink := linusDocViaFelix.GetField("buddy").Value.(*orient.OLink)
		Equals(t, "Germaine", linusBuddyLink.Record.GetField("name").Value)

		// ------
	*/
	// Create two records that reference only each other (a.buddy = b and b.buddy = a)
	//  do:  SELECT FROM Cat where name = "a" OR name = "b" with *:-1 fetchPlan
	//  and make sure if the LINK fields are filled in
	//  with the *:-1 fetchPlan, OrientDB server will return all the link docs in the
	//  "supplementary section" even if they are already in the primary docs section

	sqlCommandAll(`INSERT INTO Cat SET name='Tom', age=3`, &docs)
	Equals(t, 1, len(docs))
	tomRID := docs[0].RID
	True(t, tomRID.IsValid(), "RID should be filled in")

	sqlCommandAll(`INSERT INTO Cat SET name='Nick', age=4, buddy=?`, &docs, tomRID)
	Equals(t, 1, len(docs))
	nickRID := docs[0].RID

	sqlCommand(`UPDATE Cat SET buddy=? WHERE name='Tom' and age=3`, nickRID)

	err = db.ReloadSchema()
	Nil(t, err)
	// TODO: fix fetch plan
	/*
		// in this case the buddy links should be filled in with full Documents
		sql = `SELECT FROM Cat WHERE name=? OR name=? ORDER BY name desc`
		docs = nil
		recs, err = db.SQLQuery(&docs, orient.FetchPlanFollowAll, sql, "Tom", "Nick")
		Nil(t, err)
		Equals(t, 2, len(docs))
		tomDoc := docs[0]
		nickDoc := docs[1]
		Equals(t, "Tom", tomDoc.GetField("name").Value)
		Equals(t, "Nick", nickDoc.GetField("name").Value)

		// TODO: FIX

		//	// TODO: this section fails with orientdb-community-2.1-rc5
		//	tomsBuddy := tomDoc.GetField("buddy").Value.(*orient.OLink)
		//	nicksBuddy := nickDoc.GetField("buddy").Value.(*orient.OLink)
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
		//	tomsBuddy = tomDoc.GetField("buddy").Value.(*orient.OLink)
		//	nicksBuddy = nickDoc.GetField("buddy").Value.(*orient.OLink)
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
		buddies = docs[0].GetField("buddies").Value.([]*orient.OLink)
		sort.Sort(byRID(buddies))
		Equals(t, 2, len(buddies))
		linusDoc := buddies[0].Record
		True(t, linusDoc != nil, "first level should be filled in")
		linusBuddy = linusDoc.GetField("buddy").Value.(*orient.OLink)
		True(t, linusBuddy.RID.ClusterID != -1, "RID should be filled in")
		True(t, linusBuddy.Record == nil, "Record of second level should NOT be filled in")

		keikoDoc := buddies[1].Record
		True(t, keikoDoc != nil, "first level should be filled in")
	*/
	// ------

	// ---[ Try DATETIME ]---

	sqlCommandAll(`Create PROPERTY Cat.dt DATETIME`, &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")
	defer removeProperty(db, "Cat", "dt")

	sqlCommandAll(`Create PROPERTY Cat.birthday DATE`, &retint)
	True(t, retint >= 0, "retval from PROPERTY creation should be a positive number")
	defer removeProperty(db, "Cat", "birthday")

	// OrientDB DATETIME is precise to the millisecond
	sqlCommandAll(`INSERT into Cat SET name = 'Bruce', dt = '2014-11-25 09:14:54'`, &docs)
	Equals(t, 1, len(docs))
	Equals(t, "Bruce", docs[0].GetField("name").Value)

	dt := docs[0].GetField("dt").Value.(time.Time)
	zone, zoneOffset := dt.Zone()
	zoneLocation := time.FixedZone(zone, zoneOffset)
	expectedTm, err := time.Parse("2006-01-02 03:04:05", "2014-11-25 09:14:54") //time.ParseInLocation("2006-01-02 03:04:05", "2014-11-25 09:14:54", zoneLocation)
	Nil(t, err)
	Equals(t, expectedTm.Local().String(), dt.String())

	bruceRID := docs[0].RID

	sqlCommandAll(`INSERT into Cat SET name = 'Tiger', birthday = '2014-11-25'`, &docs)
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

	ridsToDelete := []interface{}{felixRID, tildeRID /*charlieRID,*/, bruceRID, tigerRID, germaineRID, tomRID, nickRID}
	if orientVersion < "2.1" {
		var srids []string
		for _, r := range ridsToDelete {
			srids = append(srids, r.(orient.OIdentifiable).GetIdentity().String())
		}
		sqlCommandAll("DELETE from ["+strings.Join(srids, ",")+"]", &retint)
	} else {
		sqlCommandAll("DELETE from ?", &retint, ridsToDelete)
	}

	Equals(t, len(ridsToDelete), retint)

	err = db.ReloadSchema()
	Nil(t, err)

	var retbool bool
	sqlCommandAll("DROP CLASS Patient", &retbool)
	Equals(t, true, retbool)
}

func TestClusterNativeAPI(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	cnt1, err := db.ClustersCount(true, "default", "index", "ouser")
	Nil(t, err)
	True(t, cnt1 > 0, "should be clusters")

	cnt2, err := db.ClustersCount(false, "default", "index", "ouser")
	Nil(t, err)
	True(t, cnt1 >= cnt2, "counts should match or have more deleted")

	begin, end, err := db.GetClusterDataRange("ouser")
	Nil(t, err)
	True(t, end >= begin, "begin and end of ClusterDataRange")

	var ival int
	err = db.Command(orient.NewSQLCommand("CREATE CLUSTER CatUSA")).All(&ival)
	Nil(t, err)
	True(t, ival > 5, fmt.Sprintf("Unexpected value of ival: %d", ival))

	err = db.Command(orient.NewSQLCommand("ALTER CLUSTER CatUSA Name CatAmerica")).Err()
	Nil(t, err)

	var bval bool = true
	err = db.Command(orient.NewSQLCommand("DROP CLUSTER CatUSA")).All(&bval)
	Nil(t, err)
	Equals(t, false, bval)

	bval = false
	err = db.Command(orient.NewSQLCommand("DROP CLUSTER CatAmerica")).All(&bval)
	Nil(t, err)
	Equals(t, true, bval)

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
	// TODO: this will fail, as cluster still exists
	// this time it should return an error
	//	err = db.DropCluster("bigapple")
	//	True(t, err != nil, "DropCluster should return error when cluster doesn't exist")
}

func TestConcurrentClients(t *testing.T) {
	const N = 5
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	SeedDB(t, db)

	// ---[ queries and insertions concurrently ]---

	var wg sync.WaitGroup

	var docs []*orient.Document
	err := db.Command(orient.NewSQLQuery(`select count(*) from Cat where caretaker like 'Eva%'`)).All(&docs)
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

	docs = nil
	err = db.Command(orient.NewSQLQuery(`select count(*) from Cat where caretaker like 'Eva%'`)).All(&docs)
	Nil(t, err)
	afterCount := toInt(docs[0].GetField("count").Value)
	Equals(t, beforeCount, afterCount)
}

func doQueriesAndInsertions(t *testing.T, db *orient.Database, id int) {
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	nreps := 1000
	ridsToDelete := make([]string, 0, nreps)

	var docs []*orient.Document
	for i := 0; i < nreps; i++ {
		randInt := rnd.Intn(3)
		if randInt > 0 {
			time.Sleep(time.Duration(randInt) * time.Millisecond)
		}

		docs = nil
		if (i+randInt)%2 == 0 {
			sql := fmt.Sprintf(`insert into Cat set name="Bar", age=%d, caretaker="Eva%d"`, 20+id, id)
			err := db.Command(orient.NewSQLCommand(sql)).All(&docs)
			Nil(t, err)
			Equals(t, 1, len(docs))
			ridsToDelete = append(ridsToDelete, docs[0].RID.String())
		} else {
			sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
			err := db.Command(orient.NewSQLQuery(sql)).All(&docs)
			Nil(t, err)
			Equals(t, toInt(docs[0].GetField("count").Value), len(ridsToDelete))
		}
	}

	//t.Logf("records insert by goroutine %d: %v", id, len(ridsToDelete))

	// ---[ clean up ]---

	for _, rid := range ridsToDelete {
		err := db.Command(orient.NewSQLCommand(`delete from Cat where @rid=` + rid)).Err()
		Nil(t, err)
	}
	docs = nil
	sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
	err := db.Command(orient.NewSQLQuery(sql)).All(&docs)
	Nil(t, err)
	Equals(t, toInt(docs[0].GetField("count").Value), 0)
}
