package orient_test

import (
	//"fmt"
	//	"sort"

	"github.com/dyy18/orientgo"
	//"github.com/dyy18/orientgo/oschema"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func createOgonoriGraphDb(t *testing.T, dbc orient.Client) {
	glog.Infoln("- - - - - - CREATE GRAPHDB - - - - - - -")

	sess, err := dbc.Auth(dbUser, dbPass)
	assert.Nil(t, err)

	//	assert.True(t, dbc.GetSessionId() >= int32(0), "sessionid")
	//	assert.True(t, dbc.GetCurrDB() == nil, "currDB should be nil")

	dbexists, err := sess.DatabaseExists(dbGraphName, orient.Persistent)
	assert.Nil(t, err)
	if dbexists {
		dropDatabase(t, dbc, dbGraphName, orient.Persistent)
	}

	err = sess.CreateDatabase(dbGraphName, orient.GraphDB, orient.Persistent)
	assert.Nil(t, err)
	dbexists, err = sess.DatabaseExists(dbGraphName, orient.Persistent)
	assert.Nil(t, err)
	assert.True(t, dbexists, dbGraphName+" should now exists after creating it")
}

/*
func graphCommandsNativeAPI(dbc *obinary.Client, fullTest bool) {
	var (
		sql    string
		docs   []*oschema.ODocument
		recs   obinary.Records
		err    error
	)

	intrec := func(recs obinary.Records) int {
		r, err := recs.One()
		assert.Nil(t, err)
		var val int
		err = r.Deserialize(&val)
		assert.Nil(t, err)
		return val
	}

	createOgonoriGraphDb(dbc)

	glog.Infoln("- - - - - - GRAPH COMMANDS - - - - - - -")

	err = dbc.OpenDatabase(dbGraphName, constants.GraphDB, "admin", "admin")
	assert.Nil(t, err)
	defer dbc.CloseDatabase()

	sql = `CREATE Class Person extends V`
	recs, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)
	numval := intrec(recs)
	assert.True(t, numval > 0, "numval > 0 failed")

	sql = `CREATE VERTEX Person SET firstName = 'Bob', lastName = 'Wilson'`
	docs = nil
	_, err = dbc.SQLCommand(&docs, sql)
	assert.Nil(t, err)
	Equals(t, 1, len(docs))
	Equals(t, 2, len(docs[0].FieldNames()))
	Equals(t, "Wilson", docs[0].GetField("lastName").Value)

	sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	recs, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)
	Equals(t, 1, intrec(recs))

	sql = `INSERT INTO Person (firstName, lastName, SSN) VALUES ('Abbie', 'Wilson', '123-55-5555'), ('Zeke', 'Rossi', '444-44-4444')`
	docs = nil
	_, err = dbc.SQLCommand(&docs, sql)
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, 3, len(docs[0].FieldNames()))
	Equals(t, "Wilson", docs[0].GetField("lastName").Value)
	abbieRID := docs[0].RID
	zekeRID := docs[1].RID

	// ---[ Update with the native API ]---
	abbie := docs[0]
	abbie.Field("SSN", "555-55-5555")
	err = dbc.UpdateRecord(abbie)
	assert.Nil(t, err)

	sql = `CREATE CLASS Friend extends E`
	_, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)

	// sql = `CREATE EDGE Friend FROM ? to ?`
	// _, docs, err = dbc.SQLCommand(nil, sql, abbieRID.String(), zekeRID.String())
	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, abbieRID.String(), zekeRID.String())
	_, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)

	dbc.ReloadSchema()

	var abbieVtx, zekeVtx *oschema.ODocument
	var friendLinkBag *oschema.OLinkBag

	// TODO: this query fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	sql = `SELECT from Person where any() traverse(0,2) (firstName = 'Abbie') ORDER BY firstName`
	docs = nil
	_, err = dbc.SQLQuery(&docs, nil, sql)
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	abbieVtx = docs[0]
	zekeVtx = docs[1]
	Equals(t, "Wilson", abbieVtx.GetField("lastName").Value)
	Equals(t, "Rossi", zekeVtx.GetField("lastName").Value)
	friendLinkBag = abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(t, 0, friendLinkBag.GetRemoteSize()) // FIXME: this is probably wrong -> is now 0
	Equals(t, 1, len(friendLinkBag.Links))
	assert.True(t, zekeVtx.RID.ClusterID != friendLinkBag.Links[0].RID.ClusterID, "friendLink should be from friend table")
	assert.True(t, friendLinkBag.Links[0].Record == nil, "Record should not be filled in (no extended fetchPlan)")

	// TODO: this query fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	//       error is: FATAL: client.go:904: github.com/dyy18/orientgo/obinary/qrycmd.go:125; cause: ERROR: readResultSet: expected short value of 0 but is -3
	sql = `TRAVERSE * from ` + abbieRID.String()
	docs = nil
	_, err = dbc.SQLQuery(&docs, nil, sql)
	assert.Nil(t, err)
	Equals(t, 3, len(docs))
	// AbbieVertex -out-> FriendEdge -in-> ZekeVertex, in that order
	abbieVtx = docs[0]
	friendEdge := docs[1]
	zekeVtx = docs[2]
	Equals(t, "Person", abbieVtx.Classname)
	Equals(t, "Friend", friendEdge.Classname)
	Equals(t, "Person", zekeVtx.Classname)
	Equals(t, "555-55-5555", abbieVtx.GetField("SSN").Value)
	linkBagInAbbieVtx := abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(t, 0, linkBagInAbbieVtx.GetRemoteSize())
	Equals(t, 1, len(linkBagInAbbieVtx.Links))
	assert.True(t, linkBagInAbbieVtx.Links[0].Record == nil, "Record should not be filled in (no extended fetchPlan)")
	Equals(t, linkBagInAbbieVtx.Links[0].RID, friendEdge.RID)
	Equals(t, 2, len(friendEdge.FieldNames()))
	outEdgeLink := friendEdge.GetField("out").Value.(*oschema.OLink)
	Equals(t, abbieVtx.RID, outEdgeLink.RID)
	inEdgeLink := friendEdge.GetField("in").Value.(*oschema.OLink)
	Equals(t, zekeVtx.RID, inEdgeLink.RID)
	linkBagInZekeVtx := zekeVtx.GetField("in_Friend").Value.(*oschema.OLinkBag)
	Equals(t, 1, len(linkBagInZekeVtx.Links))
	Equals(t, friendEdge.RID, linkBagInZekeVtx.Links[0].RID)

	sql = `SELECT from Person where any() traverse(0,2) (firstName = ?)`
	docs = nil
	_, err = dbc.SQLQuery(&docs, obinary.FetchPlanFollowAllLinks, sql, "Abbie")
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	abbieVtx = docs[0]
	zekeVtx = docs[1]
	Equals(t, "Wilson", abbieVtx.GetField("lastName").Value)
	Equals(t, "Rossi", zekeVtx.GetField("lastName").Value)
	friendLinkBag = abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(t, 1, len(friendLinkBag.Links))
	assert.True(t, zekeVtx.RID.ClusterID != friendLinkBag.Links[0].RID.ClusterID, "friendLink should be from friend table")
	// the link in abbie is an EDGE (of Friend class)
	Equals(t, "Friend", friendLinkBag.Links[0].Record.Classname)
	outEdgeLink = friendLinkBag.Links[0].Record.GetField("out").Value.(*oschema.OLink)
	Equals(t, abbieVtx.RID, outEdgeLink.RID)
	inEdgeLink = friendLinkBag.Links[0].Record.GetField("in").Value.(*oschema.OLink)
	Equals(t, zekeVtx.RID, inEdgeLink.RID)

	// now add more entries and Friend edges
	// Abbie --Friend--> Zeke
	// Zeke  --Friend--> Jim
	// Jim   --Friend--> Zeke
	// Jim   --Friend--> Abbie
	// Zeke  --Friend--> Paul

	abbieRID = abbieVtx.RID
	zekeRID = zekeVtx.RID

	sql = `INSERT INTO Person (firstName, lastName, SSN) VALUES ('Jim', 'Sorrento', '222-22-2222'), ('Paul', 'Pepper', '333-33-3333')`
	docs = nil
	_, err = dbc.SQLCommand(&docs, sql)
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	jimRID := docs[0].RID
	paulRID := docs[1].RID

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, zekeRID.String(), jimRID.String())
	_, docs, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, jimRID.String(), zekeRID.String())
	_, docs, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, jimRID.String(), abbieRID.String())
	_, docs, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, zekeRID.String(), paulRID.String())
	_, docs, err = dbc.SQLCommand(nil, sql)
	assert.Nil(t, err)

	// ----+-----+------+---------+--------+-----------+----------+---------
	// #   |@RID |@CLASS|firstName|lastName|SSN        |out_Friend|in_Friend
	// ----+-----+------+---------+--------+-----------+----------+---------
	// 0   |#11:1|Person|Abbie    |Wilson  |555-55-5555|[size=1]  |[size=1]
	// 1   |#11:2|Person|Zeke     |Rossi   |444-44-4444|[size=2]  |[size=2]
	// 2   |#11:3|Person|Jim      |Sorrento|222-22-2222|[size=2]  |[size=1]
	// 3   |#11:4|Person|Paul     |Pepper  |333-33-3333|null      |[size=1]
	// ----+-----+------+---------+--------+-----------+----------+---------

	// [ODocument[Classname: Person; RID: #11:1; Version: 4; fields:
	//   OField[id: -1; name: firstName; datatype: 7; value: Abbie]
	//   OField[id: -1; name: lastName; datatype: 7; value: Wilson]
	//   OField[id: -1; name: SSN; datatype: 7; value: 555-55-5555]
	//   OField[id: -1; name: out_Friend; datatype: 22; value: [<OLink RID: #12:0, Record: <nil>>]]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:3, Record: <nil>>]]]
	//  ODocument[Classname: Person; RID: #11:3; Version: 4; fields:
	//   OField[id: -1; name: out_Friend; datatype: 22; value: [<OLink RID: #12:2, Record: <nil>> <OLink RID: #12:3, Record: <nil>>]]
	//   OField[id: -1; name: lastName; datatype: 7; value: Sorrento]
	//   OField[id: -1; name: SSN; datatype: 7; value: 222-22-2222]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:1, Record: <nil>>]]
	//   OField[id: -1; name: firstName; datatype: 7; value: Jim]]
	//  ODocument[Classname: Person; RID: #11:4; Version: 2; fields:
	//   OField[id: -1; name: firstName; datatype: 7; value: Paul]
	//   OField[id: -1; name: lastName; datatype: 7; value: Pepper]
	//   OField[id: -1; name: SSN; datatype: 7; value: 333-33-3333]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:4, Record: <nil>>]]]
	//  ODocument[Classname: Person; RID: #11:2; Version: 5; fields:
	//   OField[id: -1; name: out_Friend; datatype: 22; value: [<OLink RID: #12:1, Record: <nil>> <OLink RID: #12:4, Record: <nil>>]]
	//   OField[id: -1; name: in_Friend; datatype: 22; value: [<OLink RID: #12:0, Record: <nil>> <OLink RID: #12:2, Record: <nil>>]]
	//   OField[id: -1; name: firstName; datatype: 7; value: Zeke]
	//   OField[id: -1; name: lastName; datatype: 7; value: Rossi]
	//   OField[id: -1; name: SSN; datatype: 7; value: 444-44-4444]]

	sql = `SELECT from Person where any() traverse(0,5) (firstName = 'Jim') ORDER BY firstName`
	docs = nil
	_, err = dbc.SQLQuery(&docs, nil, sql)
	assert.Nil(t, err)
	Equals(t, 4, len(docs))
	Equals(t, "Abbie", docs[0].GetField("firstName").Value)
	Equals(t, "Jim", docs[1].GetField("firstName").Value)
	Equals(t, "Paul", docs[2].GetField("firstName").Value)
	Equals(t, "Zeke", docs[3].GetField("firstName").Value)

	// Abbie should have one out_Friend and one in_Friend
	Equals(t, 1, len(docs[0].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(t, 1, len(docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Jim has two out_Friend and one in_Friend links
	Equals(t, 1, len(docs[1].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(t, 2, len(docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Paul has one in_Friend and zero out_Friend links
	Equals(t, 1, len(docs[2].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	assert.True(t, docs[2].GetField("out_Friend") == nil, "Paul should have no out_Field edges")

	// Zeke has two in_Friend and two out_Friend edges
	Equals(t, 2, len(docs[3].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(t, 2, len(docs[3].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Paul's in_Friend should be Zeke's outFriend link to Paul
	// the links are edges not vertexes, so have to check for a match on edge RIDs
	paulsInFriendEdge := docs[2].GetField("in_Friend").Value.(*oschema.OLinkBag).Links[0]

	zekesOutFriendEdges := docs[3].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	sort.Sort(byRID(zekesOutFriendEdges))
	// I know that zeke -> paul edge was the last one created, so it will be the second
	// in Zeke's LinkBag list
	Equals(t, paulsInFriendEdge.RID, zekesOutFriendEdges[1].RID)

	// ------

	// should return two links Abbie -> Zeke and Jim -> Abbie
	sql = `SELECT both('Friend') from ` + abbieRID.String()
	docs = nil
	_, err = dbc.SQLQuery(&docs, nil, sql)
	assert.Nil(t, err)
	Equals(t, 1, len(docs))
	abbieBothLinks := docs[0].GetField("both").Value.([]*oschema.OLink)
	Equals(t, 2, len(abbieBothLinks))
	sort.Sort(byRID(abbieBothLinks))
	Equals(t, zekeRID, abbieBothLinks[0].RID)
	Equals(t, jimRID, abbieBothLinks[1].RID)

	sql = fmt.Sprintf(`SELECT dijkstra(%s, %s, 'weight') `, abbieRID.String(), paulRID.String())
	docs = nil
	_, err = dbc.SQLQuery(&docs, nil, sql)
	assert.Nil(t, err)
	// return value is a single Document with single field called 'dijkstra' with three links
	// from abbie to paul, namely: abbie -> zeke -> paul
	Equals(t, 1, len(docs))
	pathLinks := docs[0].GetField("dijkstra").Value.([]*oschema.OLink)
	Equals(t, 3, len(pathLinks))
	Equals(t, abbieRID, pathLinks[0].RID)
	Equals(t, zekeRID, pathLinks[1].RID)
	Equals(t, paulRID, pathLinks[2].RID)

	// sql = `DELETE VERTEX #24:434` // need to get the @rid of Bob
	// sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	// sql = `DELETE VERTEX Person WHERE in.@Class = 'MembershipExpired'`

	addManyLinksToFlipFriendLinkBagToExternalTreeBased(dbc, abbieRID)
	doCircularLinkExample(dbc)
}

func addManyLinksToFlipFriendLinkBagToExternalTreeBased(t *testing.T, db orient.Database, abbieRID oschema.ORID) {
	var (
		sql  string
		err  error
		docs []*oschema.ODocument
	)

	nAbbieOutFriends := 88
	for i := 0; i < nAbbieOutFriends; i++ {
		sql = fmt.Sprintf(`INSERT INTO Person (firstName, lastName) VALUES ('Matt%d', 'Black%d')`, i, i)
		_, err := dbc.SQLCommand(&docs, sql)
		assert.True(t, err == nil, fmt.Sprintf("Failure on Person insert #%d: %v", i, err))
		Equals(t, 1, len(docs))

		sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, abbieRID.String(), docs[0].RID.String())
		_, err = dbc.SQLCommand(nil, sql)
		assert.Nil(t, err)
	}

	// TODO: try the below query with FetchPlanFollowAllLinks -> are all the LinkBag docs returned ??
	sql = `SELECT from Person where any() traverse(0,2) (firstName = 'Abbie') ORDER BY firstName`
	// _, err = dbc.SQLQuery(nil, sql, FetchPlanFollowAllLinks)

	_, err = dbc.SQLQuery(&docs, nil, sql)
	assert.Nil(t, err)
	Equals(t, 91, len(docs))
	// Abbie is the first entry and for in_Friend she has an embedded LinkBag,
	// buf for out_Fridn she has a tree-based remote LinkBag, not yet filled in
	abbieVtx := docs[0]
	Equals(t, "Wilson", abbieVtx.GetField("lastName").Value)
	abbieInFriendLinkBag := abbieVtx.GetField("in_Friend").Value.(*oschema.OLinkBag)
	Equals(t, 1, len(abbieInFriendLinkBag.Links))
	Equals(t, false, abbieInFriendLinkBag.IsRemote())
	assert.True(t, abbieInFriendLinkBag.GetRemoteSize() <= 0, "GetRemoteSize should not be set to positive val")

	abbieOutFriendLinkBag := abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	assert.True(t, abbieOutFriendLinkBag.Links == nil, "out_Friends links should not be present")
	Equals(t, true, abbieOutFriendLinkBag.IsRemote())
	assert.True(t, abbieInFriendLinkBag.GetRemoteSize() <= 0, "GetRemoteSize should not be set to positive val")

	sz, err := dbc.GetSizeOfRemoteLinkBag(abbieOutFriendLinkBag)
	assert.Nil(t, err)
	Equals(t, nAbbieOutFriends+1, sz)

	// TODO: what happens if you set inclusive to false?
	inclusive := true
	err = dbc.GetEntriesOfRemoteLinkBag(abbieOutFriendLinkBag, inclusive)
	assert.Nil(t, err)
	Equals(t, 89, len(abbieOutFriendLinkBag.Links))

	// choose arbitrary Link from the LinkBag and fill in its Record doc
	link7 := abbieOutFriendLinkBag.Links[7]
	assert.True(t, link7.RID.ClusterID > 1, "RID should be filled in")
	assert.True(t, link7.Record == nil, "Link Record should NOT be filled in yet")

	// choose arbitrary Link from the LinkBag and fill in its Record doc
	link13 := abbieOutFriendLinkBag.Links[13]
	assert.True(t, link13.RID.ClusterID > 1, "RID should be filled in")
	assert.True(t, link13.Record == nil, "Link Record should NOT be filled in yet")

	fetchPlan := ""
	docs, err = dbc.GetRecordByRID(link7.RID, fetchPlan)
	Equals(t, 1, len(docs))
	link7.Record = docs[0]
	assert.True(t, abbieOutFriendLinkBag.Links[7].Record != nil, "Link Record should be filled in")

	err = dbc.ResolveLinks(abbieOutFriendLinkBag.Links) // TODO: maybe include a fetchplan here?
	assert.Nil(t, err)
	for i, outFriendLink := range abbieOutFriendLinkBag.Links {
		assert.True(t, outFriendLink.Record != nil, fmt.Sprintf("Link Record not filled in for rec %d", i))
	}
}

func doCircularLinkExample(t *testing.T, db *obinary.Client) {
	var docs []*oschema.ODocument
	_, err := dbc.SQLCommand(&docs, `create vertex Person content {"firstName":"AAA", "lastName":"BBB", "SSN":"111-11-1111"}`)
	assert.Nil(t, err)
	Equals(t, 1, len(docs))
	aaaDoc := docs[0]

	docs = nil
	_, err = dbc.SQLCommand(&docs, `create vertex Person content {"firstName":"YYY", "lastName":"ZZZ"}`)
	assert.Nil(t, err)
	yyyDoc := docs[0]

	docs = nil
	sql := fmt.Sprintf(`create edge Friend from %s to %s`, aaaDoc.RID.String(), yyyDoc.RID.String())
	_, err = dbc.SQLCommand(&docs, sql)
	assert.Nil(t, err)
	aaa2yyyFriendDoc := docs[0]

	docs = nil
	sql = fmt.Sprintf(`create edge Friend from %s to %s`, yyyDoc.RID.String(), aaaDoc.RID.String())
	_, err = dbc.SQLCommand(&docs, sql)
	assert.Nil(t, err)
	yyy2aaaFriendDoc := docs[0]

	// [ODocument<Classname: Person; RID: #11:93; Version: 3; fields:
	//   OField<id: -1; name: firstName; datatype: 7; value: AAA>
	//   OField<id: -1; name: lastName; datatype: 7; value: BBB>
	//   OField<id: -1; name: SSN; datatype: 7; value: 111-11-1111>
	//   OField<id: -1; name: out_Friend; datatype: 22; value: &{[<OLink RID: #12:93, Record: ODocument<Classname: Friend; RID:
	//  #12:93; Version: 3; fields: [...]>>] {0 <nil>}}>
	//   OField<id: -1; name: in_Friend; datatype: 22; value: &{[<OLink RID: #12:94, Record: ODocument<Classname: Friend; RID:
	//  #12:94; Version: 3; fields: [...]>>] {0 <nil>}}>>

	//  ODocument<Classname: Person; RID: #11:94; Version: 3; fields:
	//   OField<id: -1; name: lastName; datatype: 7; value: ZZZ>
	//   OField<id: -1; name: in_Friend; datatype: 22; value: &{[<OLink RID: #12:93, Record: ODocument<Classname: Friend; RID:
	//  #12:93; Version: 3; fields: [...]>>] {0 <nil>}}>
	//   OField<id: -1; name: out_Friend; datatype: 22; value: &{[<OLink RID: #12:94, Record: ODocument<Classname: Friend; RID:
	//  #12:94; Version: 3; fields: [...]>>] {0 <nil>}}>
	//   OField<id: -1; name: firstName; datatype: 7; value: YYY>>
	// ]

	// [ODocument<Classname: Friend; RID: #12:93; Version: 3; fields:
	//   OField<id: -1; name: out; datatype: 13; value: <OLink RID: #11:93, Record: ODocument<Classname: Person; RID: #11:93;
	// Version: 3; fields: [...]>>>
	//   OField<id: -1; name: in; datatype: 13; value: <OLink RID: #11:94, Record: ODocument<Classname: Person; RID: #11:94;
	// Version: 3; fields: [...]>>>>
	//  ODocument<Classname: Friend; RID: #12:94; Version: 3; fields:
	//   OField<id: -1; name: out; datatype: 13; value: <OLink RID: #11:94, Record: ODocument<Classname: Person; RID: #11:94;
	// Version: 3; fields: [...]>>>
	//   OField<id: -1; name: in; datatype: 13; value: <OLink RID: #11:93, Record: ODocument<Classname: Person; RID: #11:93;
	// Version: 3; fields: [...]>>>>

	docs = nil
	_, err = dbc.SQLQuery(&docs, nil, "SELECT FROM Person where firstName='AAA' OR firstName='YYY' SKIP 0 LIMIT 100 ORDER BY firstName")
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, aaaDoc.RID, docs[0].RID)
	aaaOutFriendLinks := docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(t, 1, len(aaaOutFriendLinks))
	Equals(t, aaaOutFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	assert.True(t, aaaOutFriendLinks[0].Record == nil, "should not be filled in")

	yyyOutFriendLinks := docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(t, 1, len(yyyOutFriendLinks))
	Equals(t, yyyOutFriendLinks[0].RID, yyy2aaaFriendDoc.RID)
	assert.True(t, yyyOutFriendLinks[0].Record == nil, "should not be filled in")

	// ------

	docs = nil
	_, err = dbc.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, "SELECT FROM Person where firstName='AAA' OR firstName='YYY' ORDER BY firstName")
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, aaaDoc.RID, docs[0].RID)
	aaaOutFriendLinks = docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(t, 1, len(aaaOutFriendLinks))
	Equals(t, aaaOutFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	assert.True(t, aaaOutFriendLinks[0].Record != nil, "should not be filled in")

	Equals(t, "YYY", docs[1].GetField("firstName").Value)
	yyyOutFriendLinks = docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(t, 1, len(yyyOutFriendLinks))
	Equals(t, yyyOutFriendLinks[0].RID, yyy2aaaFriendDoc.RID)
	assert.True(t, yyyOutFriendLinks[0].Record != nil, "should not be filled in")

	yyyInFriendLinks := docs[1].GetField("in_Friend").Value.(*oschema.OLinkBag).Links
	Equals(t, yyyInFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Equals(t, yyyInFriendLinks[0].Record.RID, aaa2yyyFriendDoc.RID)
	Equals(t, "YYY", yyyInFriendLinks[0].Record.GetField("in").Value.(*oschema.OLink).Record.GetField("firstName").Value)

	// ------

	sql = fmt.Sprintf("select from friend where @rid=%s or @rid=%s ORDER BY @rid",
		aaa2yyyFriendDoc.RID, yyy2aaaFriendDoc.RID)
	docs = nil
	_, err = dbc.SQLQuery(&docs, orient.FetchPlanFollowAllLinks, sql)
	assert.Nil(t, err)
	Equals(t, 2, len(docs))
	Equals(t, aaa2yyyFriendDoc.RID, docs[0].RID)
	outLinkToAAA := docs[0].GetField("out").Value.(*oschema.OLink)
	Equals(t, outLinkToAAA.RID, aaaDoc.RID)
	Equals(t, "AAA", outLinkToAAA.Record.GetField("firstName").Value)

	inLinkFromYYY := docs[0].GetField("in").Value.(*oschema.OLink)
	Equals(t, inLinkFromYYY.RID, yyyDoc.RID)
	Equals(t, "YYY", inLinkFromYYY.Record.GetField("firstName").Value)

	outLinkToYYY := docs[1].GetField("out").Value.(*oschema.OLink)
	Equals(t, outLinkToYYY.RID, yyyDoc.RID)
	Equals(t, "YYY", outLinkToYYY.Record.GetField("firstName").Value)

	inLinkFromAAA := docs[1].GetField("in").Value.(*oschema.OLink)
	Equals(t, inLinkFromAAA.RID, aaaDoc.RID)
	Equals(t, "AAA", inLinkFromAAA.Record.GetField("firstName").Value)
}
*/
