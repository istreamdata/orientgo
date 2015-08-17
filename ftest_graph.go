package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
)

func createOgonoriGraphDb(dbc *obinary.DBClient) {
	ogl.Println("- - - - - - CREATE GRAPHDB - - - - - - -")

	err := obinary.ConnectToServer(dbc, dbUser, dbPass)
	Ok(err)

	Assert(dbc.GetSessionID() >= int32(0), "sessionid")
	Assert(dbc.GetCurrDB() == nil, "currDB should be nil")

	dbexists, err := obinary.DatabaseExists(dbc, dbGraphName, constants.Persistent)
	Ok(err)
	if dbexists {
		dropDatabase(dbc, dbGraphName, constants.GraphDB)
	}

	err = obinary.CreateDatabase(dbc, dbGraphName, constants.GraphDB, constants.Persistent)
	Ok(err)
	dbexists, err = obinary.DatabaseExists(dbc, dbGraphName, constants.Persistent)
	Ok(err)
	Assert(dbexists, dbGraphName+" should now exists after creating it")
}

func graphCommandsNativeAPI(dbc *obinary.DBClient, fullTest bool) {
	var (
		sql    string
		retval string
		docs   []*oschema.ODocument
		err    error
	)

	createOgonoriGraphDb(dbc)

	ogl.Println("- - - - - - GRAPH COMMANDS - - - - - - -")

	err = obinary.OpenDatabase(dbc, dbGraphName, constants.GraphDB, "admin", "admin")
	Ok(err)
	defer obinary.CloseDatabase(dbc)

	sql = `CREATE Class Person extends V`
	retval, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	numval, err := strconv.Atoi(retval)
	Ok(err)
	Assert(numval > 0, "numval > 0 failed")
	Equals(0, len(docs))

	sql = `CREATE VERTEX Person SET firstName = 'Bob', lastName = 'Wilson'`
	_, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	Equals(1, len(docs))
	Equals(2, len(docs[0].FieldNames()))
	Equals("Wilson", docs[0].GetField("lastName").Value)

	sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	retval, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	Equals("1", retval)
	Equals(0, len(docs))

	sql = `INSERT INTO Person (firstName, lastName, SSN) VALUES ('Abbie', 'Wilson', '123-55-5555'), ('Zeke', 'Rossi', '444-44-4444')`
	_, docs, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)
	Equals(2, len(docs))
	Equals(3, len(docs[0].FieldNames()))
	Equals("Wilson", docs[0].GetField("lastName").Value)
	abbieRID := docs[0].RID
	zekeRID := docs[1].RID

	/* ---[ Update with the native API ]--- */
	abbie := docs[0]
	abbie.Field("SSN", "555-55-5555")
	err = obinary.UpdateRecord(dbc, abbie)
	Ok(err)

	sql = `CREATE CLASS Friend extends E`
	_, _, err = obinary.SQLCommand(dbc, sql, "")
	Ok(err)

	// sql = `CREATE EDGE Friend FROM ? to ?`
	// _, docs, err = obinary.SQLCommand(dbc, sql, abbieRID.String(), zekeRID.String())
	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, abbieRID.String(), zekeRID.String())
	_, _, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	obinary.ReloadSchema(dbc)

	var abbieVtx, zekeVtx *oschema.ODocument
	var friendLinkBag *oschema.OLinkBag

	// TODO: this query fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	sql = `SELECT from Person where any() traverse(0,2) (firstName = 'Abbie') ORDER BY firstName`
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(2, len(docs))
	abbieVtx = docs[0]
	zekeVtx = docs[1]
	Equals("Wilson", abbieVtx.GetField("lastName").Value)
	Equals("Rossi", zekeVtx.GetField("lastName").Value)
	friendLinkBag = abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(0, friendLinkBag.GetRemoteSize()) // FIXME: this is probably wrong -> is now 0
	Equals(1, len(friendLinkBag.Links))
	Assert(zekeVtx.RID.ClusterID != friendLinkBag.Links[0].RID.ClusterID, "friendLink should be from friend table")
	Assert(friendLinkBag.Links[0].Record == nil, "Record should not be filled in (no extended fetchPlan)")

	// TODO: this query fails with orientdb-community-2.1-rc5 on Windows (not tested on Linux)
	//       error is: FATAL: client.go:904: github.com/quux00/ogonori/obinary/qrycmd.go:125; cause: ERROR: readResultSet: expected short value of 0 but is -3
	sql = `TRAVERSE * from ` + abbieRID.String()
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(3, len(docs))
	// AbbieVertex -out-> FriendEdge -in-> ZekeVertex, in that order
	abbieVtx = docs[0]
	friendEdge := docs[1]
	zekeVtx = docs[2]
	Equals("Person", abbieVtx.Classname)
	Equals("Friend", friendEdge.Classname)
	Equals("Person", zekeVtx.Classname)
	Equals("555-55-5555", abbieVtx.GetField("SSN").Value)
	linkBagInAbbieVtx := abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(0, linkBagInAbbieVtx.GetRemoteSize())
	Equals(1, len(linkBagInAbbieVtx.Links))
	Assert(linkBagInAbbieVtx.Links[0].Record == nil, "Record should not be filled in (no extended fetchPlan)")
	Equals(linkBagInAbbieVtx.Links[0].RID, friendEdge.RID)
	Equals(2, len(friendEdge.FieldNames()))
	outEdgeLink := friendEdge.GetField("out").Value.(*oschema.OLink)
	Equals(abbieVtx.RID, outEdgeLink.RID)
	inEdgeLink := friendEdge.GetField("in").Value.(*oschema.OLink)
	Equals(zekeVtx.RID, inEdgeLink.RID)
	linkBagInZekeVtx := zekeVtx.GetField("in_Friend").Value.(*oschema.OLinkBag)
	Equals(1, len(linkBagInZekeVtx.Links))
	Equals(friendEdge.RID, linkBagInZekeVtx.Links[0].RID)

	sql = `SELECT from Person where any() traverse(0,2) (firstName = ?)`
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks, "Abbie")
	Ok(err)
	Equals(2, len(docs))
	abbieVtx = docs[0]
	zekeVtx = docs[1]
	Equals("Wilson", abbieVtx.GetField("lastName").Value)
	Equals("Rossi", zekeVtx.GetField("lastName").Value)
	friendLinkBag = abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Equals(1, len(friendLinkBag.Links))
	Assert(zekeVtx.RID.ClusterID != friendLinkBag.Links[0].RID.ClusterID, "friendLink should be from friend table")
	// the link in abbie is an EDGE (of Friend class)
	Equals("Friend", friendLinkBag.Links[0].Record.Classname)
	outEdgeLink = friendLinkBag.Links[0].Record.GetField("out").Value.(*oschema.OLink)
	Equals(abbieVtx.RID, outEdgeLink.RID)
	inEdgeLink = friendLinkBag.Links[0].Record.GetField("in").Value.(*oschema.OLink)
	Equals(zekeVtx.RID, inEdgeLink.RID)

	// now add more entries and Friend edges
	// Abbie --Friend--> Zeke
	// Zeke  --Friend--> Jim
	// Jim   --Friend--> Zeke
	// Jim   --Friend--> Abbie
	// Zeke  --Friend--> Paul

	abbieRID = abbieVtx.RID
	zekeRID = zekeVtx.RID

	sql = `INSERT INTO Person (firstName, lastName, SSN) VALUES ('Jim', 'Sorrento', '222-22-2222'), ('Paul', 'Pepper', '333-33-3333')`
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	Equals(2, len(docs))
	jimRID := docs[0].RID
	paulRID := docs[1].RID

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, zekeRID.String(), jimRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, jimRID.String(), zekeRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, jimRID.String(), abbieRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

	sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, zekeRID.String(), paulRID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)

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
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(4, len(docs))
	Equals("Abbie", docs[0].GetField("firstName").Value)
	Equals("Jim", docs[1].GetField("firstName").Value)
	Equals("Paul", docs[2].GetField("firstName").Value)
	Equals("Zeke", docs[3].GetField("firstName").Value)

	// Abbie should have one out_Friend and one in_Friend
	Equals(1, len(docs[0].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(1, len(docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Jim has two out_Friend and one in_Friend links
	Equals(1, len(docs[1].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(2, len(docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Paul has one in_Friend and zero out_Friend links
	Equals(1, len(docs[2].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Assert(docs[2].GetField("out_Friend") == nil, "Paul should have no out_Field edges")

	// Zeke has two in_Friend and two out_Friend edges
	Equals(2, len(docs[3].GetField("in_Friend").Value.(*oschema.OLinkBag).Links))
	Equals(2, len(docs[3].GetField("out_Friend").Value.(*oschema.OLinkBag).Links))

	// Paul's in_Friend should be Zeke's outFriend link to Paul
	// the links are edges not vertexes, so have to check for a match on edge RIDs
	paulsInFriendEdge := docs[2].GetField("in_Friend").Value.(*oschema.OLinkBag).Links[0]

	zekesOutFriendEdges := docs[3].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	sort.Sort(byRID(zekesOutFriendEdges))
	// I know that zeke -> paul edge was the last one created, so it will be the second
	// in Zeke's LinkBag list
	Equals(paulsInFriendEdge.RID, zekesOutFriendEdges[1].RID)

	// ------

	// should return two links Abbie -> Zeke and Jim -> Abbie
	sql = `SELECT both('Friend') from ` + abbieRID.String()
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(1, len(docs))
	abbieBothLinks := docs[0].GetField("both").Value.([]*oschema.OLink)
	Equals(2, len(abbieBothLinks))
	sort.Sort(byRID(abbieBothLinks))
	Equals(zekeRID, abbieBothLinks[0].RID)
	Equals(jimRID, abbieBothLinks[1].RID)

	sql = fmt.Sprintf(`SELECT dijkstra(%s, %s, 'weight') `, abbieRID.String(), paulRID.String())
	docs, err = obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	// return value is a single Document with single field called 'dijkstra' with three links
	// from abbie to paul, namely: abbie -> zeke -> paul
	Equals(1, len(docs))
	pathLinks := docs[0].GetField("dijkstra").Value.([]*oschema.OLink)
	Equals(3, len(pathLinks))
	Equals(abbieRID, pathLinks[0].RID)
	Equals(zekeRID, pathLinks[1].RID)
	Equals(paulRID, pathLinks[2].RID)

	// sql = `DELETE VERTEX #24:434` // need to get the @rid of Bob
	// sql = `DELETE VERTEX Person WHERE lastName = 'Wilson'`
	// sql = `DELETE VERTEX Person WHERE in.@Class = 'MembershipExpired'`

	addManyLinksToFlipFriendLinkBagToExternalTreeBased(dbc, abbieRID)
	doCircularLinkExample(dbc)
}

func addManyLinksToFlipFriendLinkBagToExternalTreeBased(dbc *obinary.DBClient, abbieRID oschema.ORID) {
	var (
		sql string
		err error
	)

	nAbbieOutFriends := 88
	for i := 0; i < nAbbieOutFriends; i++ {
		sql = fmt.Sprintf(`INSERT INTO Person (firstName, lastName) VALUES ('Matt%d', 'Black%d')`, i, i)
		_, docs, err := obinary.SQLCommand(dbc, sql)
		Assert(err == nil, fmt.Sprintf("Failure on Person insert #%d: %v", i, err))
		Equals(1, len(docs))

		sql = fmt.Sprintf(`CREATE EDGE Friend FROM %s to %s`, abbieRID.String(), docs[0].RID.String())
		_, docs, err = obinary.SQLCommand(dbc, sql)
		Ok(err)
	}

	// TODO: try the below query with FetchPlanFollowAllLinks -> are all the LinkBag docs returned ??
	sql = `SELECT from Person where any() traverse(0,2) (firstName = 'Abbie') ORDER BY firstName`
	// _, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	docs, err := obinary.SQLQuery(dbc, sql, "")
	Ok(err)
	Equals(91, len(docs))
	// Abbie is the first entry and for in_Friend she has an embedded LinkBag,
	// buf for out_Fridn she has a tree-based remote LinkBag, not yet filled in
	abbieVtx := docs[0]
	Equals("Wilson", abbieVtx.GetField("lastName").Value)
	abbieInFriendLinkBag := abbieVtx.GetField("in_Friend").Value.(*oschema.OLinkBag)
	Equals(1, len(abbieInFriendLinkBag.Links))
	Equals(false, abbieInFriendLinkBag.IsRemote())
	Assert(abbieInFriendLinkBag.GetRemoteSize() <= 0, "GetRemoteSize should not be set to positive val")

	abbieOutFriendLinkBag := abbieVtx.GetField("out_Friend").Value.(*oschema.OLinkBag)
	Assert(abbieOutFriendLinkBag.Links == nil, "out_Friends links should not be present")
	Equals(true, abbieOutFriendLinkBag.IsRemote())
	Assert(abbieInFriendLinkBag.GetRemoteSize() <= 0, "GetRemoteSize should not be set to positive val")

	sz, err := obinary.FetchSizeOfRemoteLinkBag(dbc, abbieOutFriendLinkBag)
	Ok(err)
	Equals(nAbbieOutFriends+1, sz)

	// TODO: what happens if you set inclusive to false?
	inclusive := true
	err = obinary.FetchEntriesOfRemoteLinkBag(dbc, abbieOutFriendLinkBag, inclusive)
	Ok(err)
	Equals(89, len(abbieOutFriendLinkBag.Links))

	// choose arbitrary Link from the LinkBag and fill in its Record doc
	link7 := abbieOutFriendLinkBag.Links[7]
	Assert(link7.RID.ClusterID > 1, "RID should be filled in")
	Assert(link7.Record == nil, "Link Record should NOT be filled in yet")

	// choose arbitrary Link from the LinkBag and fill in its Record doc
	link13 := abbieOutFriendLinkBag.Links[13]
	Assert(link13.RID.ClusterID > 1, "RID should be filled in")
	Assert(link13.Record == nil, "Link Record should NOT be filled in yet")

	fetchPlan := ""
	docs, err = obinary.FetchRecordByRID(dbc, link7.RID, fetchPlan)
	Equals(1, len(docs))
	link7.Record = docs[0]
	Assert(abbieOutFriendLinkBag.Links[7].Record != nil, "Link Record should be filled in")

	err = obinary.ResolveLinks(dbc, abbieOutFriendLinkBag.Links) // TODO: maybe include a fetchplan here?
	Ok(err)
	for i, outFriendLink := range abbieOutFriendLinkBag.Links {
		Assert(outFriendLink.Record != nil, fmt.Sprintf("Link Record not filled in for rec %d", i))
	}
}

func doCircularLinkExample(dbc *obinary.DBClient) {
	_, docs, err := obinary.SQLCommand(dbc, `create vertex Person content {"firstName":"AAA", "lastName":"BBB", "SSN":"111-11-1111"}`)
	Ok(err)
	Equals(1, len(docs))
	aaaDoc := docs[0]

	_, docs, err = obinary.SQLCommand(dbc, `create vertex Person content {"firstName":"YYY", "lastName":"ZZZ"}`)
	Ok(err)
	yyyDoc := docs[0]

	sql := fmt.Sprintf(`create edge Friend from %s to %s`, aaaDoc.RID.String(), yyyDoc.RID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
	aaa2yyyFriendDoc := docs[0]

	sql = fmt.Sprintf(`create edge Friend from %s to %s`, yyyDoc.RID.String(), aaaDoc.RID.String())
	_, docs, err = obinary.SQLCommand(dbc, sql)
	Ok(err)
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

	docs, err = obinary.SQLQuery(dbc, "SELECT FROM Person where firstName='AAA' OR firstName='YYY' SKIP 0 LIMIT 100 ORDER BY firstName", "")
	Ok(err)
	Equals(2, len(docs))
	Equals(aaaDoc.RID, docs[0].RID)
	aaaOutFriendLinks := docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(aaaOutFriendLinks))
	Equals(aaaOutFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Assert(aaaOutFriendLinks[0].Record == nil, "should not be filled in")

	yyyOutFriendLinks := docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(yyyOutFriendLinks))
	Equals(yyyOutFriendLinks[0].RID, yyy2aaaFriendDoc.RID)
	Assert(yyyOutFriendLinks[0].Record == nil, "should not be filled in")

	// ------

	docs, err = obinary.SQLQuery(dbc, "SELECT FROM Person where firstName='AAA' OR firstName='YYY' ORDER BY firstName", FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	Equals(aaaDoc.RID, docs[0].RID)
	aaaOutFriendLinks = docs[0].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(aaaOutFriendLinks))
	Equals(aaaOutFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Assert(aaaOutFriendLinks[0].Record != nil, "should not be filled in")

	Equals("YYY", docs[1].GetField("firstName").Value)
	yyyOutFriendLinks = docs[1].GetField("out_Friend").Value.(*oschema.OLinkBag).Links
	Equals(1, len(yyyOutFriendLinks))
	Equals(yyyOutFriendLinks[0].RID, yyy2aaaFriendDoc.RID)
	Assert(yyyOutFriendLinks[0].Record != nil, "should not be filled in")

	yyyInFriendLinks := docs[1].GetField("in_Friend").Value.(*oschema.OLinkBag).Links
	Equals(yyyInFriendLinks[0].RID, aaa2yyyFriendDoc.RID)
	Equals(yyyInFriendLinks[0].Record.RID, aaa2yyyFriendDoc.RID)
	Equals("YYY", yyyInFriendLinks[0].Record.GetField("in").Value.(*oschema.OLink).Record.GetField("firstName").Value)

	// ------

	sql = fmt.Sprintf("select from friend where @rid=%s or @rid=%s ORDER BY @rid",
		aaa2yyyFriendDoc.RID, yyy2aaaFriendDoc.RID)
	docs, err = obinary.SQLQuery(dbc, sql, FetchPlanFollowAllLinks)
	Ok(err)
	Equals(2, len(docs))
	Equals(aaa2yyyFriendDoc.RID, docs[0].RID)
	outLinkToAAA := docs[0].GetField("out").Value.(*oschema.OLink)
	Equals(outLinkToAAA.RID, aaaDoc.RID)
	Equals("AAA", outLinkToAAA.Record.GetField("firstName").Value)

	inLinkFromYYY := docs[0].GetField("in").Value.(*oschema.OLink)
	Equals(inLinkFromYYY.RID, yyyDoc.RID)
	Equals("YYY", inLinkFromYYY.Record.GetField("firstName").Value)

	outLinkToYYY := docs[1].GetField("out").Value.(*oschema.OLink)
	Equals(outLinkToYYY.RID, yyyDoc.RID)
	Equals("YYY", outLinkToYYY.Record.GetField("firstName").Value)

	inLinkFromAAA := docs[1].GetField("in").Value.(*oschema.OLink)
	Equals(inLinkFromAAA.RID, aaaDoc.RID)
	Equals("AAA", inLinkFromAAA.Record.GetField("firstName").Value)
}
