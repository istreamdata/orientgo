package obinary

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/dyy18/orientgo"
	"github.com/dyy18/orientgo/obinary/binserde"
	"github.com/dyy18/orientgo/obinary/rw"
	"github.com/dyy18/orientgo/oschema"
	"github.com/golang/glog"
)

// OpenDatabase sends the REQUEST_DB_OPEN command to the OrientDb server to
// open the db in read/write mode.  The database name and type are required, plus
// username and password.  Database type should be one of the obinary constants:
// DocumentDbType or GraphDbType.
func (dbc *Client) OpenDatabase(dbname string, dbtype orient.DatabaseType, username, passw string) (err error) {
	defer catch(&err)
	buf := dbc.writeBuffer()

	// first byte specifies request type
	rw.WriteByte(buf, requestDbOpen)

	// session-id - send a negative number to create a new server-side conx
	rw.WriteInt(buf, requestNewSession)
	rw.WriteStrings(buf, driverName, driverVersion)
	rw.WriteShort(buf, dbc.binaryProtocolVersion)

	// dbclient id - send as null, but cannot be null if clustered config
	rw.WriteNull(buf)

	// serialization-impl
	rw.WriteString(buf, dbc.serializationType)

	// token-session, hardcoded as false for now -> change later based on ClientOptions settings
	rw.WriteBool(buf, false)

	// dbname, dbtype, username, password
	rw.WriteStrings(buf, dbname, string(dbtype), username, passw)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	/* ---[ read back response ]--- */

	// first byte indicates success/error
	status := rw.ReadByte(dbc.conx)

	dbc.currDb = NewDatabase(dbname, dbtype)

	// the first int returned is the session id sent - which was the `RequestNewSession` sentinel
	sessionValSent := rw.ReadInt(dbc.conx)

	if sessionValSent != requestNewSession {
		return fmt.Errorf("Unexpected Error: Server did not return expected session-request-val that was sent")
	}

	// if status returned was ERROR, then the rest of server data is the exception info
	if status != responseStatusOk {
		exceptions := rw.ReadErrorResponse(dbc.conx)
		return fmt.Errorf("Server Error(s): %v", exceptions)
	}

	// for the REQUEST_DB_OPEN case, another int is returned which is the new sessionId
	sessionId := rw.ReadInt(dbc.conx)
	dbc.sessionId = sessionId

	// next is the token, which may be null
	tokenBytes := rw.ReadBytes(dbc.conx)
	dbc.token = tokenBytes

	// array of cluster info in this db // TODO: do we need to retain all this in memory?
	numClusters := rw.ReadShort(dbc.conx)

	clusters := make([]OCluster, 0, numClusters)

	for i := 0; i < int(numClusters); i++ {
		clusterName := rw.ReadString(dbc.conx)
		clusterId := rw.ReadShort(dbc.conx)
		clusters = append(clusters, OCluster{Name: clusterName, Id: clusterId})
	}
	dbc.currDb.Clusters = clusters

	// cluster-config - bytes - null unless running server in clustered config
	// TODO: treating this as an opaque blob for now
	clusterCfg := rw.ReadBytes(dbc.conx)
	dbc.currDb.ClustCfg = clusterCfg

	// orientdb server release - throwing away for now // TODO: need this?
	_ = rw.ReadString(dbc.conx)

	// ---[ load #0:0 - config record ]---
	oschemaRID, err := dbc.loadConfigRecord()
	if err != nil {
		return err
	}

	// ---[ load #0:1 - oschema record ]---
	err = dbc.loadSchema(oschemaRID)
	if err != nil {
		return err
	}

	return nil
}

// loadConfigRecord loads record #0:0 for the current database, caching
// some of the information returned into OStorageConfiguration
func (dbc *Client) loadConfigRecord() (oschemaRID oschema.ORID, err error) {
	defer catch(&err)
	// The config record comes back as type 'b' (raw bytes), which should
	// just be converted to a string then tokenized by the pipe char

	var (
		clusterId  int16 = 0
		clusterPos int64 = 0
	)

	buf := dbc.writeCommandAndSessionId(requestRecordLOAD)

	rw.WriteShort(buf, clusterId)
	rw.WriteLong(buf, clusterPos)

	fetchPlan := "*:-1 index:0"
	rw.WriteString(buf, fetchPlan)

	ignoreCache := true
	rw.WriteBool(buf, ignoreCache)

	loadTombstones := true // based on Java client code
	rw.WriteBool(buf, loadTombstones)

	// Driver supports only synchronous requests, so we need to wait until previous request is finished
	dbc.mutex.Lock()
	defer func() {
		dbc.mutex.Unlock()
	}()

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---
	if err = dbc.readStatusCodeAndError(); err != nil {
		return oschemaRID, err
	}

	payloadStatus := rw.ReadByte(dbc.conx)
	if payloadStatus == byte(0) {
		return oschemaRID, fmt.Errorf("Payload status for #0:0 load was 0. No config data returned.")
	}

	rectype := rw.ReadByte(dbc.conx)

	// this is the record version - don't see a reason to check or cache it right now
	_ = rw.ReadInt(dbc.conx)

	databytes := rw.ReadBytes(dbc.conx)

	if rectype != 'b' {
		if err != nil {
			return oschemaRID, fmt.Errorf("Expected rectype %d, but was: %d", 'b', rectype)
		}
	}

	payloadStatus = rw.ReadByte(dbc.conx)
	if payloadStatus != byte(0) {
		return oschemaRID,
			fmt.Errorf("Second Payload status for #0:0 load was not 0. More than one record returned unexpectedly")
	}

	err = parseConfigRecord(dbc.currDb, string(databytes))
	if err != nil {
		return oschemaRID, err
	}

	oschemaRID = dbc.currDb.StorageCfg.schemaRID
	return oschemaRID, err
}

// parseConfigRecord takes the pipe-separate values that comes back
// from reading record #0:0 and turns it into an OStorageConfiguration
// object, which it adds to the db database object.
// TODO: move this function to be a method of OStorageConfiguration?
func parseConfigRecord(db *ODatabase, psvData string) error {
	sc := OStorageConfiguration{}

	toks := strings.Split(psvData, "|")

	version, err := strconv.ParseInt(toks[0], 10, 8)
	if err != nil {
		return err
	}

	sc.version = byte(version)
	sc.name = strings.TrimSpace(toks[1])
	sc.schemaRID = oschema.NewORIDFromString(strings.TrimSpace(toks[2]))
	sc.dictionaryRID = strings.TrimSpace(toks[3])
	sc.idxMgrRID = oschema.NewORIDFromString(strings.TrimSpace(toks[4]))
	sc.localeLang = strings.TrimSpace(toks[5])
	sc.localeCountry = strings.TrimSpace(toks[6])
	sc.dateFmt = strings.TrimSpace(toks[7])
	sc.dateTimeFmt = strings.TrimSpace(toks[8])
	sc.timezone = strings.TrimSpace(toks[9])

	db.StorageCfg = sc

	return nil
}

// loadSchema loads record #0:1 for the current database, caching the
// SchemaVersion, GlobalProperties and Classes info in the current ODatabase
// object (dbc.currDb).
func (dbc *Client) loadSchema(oschemaRID oschema.ORID) error {
	docs, err := dbc.GetRecordByRID(oschemaRID, "*:-1 index:0") // fetchPlan used by the Java client
	if err != nil {
		return err
	}
	if len(docs) != 1 {
		return fmt.Errorf("Load Record %s should only return one record. Returned: %d", oschemaRID, len(docs))
	}

	// ---[ schemaVersion ]---
	dbc.currDb.SchemaVersion = docs[0].GetField("schemaVersion").Value.(int32)

	/* ---[ globalProperties ]--- */
	globalPropsFld := docs[0].GetField("globalProperties")

	var globalProperty oschema.OGlobalProperty
	for _, pfield := range globalPropsFld.Value.([]interface{}) {
		pdoc := pfield.(*oschema.ODocument)
		globalProperty = oschema.NewGlobalPropertyFromDocument(pdoc)
		dbc.currDb.GlobalProperties[int(globalProperty.Id)] = globalProperty
	}

	/* ---[ classes ]--- */
	var oclass *oschema.OClass
	classesFld := docs[0].GetField("classes")
	for _, cfield := range classesFld.Value.([]interface{}) {
		cdoc := cfield.(*oschema.ODocument)
		oclass = oschema.NewOClassFromDocument(cdoc)
		dbc.currDb.Classes[oclass.Name] = oclass
	}

	return nil
}

// CloseDatabase closes down a session with a specific database that
// has already been opened (via OpenDatabase). This should be called
// when exiting an app or before starting a connection to a different
// OrientDB database.
func (dbc *Client) CloseDatabase() (err error) {
	defer catch(&err)

	buf := dbc.writeCommandAndSessionId(requestDbClose)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// the server has no response to a DB_CLOSE

	// remove session, token and currDb info
	dbc.sessionId = noSessionId
	dbc.token = nil
	dbc.currDb = nil // TODO: anything in currDB that needs to be closed?

	return nil
}

// FetchDatabaseSize retrieves the size of the current database in bytes.
// It is a database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
func (dbc *Client) getDbSize() (dbSize int64, err error) {
	return dbc.getLongFromDB(requestDbSIZE)
}

// FetchNumRecordsInDatabase retrieves the number of records of the current
// database. It is a database-level operation, so OpenDatabase must have
// already been called first in order to start a session with the database.
func (dbc *Client) CountRecords() (int64, error) {
	return dbc.getLongFromDB(requestDbCOUNTRECORDS)
}

func (dbc *Client) DeleteRecordByRIDAsync(rid string, recVersion int32) error {
	return dbc.deleteByRID(rid, recVersion, true)
}

// DeleteRecordByRID deletes a record specified by its RID and its version.
// This is the synchronous version where the server confirms whether the
// delete was successful and the client reports that back to the caller.
// See DeleteRecordByRIDAsync for the async version.
//
// If nil is returned, delete succeeded.
// If error is returned, delete request was either never issued, or there was
// a problem on the server end or the record did not exist in the database.
func (dbc *Client) DeleteRecordByRID(rid string, recVersion int32) error {
	return dbc.deleteByRID(rid, recVersion, false)
}

func (dbc *Client) deleteByRID(rid string, recVersion int32, async bool) error {
	orid := oschema.NewORIDFromString(rid)

	buf := dbc.writeCommandAndSessionId(requestRecordDELETE)

	rw.WriteShort(buf, orid.ClusterID)

	rw.WriteLong(buf, orid.ClusterPos)

	rw.WriteInt(buf, recVersion)

	// sync mode ; 0 = synchronous; 1 = asynchronous
	var syncMode byte
	if async {
		syncMode = byte(1)
	}
	rw.WriteByte(buf, syncMode)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err := dbc.readStatusCodeAndError(); err != nil {
		return err
	}

	payloadStatus := rw.ReadByte(dbc.conx)

	// status 1 means record was deleted;
	// status 0 means record was not deleted (either failed or didn't exist)
	if payloadStatus == byte(0) {
		return fmt.Errorf("Server reports record %s was not deleted. Either failed or did not exist.",
			rid)
	}

	return nil
}

// GetRecordByRID takes an ORID and reads that record from the database.
// NOTE: for now I'm assuming all records are Documents (they can also be "raw bytes" or "flat data")
// and for some reason I don't understand, multiple records can be returned, so I'm returning
// a slice of ODocument
//
// TODO: may also want to expose options: ignoreCache, loadTombstones bool
// TODO: need to properly handle fetchPlan
func (dbc *Client) GetRecordByRID(orid oschema.ORID, fetchPlan string) (docs []*oschema.ODocument, err error) {
	defer catch(&err)

	buf := dbc.writeCommandAndSessionId(requestRecordLOAD)

	rw.WriteShort(buf, orid.ClusterID)
	rw.WriteLong(buf, orid.ClusterPos)

	rw.WriteString(buf, fetchPlan)

	ignoreCache := true // hardcoding for now
	rw.WriteBool(buf, ignoreCache)

	loadTombstones := false // hardcoding for now
	rw.WriteBool(buf, loadTombstones)

	// Driver supports only synchronous requests, so we need to wait until previous request is finished
	dbc.mutex.Lock()
	defer func() {
		dbc.mutex.Unlock()
	}()

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---
	if err = dbc.readStatusCodeAndError(); err != nil {
		return nil, err
	}

	// this query can return multiple records (though I don't understand why)
	// so must do this in a loop
	docs = make([]*oschema.ODocument, 0, 1)
	for {
		payloadStatus := rw.ReadByte(dbc.conx)
		if payloadStatus == byte(0) {
			break
		}

		rectype := rw.ReadByte(dbc.conx)
		recversion := rw.ReadInt(dbc.conx)

		databytes := rw.ReadBytes(dbc.conx)

		if rectype == 'd' {
			// we don't know the classname so set empty value
			doc := oschema.NewDocument("")
			doc.RID = orid
			doc.Version = recversion

			// the first byte specifies record serialization version
			// use it to look up serializer
			serde := dbc.currDb.RecordSerDes[int(databytes[0])]
			// then strip off the version byte and send the data to the serde
			err = serde.Deserialize(dbc, doc, bytes.NewReader(databytes[1:]))
			if err != nil {
				return nil, fmt.Errorf("ERROR in Deserialize for rid %v: %v\n", orid, err)
			}
			docs = append(docs, doc)

		} else {
			return nil,
				fmt.Errorf("Only `document` records are currently supported by the client. Record returned was type: %v", rectype)
		}
	}

	return docs, nil
}

// ReloadSchema should be called after a schema is altered, such as properties
// added, deleted or renamed.
func (dbc *Client) ReloadSchema() error {
	return dbc.loadSchema(oschema.ORID{ClusterID: 0, ClusterPos: 1})
}

// FetchClusterDataRange returns the range of record ids for a cluster
func (dbc *Client) FetchClusterDataRange(clusterName string) (begin, end int64, err error) {
	defer catch(&err)

	clusterID := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterID < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a FetchClusterRangeById(dbc, clusterID)
		return begin, end,
			fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	buf := dbc.writeCommandAndSessionId(requestDataClusterDATARANGE)

	rw.WriteShort(buf, clusterID)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return begin, end, err
	}

	begin = rw.ReadLong(dbc.conx)
	end = rw.ReadLong(dbc.conx)
	return begin, end, err
}

// AddCluster adds a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// The clusterID is returned if the command is successful.
func (dbc *Client) AddCluster(clusterName string) (clusterID int16, err error) {
	defer catch(&err)
	buf := dbc.writeCommandAndSessionId(requestDataClusterADD)

	cname := strings.ToLower(clusterName)

	rw.WriteString(buf, cname)

	rw.WriteShort(buf, -1) // -1 means generate new cluster id

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return int16(0), err
	}

	clusterID = rw.ReadShort(dbc.conx)

	dbc.currDb.Clusters = append(dbc.currDb.Clusters, OCluster{cname, clusterID})
	return clusterID, err
}

// DropCluster drops a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// If nil is returned, then the action succeeded.
func (dbc *Client) DropCluster(clusterName string) (err error) {
	defer catch(&err)
	clusterID := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterID < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a DropClusterById(dbc, clusterID)
		return fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	buf := dbc.writeCommandAndSessionId(requestDataClusterDROP)

	rw.WriteShort(buf, clusterID)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return err
	}

	delStatus := rw.ReadByte(dbc.conx)

	if delStatus != byte(1) {
		return fmt.Errorf("Drop cluster action failed. Return code from server was not '1', but %d",
			delStatus)
	}

	return nil
}

// FetchEntriesOfRemoteLinkBag fills in the links of an OLinkBag that is remote
// (tree-based) rather than embedded.  This function will fill in the links
// of the passed in OLinkBag, rather than returning the new links. The Links
// will have RIDs only, not full Records (ODocuments).  If you then want the
// Records filled in, call the ResolveLinks function.
func (dbc *Client) GetEntriesOfRemoteLinkBag(linkBag *oschema.OLinkBag, inclusive bool) (err error) {
	defer catch(&err)
	var (
		firstLink *oschema.OLink
		linkSerde = binserde.TypeSerializers[binserde.LinkSerializer] // the OLinkSerializer
	)

	firstLink, err = dbc.GetFirstKeyOfRemoteLinkBag(linkBag)
	if err != nil {
		return err
	}

	buf := dbc.writeCommandAndSessionId(requestSBTREE_BONSAI_GET_ENTRIES_MAJOR)

	writeLinkBagCollectionPointer(buf, linkBag)

	linkBytes, err := linkSerde.Serialize(firstLink)
	if err != nil {
		return err
	}

	rw.WriteBytes(buf, linkBytes)

	rw.WriteBool(buf, inclusive)

	// copied from Java client OSBTreeBonsaiRemote#fetchEntriesMajor
	if dbc.binaryProtocolVersion >= 21 {
		rw.WriteInt(buf, 128)
	}

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return err
	}

	linkEntryBytes := rw.ReadBytes(dbc.conx)

	// all the rest of the response from the server in in this byte slice so
	// we can reset the dbc.buf and reuse it to deserialize the serialized links
	buf.Reset()
	// ignoring error since doc says this method panics rather than return
	// non-nil error
	rw.WriteRawBytes(buf, linkEntryBytes)

	nrecs := rw.ReadInt(buf)

	var result interface{}
	nr := int(nrecs)
	// loop over all the serialized links
	for i := 0; i < nr; i++ {
		result, err = linkSerde.Deserialize(buf)
		if err != nil {
			return err
		}
		linkBag.AddLink(result.(*oschema.OLink))

		// FIXME: for some reason the server returns a serialized link
		//        followed by an integer (so far always a 1 in my expts).
		//        Not sure what to do with this int, so ignore for now
		intval := rw.ReadInt(buf)

		if intval != int32(1) {
			glog.Warningf("Found a use case where the val pair of a link was not 1: %d", intval)
		}
	}

	return nil
}

// FetchFirstKeyOfRemoteLinkBag is the entry point for retrieving links from
// a remote server-side side LinkBag.  In general, this method should not be
// called by end users. Instead, end users should call FetchEntriesOfRemoteLinkBag
//
// TODO: make this an unexported func?
func (dbc *Client) GetFirstKeyOfRemoteLinkBag(linkBag *oschema.OLinkBag) (lnk *oschema.OLink, err error) {
	defer catch(&err)
	buf := dbc.writeCommandAndSessionId(requestSBTREE_BONSAI_FIRST_KEY)
	if err != nil {
		return nil, err
	}

	writeLinkBagCollectionPointer(buf, linkBag)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return nil, err
	}

	firstKeyBytes := rw.ReadBytes(dbc.conx)

	var (
		typeByte  byte
		typeSerde binserde.OBinaryTypeSerializer
	)

	typeByte = firstKeyBytes[0]
	typeSerde = binserde.TypeSerializers[typeByte]
	result, err := typeSerde.Deserialize(bytes.NewBuffer(firstKeyBytes[1:]))
	if err != nil {
		return nil, err
	}

	firstLink, ok := result.(*oschema.OLink)

	if !ok {
		return nil, fmt.Errorf("Typecast error. Expected *oschema.OLink but is %T", result)
	}

	return firstLink, nil
}

func writeLinkBagCollectionPointer(buf *bytes.Buffer, linkBag *oschema.OLinkBag) {
	// (treePointer:collectionPointer)(changes)
	// where collectionPtr = (fileId:long)(pageIndex:long)(pageOffset:int)
	rw.WriteLong(buf, linkBag.GetFileID())

	rw.WriteLong(buf, linkBag.GetPageIndex())

	rw.WriteInt(buf, linkBag.GetPageOffset())
}

// ResolveLinks iterates over all the OLinks passed in and does a
// FetchRecordByRID for each one that has a null Record.
// TODO: maybe include a fetchplan here?
func (dbc *Client) ResolveLinks(links []*oschema.OLink) error {
	fetchPlan := ""
	for i := 0; i < len(links); i++ {
		if links[i].Record == nil {
			docs, err := dbc.GetRecordByRID(links[i].RID, fetchPlan)
			if err != nil {
				return err
			}
			// DEBUG
			if len(docs) > 1 {
				glog.Warningf("More than one record returned from GetRecordByRID. Please report this use case!")
			}
			// END DEBUG
			links[i].Record = docs[0]
		}
	}
	return nil
}

// Large LinkBags (aka RidBags) are stored on the server. To look up their
// size requires a call to the database.  The size is returned.  Note that the
// Size field of the linkBag is NOT updated.  That is left for the caller to
// decide whether to do.
func (dbc *Client) GetSizeOfRemoteLinkBag(linkBag *oschema.OLinkBag) (val int, err error) {
	defer catch(&err)
	buf := dbc.writeCommandAndSessionId(requestRIDBAG_GET_SIZE)

	writeLinkBagCollectionPointer(buf, linkBag)

	// changes => TODO: right now not supporting any change -> just writing empty changes
	rw.WriteBytes(buf, []byte{0, 0, 0, 0})

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return 0, err
	}

	size := rw.ReadInt(dbc.conx)

	return int(size), nil
}

// GetClusterCountIncludingDeleted gets the number of records in all
// the clusters specified *including* deleted records (applicable for
// autosharded storage only)
func (dbc *Client) GetClusterCountIncludingDeleted(clusterNames ...string) (int64, error) {
	return dbc.getClusterCount(true, clusterNames)
}

// FetchClusterCountIncludingDeleted gets the number of records in all the
// clusters specified. The count does NOT include deleted records in
// autosharded storage. Use FetchClusterCountIncludingDeleted if you want
// the count including deleted records
func (dbc *Client) GetClusterCount(clusterNames ...string) (int64, error) {
	return dbc.getClusterCount(false, clusterNames)
}

func (dbc *Client) getClusterCount(countTombstones bool, clusterNames []string) (count int64, err error) {
	defer catch(&err)

	clusterIDs := make([]int16, len(clusterNames))
	for i, name := range clusterNames {
		clusterID := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(name))
		if clusterID < 0 {
			// TODO: This is problematic - someone else may add the cluster not through this
			//       driver session and then this would fail - so options:
			//       1) do a lookup of all clusters on the DB
			//       2) provide a FetchClusterCountById(dbc, clusterID)
			return int64(0),
				fmt.Errorf("No cluster with name %s is known in database %s\n", name, dbc.currDb.Name)
		}
		clusterIDs[i] = clusterID
	}

	buf := dbc.writeCommandAndSessionId(requestDataClusterCOUNT)
	if err != nil {
		return int64(0), err
	}

	// specify number of clusterIDs being sent and then write the clusterIDs
	rw.WriteShort(buf, int16(len(clusterIDs)))

	for _, cid := range clusterIDs {
		rw.WriteShort(buf, cid)
	}

	// count-tombstones
	var ct byte
	if countTombstones {
		ct = byte(1)
	}
	rw.WriteByte(buf, ct) // presuming that 0 means "false" // TODO: replace with WriteBool?

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return int64(0), err
	}

	nrecs := rw.ReadLong(dbc.conx)

	return nrecs, err
}

func (dbc *Client) writeBuffer() *bytes.Buffer {
	buf := new(bytes.Buffer)
	buf.Reset()
	return buf
}

func (dbc *Client) writeCommandAndSessionId(cmd byte) *bytes.Buffer {
	if dbc.sessionId == noSessionId {
		panic(fmt.Errorf("Session not initialized"))
	}
	buf := dbc.writeBuffer()
	rw.WriteByte(buf, cmd)
	rw.WriteInt(buf, dbc.sessionId)
	return buf
}

func (dbc *Client) getLongFromDB(cmd byte) (val int64, err error) {
	defer catch(&err)
	val = -1
	buf := dbc.writeCommandAndSessionId(cmd)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return
	}

	// the answer to the query
	longFromDB := rw.ReadLong(dbc.conx)

	return longFromDB, nil
}

//
// Returns negative number if no cluster with `clusterName` is found
// in the clusters slice.
//
func findClusterWithName(clusters []OCluster, clusterName string) int16 {
	for _, cluster := range clusters {
		if cluster.Name == clusterName {
			return cluster.Id
		}
	}
	return int16(-1)
}

func (dbc *Client) readStatusCodeAndError() error {
	status := rw.ReadByte(dbc.conx)
	sessionId := rw.ReadInt(dbc.conx)
	if sessionId != dbc.sessionId {
		panic(fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)", sessionId, dbc.sessionId))
	}
	if status == responseStatusError {
		return rw.ReadErrorResponse(dbc.conx)
	}
	return nil
}
