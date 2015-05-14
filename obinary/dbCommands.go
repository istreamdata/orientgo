package obinary

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary/binserde"
	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
)

//
// OpenDatabase sends the REQUEST_DB_OPEN command to the OrientDb server to
// open the db in read/write mode.  The database name and type are required, plus
// username and password.  Database type should be one of the obinary constants:
// DocumentDbType or GraphDbType.
//
func OpenDatabase(dbc *DBClient, dbname string, dbtype constants.DatabaseType, username, passw string) error {
	buf := dbc.buf
	buf.Reset()

	// first byte specifies request type
	err := rw.WriteByte(buf, REQUEST_DB_OPEN)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// session-id - send a negative number to create a new server-side conx
	err = rw.WriteInt(buf, RequestNewSession)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteStrings(buf, DriverName, DriverVersion)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteShort(buf, dbc.binaryProtocolVersion)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// dbclient id - send as null, but cannot be null if clustered config
	// TODO: change to use dbc.clusteredConfig once that is added
	err = rw.WriteNull(buf)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// serialization-impl
	err = rw.WriteString(buf, dbc.serializationType)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// token-session  // TODO: hardcoded as false for now -> change later based on ClientOptions settings
	err = rw.WriteBool(buf, false)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// dbname, dbtype, username, password
	err = rw.WriteStrings(buf, dbname, string(dbtype), username, passw)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// now send to the OrientDB server
	_, err = dbc.conx.Write(buf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	/* ---[ read back response ]--- */

	// first byte indicates success/error
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	dbc.currDb = NewDatabase(dbname, dbtype)

	// the first int returned is the session id sent - which was the `RequestNewSession` sentinel
	sessionValSent, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if sessionValSent != RequestNewSession {
		return errors.New("Unexpected Error: Server did not return expected session-request-val that was sent")
	}

	// if status returned was ERROR, then the rest of server data is the exception info
	if status != RESPONSE_STATUS_OK {
		exceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		return fmt.Errorf("Server Error(s): %v", exceptions)
	}

	// for the REQUEST_DB_OPEN case, another int is returned which is the new sessionId
	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	dbc.sessionId = sessionId

	// next is the token, which may be null
	tokenBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	dbc.token = tokenBytes

	// array of cluster info in this db // TODO: do we need to retain all this in memory?
	numClusters, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	clusters := make([]OCluster, 0, numClusters)

	for i := 0; i < int(numClusters); i++ {
		clusterName, err := rw.ReadString(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		clusterId, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		clusters = append(clusters, OCluster{Name: clusterName, Id: clusterId})
	}
	dbc.currDb.Clusters = clusters

	// cluster-config - bytes - null unless running server in clustered config
	// TODO: treating this as an opaque blob for now
	clusterCfg, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	dbc.currDb.ClustCfg = clusterCfg

	// orientdb server release - throwing away for now // TODO: need this?
	_, err = rw.ReadString(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	//
	/* ---[ load #0:0 - config record ]--- */
	schemaRID, err := loadConfigRecord(dbc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	//
	/* ---[ load #0:1 - schema record ]--- */
	err = loadSchema(dbc, schemaRID)
	if err != nil {
		return oerror.NewTrace(err)
	}

	return nil
}

//
// loadConfigRecord loads record #0:0 for the current database, caching
// some of the information returned into OStorageConfiguration
//
func loadConfigRecord(dbc *DBClient) (schemaRID string, err error) {
	// The config record comes back as type 'b' (raw bytes), which should
	// just be converted to a string then tokenized by the pipe char

	dbc.buf.Reset()
	var (
		clusterId  int16
		clusterPos int64
	)
	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_LOAD)
	if err != nil {
		return schemaRID, err
	}

	clusterId = 0
	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return schemaRID, err
	}

	clusterPos = 0
	err = rw.WriteLong(dbc.buf, clusterPos)
	if err != nil {
		return schemaRID, err
	}

	fetchPlan := "*:-1 index:0"
	err = rw.WriteString(dbc.buf, fetchPlan)
	if err != nil {
		return schemaRID, err
	}

	ignoreCache := true
	err = rw.WriteBool(dbc.buf, ignoreCache)
	if err != nil {
		return schemaRID, err
	}

	loadTombstones := true // based on Java client code
	err = rw.WriteBool(dbc.buf, loadTombstones)
	if err != nil {
		return schemaRID, err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return schemaRID, err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return schemaRID, err
	}

	payloadStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	if payloadStatus == byte(0) {
		return schemaRID, errors.New("Payload status for #0:0 load was 0. No config data returned.")
	}

	rectype, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	// this is the record version - don't see a reason to check or cache it right now
	_, err = rw.ReadInt(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	databytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	if rectype != 'b' {
		if err != nil {
			return schemaRID, fmt.Errorf("Expected rectype %d, but was: %d", 'b', rectype)
		}
	}

	payloadStatus, err = rw.ReadByte(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	if payloadStatus != byte(0) {
		return schemaRID,
			errors.New("Second Payload status for #0:0 load was not 0. More than one record returned unexpectedly")
	}

	err = parseConfigRecord(dbc.currDb, string(databytes))
	if err != nil {
		return schemaRID, err
	}

	schemaRID = dbc.currDb.StorageCfg.schemaRID
	return schemaRID, err
}

//
// parseConfigRecord takes the pipe-separate values that comes back
// from reading record #0:0 and turns it into an OStorageConfiguration
// object, which it adds to the db database object.
// TODO: move this function to be a method of OStorageConfiguration?
//
func parseConfigRecord(db *ODatabase, psvData string) error {
	sc := OStorageConfiguration{}

	toks := strings.Split(psvData, "|")

	version, err := strconv.ParseInt(toks[0], 10, 8)
	if err != nil {
		return err
	}

	sc.version = byte(version)
	sc.name = strings.TrimSpace(toks[1])
	sc.schemaRID = strings.TrimSpace(toks[2])
	sc.dictionaryRID = strings.TrimSpace(toks[3])
	sc.idxMgrRID = strings.TrimSpace(toks[4])
	sc.localeLang = strings.TrimSpace(toks[5])
	sc.localeCountry = strings.TrimSpace(toks[6])
	sc.dateFmt = strings.TrimSpace(toks[7])
	sc.dateTimeFmt = strings.TrimSpace(toks[8])
	sc.timezone = strings.TrimSpace(toks[9])

	db.StorageCfg = sc

	return nil
}

//
// loadSchema loads record #0:1 for the current database, caching the
// SchemaVersion, GlobalProperties and Classes info in the current ODatabase
// object (dbc.currDb).
//
func loadSchema(dbc *DBClient, schemaRID string) error {
	docs, err := GetRecordByRID(dbc, schemaRID, "*:-1 index:0") // fetchPlan used by the Java client
	if err != nil {
		return err
	}
	// TODO: this idea of returning multiple docs has to be wrong
	if len(docs) != 1 {
		return fmt.Errorf("Load Record %s should only return one record. Returned: %d", schemaRID, len(docs))
	}

	/* ---[ schemaVersion ]--- */
	dbc.currDb.SchemaVersion = docs[0].GetField("schemaVersion").Value.(int32)

	/* ---[ globalProperties ]--- */
	globalPropsFld := docs[0].GetField("globalProperties")

	var globalProperty oschema.OGlobalProperty
	for _, pfield := range globalPropsFld.Value.([]interface{}) {
		pdoc := pfield.(*oschema.ODocument)
		globalProperty = oschema.NewGlobalPropertyFromDocument(pdoc)
		dbc.currDb.GlobalProperties[int(globalProperty.Id)] = globalProperty
	}

	ogl.Debugln("=======================================")
	ogl.Debugln("=======================================")
	ogl.Debugf("dbc.currDb.SchemaVersion: %v\n", dbc.currDb.SchemaVersion)
	ogl.Debugf("len(dbc.currDb.GlobalProperties): %v\n", len(dbc.currDb.GlobalProperties))
	ogl.Debugf("dbc.currDb.GlobalProperties: %v\n", dbc.currDb.GlobalProperties)
	ogl.Debugln("=======================================")
	ogl.Debugln("=======================================")

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

//
// CloseDatabase closes down a session with a specific database that
// has already been opened (via OpenDatabase). This should be called
// when exiting an app or before starting a connection to a different
// OrientDB database.
//
func CloseDatabase(dbc *DBClient) error {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_DB_CLOSE)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	// the server has no response to a DB_CLOSE

	// remove session, token and currDb info
	dbc.sessionId = NoSessionId
	dbc.token = nil
	dbc.currDb = nil // TODO: anything in currDb that needs to be closed?

	return nil
}

//
// GetDatabaseSize retrieves the size of the current database in bytes.
// It is a database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
//
func GetDatabaseSize(dbc *DBClient) (int64, error) {
	return getLongFromDb(dbc, byte(REQUEST_DB_SIZE))
}

//
// GetNumRecordsInDatabase retrieves the number of records of the current
// database. It is a database-level operation, so OpenDatabase must have
// already been called first in order to start a session with the database.
//
func GetNumRecordsInDatabase(dbc *DBClient) (int64, error) {
	return getLongFromDb(dbc, byte(REQUEST_DB_COUNTRECORDS))
}

func DeleteRecordByRIDAsync(dbc *DBClient, rid string, recVersion int32) error {
	return deleteByRID(dbc, rid, recVersion, true)
}

//
// DeleteRecordByRID deletes a record specified by its RID and its version.
// This is the synchronous version where the server confirms whether the
// delete was successful and the client reports that back to the caller.
// See DeleteRecordByRIDAsync for the async version.
//
// If nil is returned, delete succeeded.
// If error is returned, delete request was either never issued, or there was
// a problem on the server end or the record did not exist in the database.
//
func DeleteRecordByRID(dbc *DBClient, rid string, recVersion int32) error {
	return deleteByRID(dbc, rid, recVersion, false)
}

func deleteByRID(dbc *DBClient, rid string, recVersion int32, async bool) error {
	dbc.buf.Reset()

	orid := oschema.NewORIDFromString(rid)

	err := writeCommandAndSessionId(dbc, REQUEST_RECORD_DELETE)
	if err != nil {
		return err
	}

	err = rw.WriteShort(dbc.buf, orid.ClusterID)
	if err != nil {
		return err
	}

	err = rw.WriteLong(dbc.buf, orid.ClusterPos)
	if err != nil {
		return err
	}

	err = rw.WriteInt(dbc.buf, recVersion)
	if err != nil {
		return err
	}

	// sync mode ; 0 = synchronous; 1 = asynchronous
	var syncMode byte
	if async {
		syncMode = byte(1)
	}
	err = rw.WriteByte(dbc.buf, syncMode)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return err
	}

	payloadStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	// status 1 means record was deleted;
	// status 0 means record was not deleted (either failed or didn't exist)
	if payloadStatus == byte(0) {
		return fmt.Errorf("Server reports record %s was not deleted. Either failed or did not exist.",
			rid)
	}

	return nil
}

//
// GetRecordById takes an RID of the form N:M or #N:M and reads that record from
// the database.
// NOTE: for now I'm assuming all records are Documents (they can also be "raw bytes" or "flat data")
// and for some reason I don't understand, multiple records can be returned, so I'm returning
// a slice of ODocument
//
// TODO: may also want to expose options: ignoreCache, loadTombstones bool
func GetRecordByRID(dbc *DBClient, rid string, fetchPlan string) ([]*oschema.ODocument, error) {
	dbc.buf.Reset()

	orid := oschema.NewORIDFromString(rid)

	err := writeCommandAndSessionId(dbc, REQUEST_RECORD_LOAD)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, orid.ClusterID)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = rw.WriteLong(dbc.buf, orid.ClusterPos)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = rw.WriteString(dbc.buf, fetchPlan)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	ignoreCache := true // hardcoding for now
	err = rw.WriteBool(dbc.buf, ignoreCache)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	loadTombstones := false // hardcoding for now
	err = rw.WriteBool(dbc.buf, loadTombstones)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// this query can return multiple records (though I don't understand why)
	// so must do this in a loop
	docs := make([]*oschema.ODocument, 0, 1)
	for {
		payloadStatus, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		if payloadStatus == byte(0) {
			break
		}

		rectype, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		recversion, err := rw.ReadInt(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		databytes, err := rw.ReadBytes(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		ogl.Debugf("rectype:%v, recversion:%v, len(databytes):%v\n", rectype, recversion, len(databytes))

		if rectype == 'd' {
			// we don't know the classname so set empty value
			doc := oschema.NewDocument("")
			doc.RID = orid
			doc.Version = recversion

			// the first byte specifies record serialization version
			// use it to look up serializer
			serde := dbc.currDb.RecordSerDes[int(databytes[0])]
			// then strip off the version byte and send the data to the serde
			err = serde.Deserialize(dbc, doc, bytes.NewBuffer(databytes[1:]))
			if err != nil {
				return nil, fmt.Errorf("ERROR in Deserialize for rid %v: %v\n", rid, err)
			}
			docs = append(docs, doc)

		} else {
			return nil,
				fmt.Errorf("Only `document` records are currently supported by the client. Record returned was type: %v", rectype)
		}
	}

	return docs, nil
}

//
// parseRid splits an OrientDB RID into its components parts - clusterID
// and clusterPos, returning the integer value of each. Note that the rid
// passed in must NOT have a leading '#'.
//
func parseRid(rid string) (clusterID int16, clusterPos int64, err error) {
	parts := strings.Split(rid, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("RID %s is not of form x:y", rid)
	}
	id64, err := strconv.ParseInt(parts[0], 10, 16)
	if err != nil {
		return 0, 0, oerror.NewTrace(err)
	}
	clusterID = int16(id64)

	clusterPos, err = strconv.ParseInt(parts[1], 10, 64)
	return clusterID, clusterPos, err
}

//
// ReloadSchema should be called after a schema is altered, such as properties
// added, deleted or renamed.
//
func ReloadSchema(dbc *DBClient) error {
	return loadSchema(dbc, "#0:1")
}

//
// GetClusterDataRange returns the range of record ids for a cluster
//
func GetClusterDataRange(dbc *DBClient, clusterName string) (begin, end int64, err error) {
	dbc.buf.Reset()

	clusterID := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterID < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a GetClusterRangeById(dbc, clusterID)
		return begin, end,
			fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DATARANGE)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, clusterID)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	begin, err = rw.ReadLong(dbc.conx)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	end, err = rw.ReadLong(dbc.conx)
	return begin, end, err
}

//
// AddCluster adds a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// The clusterID is returned if the command is successful.
//
func AddCluster(dbc *DBClient, clusterName string) (clusterID int16, err error) {
	dbc.buf.Reset()

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_ADD)
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	cname := strings.ToLower(clusterName)

	err = rw.WriteString(dbc.buf, cname)
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, -1) // -1 means generate new cluster id
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	clusterID, err = rw.ReadShort(dbc.conx)
	if err != nil {
		return clusterID, oerror.NewTrace(err)
	}

	dbc.currDb.Clusters = append(dbc.currDb.Clusters, OCluster{cname, clusterID})
	return clusterID, err
}

//
// DropCluster drops a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// If nil is returned, then the action succeeded.
//
func DropCluster(dbc *DBClient, clusterName string) error {
	dbc.buf.Reset()

	clusterID := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterID < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a DropClusterById(dbc, clusterID)
		return fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err := writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DROP)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, clusterID)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	delStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if delStatus != byte(1) {
		return fmt.Errorf("Drop cluster action failed. Return code from server was not '1', but %d",
			delStatus)
	}

	return nil
}

//
// Fetch Entries Major
// TODO: need to enquire when inclusive should be true/false
//
func GetKeysOfRemoteLinkBag(dbc *DBClient, linkBag *oschema.OLinkBag, inclusive bool) error {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = writeLinkBagCollectionPointer(dbc.buf, linkBag)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	// TODO: missing steps

	return nil
}

//
// DOCUMENT ME
// should this fill in the linkBag rather than returning the link ???
//
func GetFirstKeyOfRemoteLinkBag(dbc *DBClient, linkBag *oschema.OLinkBag) (*oschema.OLink, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_SBTREE_BONSAI_FIRST_KEY)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = writeLinkBagCollectionPointer(dbc.buf, linkBag)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	lvl := ogl.GetLevel()
	ogl.SetLevel(ogl.DEBUG)
	defer ogl.SetLevel(lvl)
	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	firstKeyBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	var (
		typeByte  byte
		typeSerde binserde.OBinaryTypeSerializer
	)

	typeByte = firstKeyBytes[0]
	typeSerde = binserde.TypeSerializers[typeByte]
	result, err := typeSerde.Deserialize(bytes.NewBuffer(firstKeyBytes[1:]))
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	firstLink, ok := result.(*oschema.OLink)

	if !ok {
		// TODO: fmt.Errorf is an anti-pattern
		return nil, fmt.Errorf("Typecast error. Expected *oschema.OLink but is %T", result)
	}

	return firstLink, nil
}

func writeLinkBagCollectionPointer(buf *bytes.Buffer, linkBag *oschema.OLinkBag) error {
	// (treePointer:collectionPointer)(changes)
	// where collectionPtr = (fileId:long)(pageIndex:long)(pageOffset:int)
	err := rw.WriteLong(buf, linkBag.GetFileID())
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteLong(buf, linkBag.GetPageIndex())
	if err != nil {
		return oerror.NewTrace(err)
	}

	return rw.WriteInt(buf, linkBag.GetPageOffset())
}

//
// Large LinkBags (aka RidBags) are stored on the server. To look up their
// size requires a call to the database.  The size is returned.  Note that the
// Size field of the linkBag is NOT updated.  That is left for the caller to
// decide whether to do.
//
func GetSizeOfRemoteLinkBag(dbc *DBClient, linkBag *oschema.OLinkBag) (int, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_RIDBAG_GET_SIZE)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}

	err = writeLinkBagCollectionPointer(dbc.buf, linkBag)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}

	// changes => TODO: right now not supporting any change -> just writing empty changes
	err = rw.WriteBytes(dbc.buf, []byte{0, 0, 0, 0})
	if err != nil {
		return 0, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return 0, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}

	size, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}

	return int(size), nil
}

//
// GetClusterCountIncludingDeleted gets the number of records in all
// the clusters specified *including* deleted records (applicable for
// autosharded storage only)
//
func GetClusterCountIncludingDeleted(dbc *DBClient, clusterNames ...string) (count int64, err error) {
	return getClusterCount(dbc, true, clusterNames)
}

//
// GetClusterCountIncludingDeleted gets the number of records in all the
// clusters specified. The count does NOT include deleted records in
// autosharded storage. Use GetClusterCountIncludingDeleted if you want
// the count including deleted records
//
func GetClusterCount(dbc *DBClient, clusterNames ...string) (count int64, err error) {
	return getClusterCount(dbc, false, clusterNames)
}

func getClusterCount(dbc *DBClient, countTombstones bool, clusterNames []string) (count int64, err error) {
	dbc.buf.Reset()

	clusterIDs := make([]int16, len(clusterNames))
	for i, name := range clusterNames {
		clusterID := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(name))
		if clusterID < 0 {
			// TODO: This is problematic - someone else may add the cluster not through this
			//       driver session and then this would fail - so options:
			//       1) do a lookup of all clusters on the DB
			//       2) provide a GetClusterCountById(dbc, clusterID)
			return int64(0),
				fmt.Errorf("No cluster with name %s is known in database %s\n",
					name, dbc.currDb.Name)
		}
		clusterIDs[i] = clusterID
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_COUNT)
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	// specify number of clusterIDs being sent and then write the clusterIDs
	err = rw.WriteShort(dbc.buf, int16(len(clusterIDs)))
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	for _, cid := range clusterIDs {
		err = rw.WriteShort(dbc.buf, cid)
		if err != nil {
			return int64(0), oerror.NewTrace(err)
		}
	}

	// count-tombstones
	var ct byte
	if countTombstones {
		ct = byte(1)
	}
	err = rw.WriteByte(dbc.buf, ct) // presuming that 0 means "false"
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	nrecs, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	return nrecs, err
}

func writeCommandAndSessionId(dbc *DBClient, cmd byte) error {
	if dbc.sessionId == NoSessionId {
		return oerror.SessionNotInitialized{}
	}

	err := rw.WriteByte(dbc.buf, cmd)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return oerror.NewTrace(err)
	}

	return nil
}

func getLongFromDb(dbc *DBClient, cmd byte) (int64, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, cmd)
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	// the answer to the query
	longFromDb, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	return longFromDb, nil
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

func readStatusCodeAndSessionId(dbc *DBClient) error {
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if sessionId != dbc.sessionId {
		// FIXME: use of fmt.Errorf is an anti-pattern
		return fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	if status == RESPONSE_STATUS_ERROR {
		serverException, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		return serverException
	}

	return nil
}
