package obinary

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oschema"
)

//
// OpenDatabase sends the REQUEST_DB_OPEN command to the OrientDb server to
// open the db in read/write mode.  The database name and type are required, plus
// username and password.  Database type should be one of the obinary constants:
// DocumentDbType or GraphDbType.
//
func OpenDatabase(dbc *DbClient, dbname, dbtype, username, passw string) error {
	buf := dbc.buf
	buf.Reset()

	// first byte specifies request type
	err := rw.WriteByte(buf, REQUEST_DB_OPEN)
	if err != nil {
		return err
	}

	// session-id - send a negative number to create a new server-side conx
	err = rw.WriteInt(buf, RequestNewSession)
	if err != nil {
		return err
	}

	err = rw.WriteStrings(buf, DriverName, DriverVersion)
	if err != nil {
		return err
	}

	err = rw.WriteShort(buf, dbc.binaryProtocolVersion)
	if err != nil {
		return err
	}

	// dbclient id - send as null, but cannot be null if clustered config
	// TODO: change to use dbc.clusteredConfig once that is added
	err = rw.WriteNull(buf)
	if err != nil {
		return err
	}

	// serialization-impl
	err = rw.WriteString(buf, dbc.serializationType)
	if err != nil {
		return err
	}

	// token-session  // TODO: hardcoded as false for now -> change later based on ClientOptions settings
	err = rw.WriteBool(buf, false)
	if err != nil {
		return err
	}

	// dbname, dbtype, username, password
	err = rw.WriteStrings(buf, dbname, dbtype, username, passw)
	if err != nil {
		return err
	}

	// now send to the OrientDB server
	n, err := dbc.conx.Write(buf.Bytes())
	fmt.Printf("number of bytes written: %v\n", n) // DEBUG
	if err != nil {
		return err
	}

	/* ---[ read back response ]--- */

	// first byte indicates success/error
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	dbc.currDb = NewDatabase(dbname, dbtype)

	// the first int returned is the session id sent - which was the `RequestNewSession` sentinel
	sessionValSent, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	if sessionValSent != RequestNewSession {
		return errors.New("Unexpected Error: Server did not return expected session-request-val that was sent")
	}

	// if status returned was ERROR, then the rest of server data is the exception info
	if status != RESPONSE_STATUS_OK {
		exceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return err
		}
		return fmt.Errorf("Server Error(s): %v", exceptions)
	}

	// for the REQUEST_DB_OPEN case, another int is returned which is the new sessionId
	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	dbc.sessionId = sessionId
	fmt.Printf("sessionId just set to: %v\n", dbc.sessionId) // DEBUG

	// next is the token, which may be null
	tokenBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	fmt.Printf("len tokenBytes: %v\n", len(tokenBytes)) // DEBUG
	fmt.Printf("tokenBytes: %v\n", tokenBytes)          // DEBUG
	dbc.token = tokenBytes

	// array of cluster info in this db // TODO: do we need to retain all this in memory?
	numClusters, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return err
	}

	clusters := make([]OCluster, 0, numClusters)

	for i := 0; i < int(numClusters); i++ {
		clusterName, err := rw.ReadString(dbc.conx)
		if err != nil {
			return err
		}
		clusterId, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return err
		}
		clusters = append(clusters, OCluster{Name: clusterName, Id: clusterId})
	}
	dbc.currDb.Clusters = clusters

	// cluster-config - bytes - null unless running server in clustered config
	// TODO: treating this as an opaque blob for now
	clusterCfg, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	dbc.currDb.ClustCfg = clusterCfg

	// orientdb server release - throwing away for now // TODO: need this?
	_, err = rw.ReadString(dbc.conx)
	if err != nil {
		return err
	}

	// load #0:0
	schemaRID, err := loadConfigRecord(dbc)
	if err != nil {
		return err
	}

	// load schemaRecord (usually #0:1)
	err = loadSchema(dbc, schemaRID)
	if err != nil {
		return err
	}

	return nil
}

//
// loadConfigRecord loads record #0:0 for the current database, caching
// some of the information returned into OStorageConfiguration
//
func loadConfigRecord(dbc *DbClient) (schemaRID string, err error) {
	// The config record comes back as type 'b' (binary?), which should just be converted
	// to a string then tokenized by the pipe char

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
	fmt.Printf("xxD5: payloadStatus: %v\n", payloadStatus)

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
		return schemaRID, errors.New("Second Payload status for #0:0 load was not 0. More than one record returned unexpectedly")
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
func loadSchema(dbc *DbClient, schemaRID string) error {
	docs, err := GetRecordByRID(dbc, schemaRID, "*:-1 index:0") // fetchPlan used by the Java client
	if err != nil {
		return err
	}
	// TODO: this idea of returning multiple docs has to be wrong
	if len(docs) != 1 {
		return fmt.Errorf("Load Record %s should only return one record. Returned: %d", schemaRID, len(docs))
	}

	/* ---[ schemaVersion ]--- */
	dbc.currDb.SchemaVersion = docs[0].Fields["schemaVersion"].Value.(int32)

	/* ---[ globalProperties ]--- */
	globalPropsFld := docs[0].Fields["globalProperties"]

	var globalProperty oschema.OGlobalProperty
	for _, pfield := range globalPropsFld.Value.([]interface{}) {
		pdoc := pfield.(*oschema.ODocument)
		globalProperty = oschema.NewGlobalPropertyFromDocument(pdoc)
		dbc.currDb.GlobalProperties[int(globalProperty.Id)] = globalProperty
	}

	fmt.Println("=======================================\n=======================================\n=======================================")
	fmt.Printf("dbc.currDb.SchemaVersion: %v\n", dbc.currDb.SchemaVersion)
	fmt.Printf("len(dbc.currDb.GlobalProperties): %v\n", len(dbc.currDb.GlobalProperties))
	fmt.Printf("dbc.currDb.GlobalProperties[19].Name: %v\n", dbc.currDb.GlobalProperties[19].Name)
	fmt.Printf("dbc.currDb.GlobalProperties[19].Name: %v\n", dbc.currDb.GlobalProperties[2].Type)
	fmt.Printf("dbc.currDb.GlobalProperties[19].Name: %v\n", dbc.currDb.GlobalProperties[23].Name)
	fmt.Println("=======================================\n=======================================\n=======================================")

	/* ---[ classes ]--- */
	var oclass *oschema.OClass
	classesFld := docs[0].Fields["classes"]
	for _, cfield := range classesFld.Value.([]interface{}) {
		cdoc := cfield.(*oschema.ODocument)
		oclass = oschema.NewOClassFromDocument(cdoc)
		fmt.Printf("\noclass =>> %v\n", *oclass)
	}

	return nil
}

//
// CloseDatabase closes down a session with a specific database that
// has already been opened (via OpenDatabase). This should be called
// when exiting an app or before starting a connection to a different
// OrientDB database.
//
func CloseDatabase(dbc *DbClient) error {
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
func GetDatabaseSize(dbc *DbClient) (int64, error) {
	return getLongFromDb(dbc, byte(REQUEST_DB_SIZE))
}

//
// GetNumRecordsInDatabase retrieves the number of records of the current
// database. It is a database-level operation, so OpenDatabase must have
// already been called first in order to start a session with the database.
//
func GetNumRecordsInDatabase(dbc *DbClient) (int64, error) {
	return getLongFromDb(dbc, byte(REQUEST_DB_COUNTRECORDS))
}

func DeleteRecordByRIDAsync(dbc *DbClient, rid string, recVersion int32) error {
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
func DeleteRecordByRID(dbc *DbClient, rid string, recVersion int32) error {
	return deleteByRID(dbc, rid, recVersion, false)
}

func deleteByRID(dbc *DbClient, rid string, recVersion int32, async bool) error {
	dbc.buf.Reset()
	var (
		clusterId  int16
		clusterPos int64
		err        error
	)
	rid = strings.TrimPrefix(rid, "#")
	clusterId, clusterPos, err = parseRid(rid)

	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_DELETE)
	if err != nil {
		return err
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return err
	}

	err = rw.WriteLong(dbc.buf, clusterPos)
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
func GetRecordByRID(dbc *DbClient, rid string, fetchPlan string) ([]*oschema.ODocument, error) {
	dbc.buf.Reset()
	var (
		clusterId  int16
		clusterPos int64
		err        error
	)
	rid = strings.TrimPrefix(rid, "#")
	clusterId, clusterPos, err = parseRid(rid)

	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_LOAD)
	if err != nil {
		return nil, err
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return nil, err
	}

	err = rw.WriteLong(dbc.buf, clusterPos)
	if err != nil {
		return nil, err
	}

	err = rw.WriteString(dbc.buf, fetchPlan)
	if err != nil {
		return nil, err
	}

	ignoreCache := true // hardcoding for now
	err = rw.WriteBool(dbc.buf, ignoreCache)
	if err != nil {
		return nil, err
	}

	loadTombstones := false // hardcoding for now
	err = rw.WriteBool(dbc.buf, loadTombstones)
	if err != nil {
		return nil, err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return nil, err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return nil, err
	}

	// this query can return multiple records (though I don't understand why)
	// so must do this in a loop
	docs := make([]*oschema.ODocument, 0, 1)
	for {
		payloadStatus, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, err
		}
		fmt.Printf("D5: payloadStatus: %v\n", payloadStatus)

		if payloadStatus == byte(0) {
			break
		}

		rectype, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, err
		}

		recversion, err := rw.ReadInt(dbc.conx)
		if err != nil {
			return nil, err
		}

		databytes, err := rw.ReadBytes(dbc.conx)
		if err != nil {
			return nil, err
		}

		// DEBUG
		fmt.Printf("rectype:%v, recversion:%v, len(databytes):%v\n", rectype, recversion, len(databytes))
		// END DEBUG

		if rectype == 'd' {
			// we don't know the classname so set empty value
			doc := oschema.NewDocument("")
			doc.Rid = rid
			doc.Version = recversion

			// the first byte specifies record serialization version
			// use it to look up serializer and strip off that byte
			serde := dbc.RecordSerDes[int(databytes[0])]
			err = serde.Deserialize(doc, bytes.NewBuffer(databytes[1:]))
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
// parseRid splits an OrientDB RID into its components parts - clusterId
// and clusterPos, returning the integer value of each. Note that the rid
// passed in must NOT have a leading '#'.
//
func parseRid(rid string) (clusterId int16, clusterPos int64, err error) {
	parts := strings.Split(rid, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("RID %s is not of form x:y", rid)
	}
	id64, err := strconv.ParseInt(parts[0], 10, 16)
	if err != nil {
		return 0, 0, err
	}
	clusterId = int16(id64)

	clusterPos, err = strconv.ParseInt(parts[1], 10, 64)
	return clusterId, clusterPos, err
}

// TODO: what is this going to return? a cursor?
func SQLQuery(dbc *DbClient, sql string) error {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_COMMAND)
	if err != nil {
		return err
	}

	mode := byte('s') // synchronous only supported for now

	err = rw.WriteByte(dbc.buf, mode)
	if err != nil {
		return err
	}

	// need a separate buffer to write the command-payload to, so
	// we can calculate its length before writing it to main dbc.buf
	commandBuf := new(bytes.Buffer)

	err = rw.WriteStrings(commandBuf, "q", sql) // q for query
	if err != nil {
		return err
	}

	// non-text-limit (-1 = use limit from query text)
	err = rw.WriteInt(commandBuf, -1)
	if err != nil {
		return err
	}

	// fetch plan // TODO: need to support fetch plans
	fetchPlan := ""
	err = rw.WriteString(commandBuf, fetchPlan)
	if err != nil {
		return err
	}

	// serialized-params => NONE currently supported => TODO: add support for these; see note below
	//// --------------------------------------- ////
	//// Serialized Parameters ODocument content ////
	//// --------------------------------------- ////
	// The ODocument have to contain a field called "params" of type Map.
	// The Map should have as key, in case of positional perameters the numeric
	// position of the parameter, in case of named parameters the name of the
	// parameter and as value the value of the parameter.
	err = rw.WriteBytes(commandBuf, make([]byte, 0, 0))
	if err != nil {
		return err
	}

	serializedCmd := commandBuf.Bytes()
	fmt.Printf("serializedCmd:\n%v\n", serializedCmd) // DEBUG

	// command-payload-length and command-payload
	err = rw.WriteBytes(dbc.buf, serializedCmd)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	finalBytes := dbc.buf.Bytes()
	fmt.Printf("finalBytes:\n%v\n", finalBytes) // DEBUG

	_, err = dbc.conx.Write(finalBytes)
	if err != nil {
		return err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return err
	}

	resType, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	resultType := int32(resType)
	fmt.Printf("resultType: %v\n", string(resultType))

	if resultType == 'n' {
		fmt.Println("resultVal: Null")

	} else if resultType == 'r' {
		fmt.Println("Now need to parse a record")
		record, err := rw.ReadBytes(dbc.conx)
		if err != nil {
			return err
		}
		fmt.Printf("record: %v\n", record)

	} else if resultType == 'l' {
		err = readResultSet(dbc) // TODO: need to devise what a ResultSet is going to look like
		if err != nil {
			return err
		}

	} else {
		fmt.Println(">> Not yet supported")
	}

	return nil
}

//
// GetClusterDataRange returns the range of record ids for a cluster
//
func GetClusterDataRange(dbc *DbClient, clusterName string) (begin, end int64, err error) {
	dbc.buf.Reset()

	clusterId := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterId < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a GetClusterRangeById(dbc, clusterId)
		return begin, end,
			fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DATARANGE)
	if err != nil {
		return begin, end, err
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return begin, end, err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return begin, end, err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return begin, end, err
	}

	begin, err = rw.ReadLong(dbc.conx)
	if err != nil {
		return begin, end, err
	}

	end, err = rw.ReadLong(dbc.conx)
	return begin, end, err
}

//
// AddCluster adds a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// The clusterId is returned if the command is successful.
//
func AddCluster(dbc *DbClient, clusterName string) (clusterId int16, err error) {
	dbc.buf.Reset()

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_ADD)
	if err != nil {
		return int16(0), err
	}

	cname := strings.ToLower(clusterName)

	err = rw.WriteString(dbc.buf, cname)
	if err != nil {
		return int16(0), err
	}

	err = rw.WriteShort(dbc.buf, -1) // -1 means generate new cluster id
	if err != nil {
		return int16(0), err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int16(0), err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int16(0), err
	}

	clusterId, err = rw.ReadShort(dbc.conx)
	if err != nil {
		return clusterId, err
	}

	dbc.currDb.Clusters = append(dbc.currDb.Clusters, OCluster{cname, clusterId})
	return clusterId, err
}

//
// DropCluster drops a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// If nil is returned, then the action succeeded.
//
func DropCluster(dbc *DbClient, clusterName string) error {
	dbc.buf.Reset()

	fmt.Printf("Attempt DROP: %v\n", clusterName) // DEBUG

	clusterId := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterId < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a DropClusterById(dbc, clusterId)
		return fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err := writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DROP)
	if err != nil {
		return err
	}

	err = rw.WriteShort(dbc.buf, clusterId)
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

	delStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}
	if delStatus != byte(1) {
		return fmt.Errorf("Drop cluster action failed. Return code from server was not '1', but %d",
			delStatus)
	}

	return nil
}

//
// GetClusterCountIncludingDeleted gets the number of records in all
// the clusters specified *including* deleted records (applicable for
// autosharded storage only)
//
func GetClusterCountIncludingDeleted(dbc *DbClient, clusterNames ...string) (count int64, err error) {
	return getClusterCount(dbc, true, clusterNames)
}

//
// GetClusterCountIncludingDeleted gets the number of records in all the
// clusters specified. The count does NOT include deleted records in
// autosharded storage. Use GetClusterCountIncludingDeleted if you want
// the count including deleted records
//
func GetClusterCount(dbc *DbClient, clusterNames ...string) (count int64, err error) {
	return getClusterCount(dbc, false, clusterNames)
}

func getClusterCount(dbc *DbClient, countTombstones bool, clusterNames []string) (count int64, err error) {
	dbc.buf.Reset()

	clusterIds := make([]int16, len(clusterNames))
	for i, name := range clusterNames {
		clusterId := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(name))
		if clusterId < 0 {
			// TODO: This is problematic - someone else may add the cluster not through this
			//       driver session and then this would fail - so options:
			//       1) do a lookup of all clusters on the DB
			//       2) provide a GetClusterCountById(dbc, clusterId)
			return int64(0),
				fmt.Errorf("No cluster with name %s is known in database %s\n",
					name, dbc.currDb.Name)
		}
		clusterIds[i] = clusterId
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_COUNT)
	if err != nil {
		return int64(0), err
	}

	// specify number of clusterIds being sent and then write the clusterIds
	err = rw.WriteShort(dbc.buf, int16(len(clusterIds)))
	if err != nil {
		return int64(0), err
	}

	for _, cid := range clusterIds {
		err = rw.WriteShort(dbc.buf, cid)
		if err != nil {
			return int64(0), err
		}
	}

	// count-tombstones
	var ct byte
	if countTombstones {
		ct = byte(1)
	}
	err = rw.WriteByte(dbc.buf, ct) // presuming that 0 means "false"
	if err != nil {
		return int64(0), err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(0), err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int64(0), err
	}

	nrecs, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return int64(0), err
	}

	return nrecs, err
}

func writeCommandAndSessionId(dbc *DbClient, cmd byte) error {
	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	err := rw.WriteByte(dbc.buf, cmd)
	if err != nil {
		return err
	}

	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return err
	}

	return nil
}

func getLongFromDb(dbc *DbClient, cmd byte) (int64, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, cmd)
	if err != nil {
		return int64(-1), err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(-1), err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int64(-1), err
	}

	// the answer to the query
	longFromDb, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return int64(-1), err
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

func readStatusCodeAndSessionId(dbc *DbClient) error {
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	if sessionId != dbc.sessionId {
		return fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	if status == RESPONSE_STATUS_ERROR {
		serverExceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return err
		}
		return fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	return nil
}

// TODO: needs to actually return something =>
//       it will work like an external iterator where the user passes in the type to read into
func readResultSet(dbc *DbClient) error {
	// for Collection
	// next val is: (collection-size:int)
	// and then each record is serialized according to format:
	// (0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)

	resultSetSize, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return err
	}

	fmt.Printf("++ Number of records returned: %v\n", resultSetSize)

	rsize := int(resultSetSize)
	for i := 0; i < rsize; i++ {
		// TODO: move code below to readRecordInResultSet
		// this apparently should always be zero for serialized records -> not sure it's meaning
		zero, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return err
		}
		if zero != int16(0) {
			return fmt.Errorf("ERROR: readResultSet: expected short value of 0 but is %d", zero)
		}

		recType, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return err
		}
		fmt.Printf("!!recType: %v\n", recType)

		clusterId, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return err
		}
		fmt.Printf("!!clusterId: %v\n", clusterId)

		clusterPos, err := rw.ReadLong(dbc.conx)
		if err != nil {
			return err
		}
		fmt.Printf("!!clusterPos: %v\n", clusterPos)

		recVersion, err := rw.ReadInt(dbc.conx)
		if err != nil {
			return err
		}
		fmt.Printf("!!recVersion: %v\n", recVersion)
		if recType == byte('d') { // Document
			var doc *oschema.ODocument
			rid := fmt.Sprintf("%d:%d", clusterId, clusterPos)
			recBytes, err := rw.ReadBytes(dbc.conx)
			if err != nil {
				return err
			}
			doc, err = createDocument(rid, recVersion, recBytes, dbc)
			if err != nil {
				return err
			}
			fmt.Printf("ResultSet Doc: \n%v\n", doc) // DEBUG
		} else {
			_, file, line, _ := runtime.Caller(0)
			return fmt.Errorf("%v: %v: Record type %v is not yet supported", file, line+1, recType)
		}
	} // end for loop

	end, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}
	if end != byte(0) {
		return fmt.Errorf("Final Byte read from collection result set was not 0, but was: %v", end)
	}
	return nil
}
