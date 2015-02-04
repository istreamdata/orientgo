package obinary

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	err := WriteByte(buf, REQUEST_DB_OPEN)
	if err != nil {
		return err
	}

	// session-id - send a negative number to create a new server-side conx
	err = WriteInt(buf, RequestNewSession)
	if err != nil {
		return err
	}

	err = WriteStrings(buf, DriverName, DriverVersion)
	if err != nil {
		return err
	}

	err = WriteShort(buf, dbc.binaryProtocolVersion)
	if err != nil {
		return err
	}

	// dbclient id - send as null, but cannot be null if clustered config
	// TODO: change to use dbc.clusteredConfig once that is added
	err = WriteNull(buf)
	if err != nil {
		return err
	}

	// serialization-impl
	err = WriteString(buf, dbc.serializationImpl)
	if err != nil {
		return err
	}

	// token-session  // TODO: hardcoded as false for now -> change later based on ClientOptions settings
	err = WriteBool(buf, false)
	if err != nil {
		return err
	}

	// dbname, dbtype, username, password
	err = WriteStrings(buf, dbname, dbtype, username, passw)
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
	status, err := ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	dbc.currDb = &ODatabase{Name: dbname, Typ: dbtype}

	// the first int returned is the session id sent - which was the `RequestNewSession` sentinel
	sessionValSent, err := ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	if sessionValSent != RequestNewSession {
		return errors.New("Unexpected Error: Server did not return expected session-request-val that was sent")
	}

	// if status returned was ERROR, then the rest of server data is the exception info
	if status != SUCCESS {
		exceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return err
		}
		return fmt.Errorf("Server Error(s): %v", exceptions)
	}

	// for the REQUEST_DB_OPEN case, another int is returned which is the new sessionId
	sessionId, err := ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	dbc.sessionId = sessionId
	fmt.Printf("sessionId just set to: %v\n", dbc.sessionId) // DEBUG

	// next is the token, which may be null
	tokenBytes, err := ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	fmt.Printf("len tokenBytes: %v\n", len(tokenBytes)) // DEBUG
	fmt.Printf("tokenBytes: %v\n", tokenBytes)          // DEBUG
	dbc.token = tokenBytes

	// array of cluster info in this db // TODO: do we need to retain all this in memory?
	numClusters, err := ReadShort(dbc.conx)
	if err != nil {
		return err
	}

	clusters := make([]OCluster, 0, numClusters)

	for i := 0; i < int(numClusters); i++ {
		clusterName, err := ReadString(dbc.conx)
		if err != nil {
			return err
		}
		clusterId, err := ReadShort(dbc.conx)
		if err != nil {
			return err
		}
		clusters = append(clusters, OCluster{Name: clusterName, Id: clusterId})
	}
	dbc.currDb.Clusters = clusters

	// cluster-config - bytes - null unless running server in clustered config
	// TODO: treating this as an opaque blob for now
	clusterCfg, err := ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	dbc.currDb.ClustCfg = clusterCfg

	// orientdb server release - throwing away for now // TODO: need this?
	_, err = ReadString(dbc.conx)
	if err != nil {
		return err
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

	// mark this session as gone
	dbc.sessionId = NoSessionId
	// TODO: probably need to set token to nil as well?
	dbc.currDb = nil

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

//
// TODO: this probably needs to map a record into a JSON obj?  Or some other datastructure
// TODO: put fetchPlan, ignoreCache and loadTombstons into a map or Options obj?
//
func GetRecordByRID(dbc *DbClient, rid string, fetchPlan string, ignoreCache, loadTombstones bool) error {
	dbc.buf.Reset()
	// LEFT OFF -> first thing: parse rid into cluster-id (short) and cluster-pos (long)
	var (
		clusterId  int16
		clusterPos int64
		err        error
	)
	clusterId, clusterPos, err = parseRid(rid)

	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_LOAD)
	if err != nil {
		return err
	}

	err = WriteShort(dbc.buf, clusterId)
	if err != nil {
		return err
	}

	err = WriteLong(dbc.buf, clusterPos)
	if err != nil {
		return err
	}

	err = WriteString(dbc.buf, fetchPlan)
	if err != nil {
		return err
	}

	err = WriteBool(dbc.buf, ignoreCache)
	if err != nil {
		return err
	}

	err = WriteBool(dbc.buf, loadTombstones)
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

	for {
		payloadStatus, err := ReadByte(dbc.conx)
		if err != nil {
			return err
		}
		fmt.Printf("D5: payloadStatus: %v\n", payloadStatus)

		if payloadStatus == byte(0) {
			break
		}

		rectype, err := ReadByte(dbc.conx)
		fmt.Printf("D6a: rectype: %T: %v\n", rectype, rectype)
		fmt.Printf("D6b: rectype as str: %v\n", string(rectype))

		recversion, err := ReadInt(dbc.conx)
		fmt.Printf("D7: recversion: %v\n", recversion)

		databytes, err := ReadBytes(dbc.conx)
		fmt.Printf("D8: len:databytes: %v\n", len(databytes))
		if err != nil {
			fmt.Printf("D9: ERROR: %v\n", err)
		}
		// err = readRecord(dbc)
		// if err != nil {
		// 	return err
		// }
	}

	return nil
}

// TODO: needs to read record into some datastructure
func readRecord(dbc *DbClient) error {
	fmt.Printf("%v\n", "DEBUG 10")
	recType, err := ReadByte(dbc.buf)
	if err != nil {
		return err
	}
	fmt.Printf("D11: recType: %v\n", recType)

	recVersion, err := ReadInt(dbc.buf)
	if err != nil {
		return err
	}
	fmt.Printf("D12: recVersion: %v\n", recVersion)

	recData, err := ReadBytes(dbc.buf)
	if err != nil {
		return err
	}
	fmt.Printf("D13: len:recData: %v\n", len(recData))

	recTypeStr := string(recType)

	// DEBUG
	fmt.Printf("record type is: %v\n", recTypeStr)
	fmt.Printf("record version is: %v\n", recVersion)
	fmt.Printf("record data is: %v\n", string(recData))
	// END DEBUG
	return nil
}

func parseRid(rid string) (clusterId int16, clusterPos int64, err error) {
	parts := strings.Split(rid, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("RID %s is not of form x:y", rid)
	}
	if strings.HasPrefix(parts[0], "#") {
		parts[0] = parts[0][1:]
	}
	id64, err := strconv.ParseInt(parts[0], 10, 16)
	if err != nil {
		return 0, 0, err
	}
	clusterId = int16(id64)

	clusterPos, err = strconv.ParseInt(parts[1], 10, 64)
	return clusterId, clusterPos, err
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
		//       2) provide a DropClusterById(dbc, clusterId)
		return begin, end,
			fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DATARANGE)
	if err != nil {
		return begin, end, err
	}

	err = WriteShort(dbc.buf, clusterId)
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

	begin, err = ReadLong(dbc.conx)
	if err != nil {
		return begin, end, err
	}

	end, err = ReadLong(dbc.conx)
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

	err = WriteString(dbc.buf, cname)
	if err != nil {
		return int16(0), err
	}

	err = WriteShort(dbc.buf, -1) // -1 means generate new cluster id
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

	clusterId, err = ReadShort(dbc.conx)
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

	err = WriteShort(dbc.buf, clusterId)
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

	delStatus, err := ReadByte(dbc.conx)
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
	err = WriteShort(dbc.buf, int16(len(clusterIds)))
	if err != nil {
		return int64(0), err
	}

	for _, cid := range clusterIds {
		err = WriteShort(dbc.buf, cid)
		if err != nil {
			return int64(0), err
		}
	}

	// count-tombstones
	var ct byte
	if countTombstones {
		ct = byte(1)
	}
	err = WriteByte(dbc.buf, ct) // presuming that 0 means "false"
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

	nrecs, err := ReadLong(dbc.conx)
	if err != nil {
		return int64(0), err
	}

	return nrecs, err
}

func writeCommandAndSessionId(dbc *DbClient, cmd byte) error {
	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	err := WriteByte(dbc.buf, cmd)
	if err != nil {
		return err
	}

	err = WriteInt(dbc.buf, dbc.sessionId)
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
	longFromDb, err := ReadLong(dbc.conx)
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
	status, err := ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	sessionId, err := ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	if sessionId != dbc.sessionId {
		return fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return err
		}
		return fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	return nil
}
