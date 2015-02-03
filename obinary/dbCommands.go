package obinary

import (
	"errors"
	"fmt"
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

	// close cmd
	err := WriteByte(dbc.buf, REQUEST_DB_CLOSE)
	if err != nil {
		return err
	}

	// session id
	err = WriteInt(dbc.buf, dbc.sessionId)
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

	status, err := ReadByte(dbc.conx)
	if err != nil {
		return int16(0), err
	}

	sessionId, err := ReadInt(dbc.conx)
	if err != nil {
		return int16(0), err
	}
	if sessionId != dbc.sessionId {
		return int16(0), fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return int16(-1), err
		}
		return int16(-1), fmt.Errorf("Server Error(s): %v", serverExceptions)
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

func writeCommandAndSessionId(dbc *DbClient, cmd byte) error {
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

	if dbc.sessionId == NoSessionId {
		return int64(-1), SessionNotInitialized{}
	}

	// cmd
	err := WriteByte(dbc.buf, cmd)
	if err != nil {
		return int64(-1), err
	}

	// session id
	err = WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return int64(-1), err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(-1), err
	}

	/* ---[ Read Response ]--- */

	status, err := ReadByte(dbc.conx)
	if err != nil {
		return int64(-1), err
	}

	sessionId, err := ReadInt(dbc.conx)
	if err != nil {
		return int64(-1), err
	}
	if sessionId != dbc.sessionId {
		return int64(-1), fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return int64(-1), err
		}
		return int64(-1), fmt.Errorf("Server Error(s): %v", serverExceptions)
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
