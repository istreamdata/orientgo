package obinary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
)

// internal client constants
const (
	SUCCESS                           = 0
	ERROR                             = 1
	NoSessionId                       = -1
	MaxSupportedBinaryProtocolVersion = 28 // max protocol supported by this client
	MinSupportedBinaryProtocolVersion = 21 // min protocol supported by this client
	MinBinarySerializerVersion        = 22 // if server protocol version is less, use csv ser, not binary ser
	RequestNewSession                 = -4 // arbitrary negative number sent to start session
	DriverName                        = "ogo: OrientDB Go client"
	DriverVersion                     = "1.0"
	BinarySerialization               = "ORecordSerializerBinary" // name of binary serialization to pass to server
	CsvSerialization                  = "ORecordDocument2csv"     // name of csv serialization to pass to server
)

// end user constants
const (
	DocumentDbType = "document" // use in OpenDatabase() call
	GraphDbType    = "graph"    // use in OpenDatabase() call

	PersistentStorageType = "plocal" // use in DatabaseExists() call
	VolatileStorageType   = "memory" // use in DatabaseExists() call
)

// TODO: pattern this after OStorageRemote ?
type DbClient struct {
	conx                  net.Conn
	buf                   *bytes.Buffer
	sessionId             int
	token                 []byte // orientdb token when not using sessionId
	serializationImpl     string
	binaryProtocolVersion int16
	currDb                *ODatabase
}

func (dbc DbClient) String() string {
	if dbc.currDb == nil {
		return "DbClient[not-connected-to-db]"
	}
	return fmt.Sprintf("DbClient[connected-to: %v of type %v with %d clusters; sessionId: %v\n  CurrDb Details: %v]",
		dbc.currDb.Name, dbc.currDb.Typ, len(dbc.currDb.Clusters), dbc.sessionId, dbc.currDb)
}

type ODatabase struct {
	Name     string
	Typ      string // DocumentDbType or GraphDbType
	Clusters []OCluster
	ClustCfg []byte
}

type OCluster struct {
	Name string
	Id   int16 // TODO: maybe change to int?
}

//
// ClientOptions should be created by the end user to configure details and
// options needed when opening a database or connecting to the OrientDB server
//
type ClientOptions struct {
	ServerHost      string
	ServerPort      string
	ClusteredConfig string // TODO: needs research - what goes here?; currently not used
}

// *DbClient implements Closer
func (c *DbClient) Close() error {
	return c.conx.Close()
}

func ConnectToServer(opts ClientOptions) (*DbClient, error) {
	// binary port range is: 2424-2430
	if opts.ServerHost == "" {
		opts.ServerHost = "0.0.0.0"
	}
	if opts.ServerPort == "" {
		opts.ServerPort = "2424"
	}
	hostport := fmt.Sprintf("%s:%s", opts.ServerHost, opts.ServerPort)
	fmt.Printf("%v\n", hostport) // DEBUG
	conx, err := net.Dial("tcp", hostport)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return nil, err
	}

	// after connecting the OrientDB server sends back 2 bytes - its binary protocol version
	readbuf := make([]byte, 2)
	n, err := conx.Read(readbuf)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	_, err = buf.Write(readbuf[0:n])
	if err != nil {
		return nil, err
	}

	var svrProtocolNum int16
	binary.Read(buf, binary.BigEndian, &svrProtocolNum)
	if svrProtocolNum < MinSupportedBinaryProtocolVersion {
		return nil, UnsupportedVersionError{serverVersion: svrProtocolNum}
	} else if svrProtocolNum > MaxSupportedBinaryProtocolVersion {
		return nil, UnsupportedVersionError{serverVersion: svrProtocolNum}
	}

	serializerImpl := BinarySerialization
	if svrProtocolNum < MinBinarySerializerVersion {
		serializerImpl = CsvSerialization
	}

	// DEBUG
	fmt.Printf("svrProtocolNum: %v\n", svrProtocolNum)
	// END DEBUG

	dbc := &DbClient{
		conx:                  conx,
		buf:                   new(bytes.Buffer),
		serializationImpl:     serializerImpl,
		binaryProtocolVersion: svrProtocolNum,
		sessionId:             NoSessionId,
	}
	return dbc, nil
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

	// mark this session as gone
	dbc.sessionId = NoSessionId

	return nil
}

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

	// first byte returned is status code : SUCCESS/ERROR
	if status != SUCCESS {
		// TODO: would now read error details from the response and put those details in the error obj
		return errors.New("Request failed (ERROR returned from server): details XXXXX")
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
// GetDatabaseSize retrives the size of the current database in bytes.
// OpenDatabase must have been called first in order to start a session
// with the database.
//
func GetDatabaseSize(dbc *DbClient) (int64, error) {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return int64(-1), SessionNotInitialized{}
	}

	// cmd
	err := WriteByte(dbc.buf, REQUEST_DB_SIZE)
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

	// the answer to the query
	dbSize, err := ReadLong(dbc.conx)
	if err != nil {
		return int64(-1), err
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return int64(-1), err
		}
		return int64(-1), fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	return dbSize, nil
}

//
// DatabaseExists is a Server command, so must be preceded by calling InitServerSession
// otherise an authorization error will be returned.
// storageType param must be one of PersistentStorageType or VolatileStorageType.
//
func DatabaseExists(dbc *DbClient, dbname, storageType string) (bool, error) {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return false, SessionNotInitialized{}
	}

	if storageType != PersistentStorageType && storageType != VolatileStorageType {
		return false, errors.New("Storage Type is not valid: " + storageType)
	}

	// cmd
	err := WriteByte(dbc.buf, REQUEST_DB_EXIST)
	if err != nil {
		return false, err
	}

	// session id
	err = WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return false, err
	}

	// database name, storage-type
	err = WriteStrings(dbc.buf, dbname, storageType)
	if err != nil {
		return false, err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return false, err
	}

	/* ---[ Read Response From Server ]--- */

	status, err := ReadByte(dbc.conx)
	if err != nil {
		return false, err
	}

	sessionId, err := ReadInt(dbc.conx)
	if err != nil {
		return false, err
	}
	if sessionId != dbc.sessionId {
		return false, fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	// the answer to the query
	dbexists, err := ReadBool(dbc.conx)
	if err != nil {
		return false, err
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return false, err
		}
		return dbexists, fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	return dbexists, nil
}
