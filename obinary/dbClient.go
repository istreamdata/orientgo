package obinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

//
// DBClient encapsulates the active TCP connection to an OrientDB server
// to be used with the Network Binary Protocol.
// It also may be connected to up to one database at a time.
// Do not create a DBClient struct directly.  You should use NewDBClient,
// followed immediately by ConnectToServer, to connect to the OrientDB server,
// or OpenDatabase, to connect to a database on the server.
//
type DBClient struct {
	conx                  net.Conn
	buf                   *bytes.Buffer
	sessionId             int32
	token                 []byte // orientdb token when not using sessionId
	serializationType     string
	binaryProtocolVersion int16
	serializationVersion  byte
	currDb                *ODatabase          // only one db session open at a time
	RecordSerDes          []ORecordSerializer // serdes w/o globalProps - for server-level cmds
	//
	// There are two separate arrays of ORecordSerializers - the one here does NOT
	// have its GlobalProperties field set, which means it cannot be used for some
	// database-level queries where it needs to reference schema info.  But some
	// server-level commands (e.g., RequestDbList) need to used a Deserializer.
	// This list here is to be used for server-level commands.  For database-level
	// commands use the RecordSerDes in the currDb object.
	//
}

/* ---[ getters for testing ]--- */
func (dbc *DBClient) GetCurrDB() *ODatabase {
	return dbc.currDb
}

func (dbc *DBClient) GetSessionId() int32 {
	return dbc.sessionId
}

//
// NewDBClient creates a new DBClient after contacting the OrientDb server
// specified in the ClientOptions and validating that the server and client
// speak the same binary protocol version.
// The DBClient returned is ready to make calls to the OrientDb but has not
// yet established a database session or a session with the OrientDb server.
// After this, the user needs to call either OpenDatabase or CreateServerSession.
//
func NewDBClient(opts ClientOptions) (*DBClient, error) {
	// binary port range is: 2424-2430
	if opts.ServerHost == "" {
		opts.ServerHost = "0.0.0.0"
	}
	if opts.ServerPort == "" {
		opts.ServerPort = "2424"
	}
	hostport := fmt.Sprintf("%s:%s", opts.ServerHost, opts.ServerPort)
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

	var (
		svrProtocolNum int16
		serdeV0        ORecordSerializer
		serializerType string
	)
	binary.Read(buf, binary.BigEndian, &svrProtocolNum)
	if svrProtocolNum < MinSupportedBinaryProtocolVersion {
		return nil, UnsupportedVersionError{serverVersion: svrProtocolNum}
	} else if svrProtocolNum > MaxSupportedBinaryProtocolVersion {
		return nil, UnsupportedVersionError{serverVersion: svrProtocolNum}
	}

	serializerType = BinarySerialization
	serdeV0 = &ORecordSerializerV0{}
	if svrProtocolNum < MinBinarySerializerVersion {
		serializerType = CsvSerialization
		// TODO: change serializer to ORecordSerializerCsvVxxx once that is built
		panic(fmt.Sprintf("Server Binary Protocol Version (%v) is less than the Min Binary Serializer Version supported by this driver (%v)",
			svrProtocolNum, MinBinarySerializerVersion))
	}

	dbc := &DBClient{
		conx:                  conx,
		buf:                   new(bytes.Buffer),
		serializationType:     serializerType,
		binaryProtocolVersion: svrProtocolNum,
		serializationVersion:  byte(0), // default is 0 // TODO: need to detect if server is using a higher version
		sessionId:             NoSessionId,
		RecordSerDes:          []ORecordSerializer{serdeV0},
	}

	return dbc, nil
}

func (dbc *DBClient) Close() error {
	if dbc.currDb != nil {
		// ignoring any error here, since closing the conx also terminates the session
		CloseDatabase(dbc)
	}
	return dbc.conx.Close()
}

func (dbc *DBClient) String() string {
	if dbc.currDb == nil {
		return "DBClient[not-connected-to-db]"
	}
	return fmt.Sprintf("DBClient[connected-to: %v of type %v with %d clusters; sessionId: %v\n  CurrDb Details: %v]",
		dbc.currDb.Name, dbc.currDb.Typ, len(dbc.currDb.Clusters), dbc.sessionId, dbc.currDb)
}
