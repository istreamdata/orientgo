package obinary // import "gopkg.in/istreamdata/orientgo.v1/obinary"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"gopkg.in/istreamdata/orientgo.v1/oerror"
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
	currDB                *ODatabase // only one db session open at a time
	RecordSerDes          []ORecordSerializer
}

/* ---[ getters for testing ]--- */
func (dbc *DBClient) GetCurrDB() *ODatabase {
	return dbc.currDB
}

func (dbc *DBClient) GetSessionID() int32 {
	return dbc.sessionId
}

//
// NewDBClient creates a new DBClient after contacting the OrientDB server
// specified in the ClientOptions and validating that the server and client
// speak the same binary protocol version.
// The DBClient returned is ready to make calls to the OrientDB but has not
// yet established a database session or a session with the OrientDB server.
// After this, the user needs to call either OpenDatabase or CreateServerSession.
//
func NewDBClient(opts ClientOptions) (*DBClient, error) {
	// binary port range is: 2424-2430
	if opts.ServerHost == "" {
		opts.ServerHost = "127.0.0.1"
	}
	if opts.ServerPort == "" {
		opts.ServerPort = "2424"
	}
	hostport := fmt.Sprintf("%s:%s", opts.ServerHost, opts.ServerPort)
	conx, err := net.Dial("tcp", hostport)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return nil, oerror.NewTrace(err)
	}

	// after connecting the OrientDB server sends back 2 bytes - its binary protocol version
	readbuf := make([]byte, 2)
	n, err := conx.Read(readbuf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	buf := new(bytes.Buffer)
	_, err = buf.Write(readbuf[0:n])
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	var (
		svrProtocolNum int16
		serdeV0        ORecordSerializer
		serializerType string
	)
	binary.Read(buf, binary.BigEndian, &svrProtocolNum)
	if svrProtocolNum < MinSupportedBinaryProtocolVersion {
		return nil, ErrUnsupportedVersion{serverVersion: svrProtocolNum}
	} else if svrProtocolNum > MaxSupportedBinaryProtocolVersion {
		return nil, ErrUnsupportedVersion{serverVersion: svrProtocolNum}
	}

	serializerType = BinarySerialization
	serdeV0 = &ORecordSerializerV0{}
	if svrProtocolNum < MinBinarySerializerVersion {
		serializerType = CsvSerialization
		panic(fmt.Sprintf("Server Binary Protocol Version (%v) is less than the Min Binary Serializer Version supported by this driver (%v)",
			svrProtocolNum, MinBinarySerializerVersion))
	}

	dbc := &DBClient{
		conx:                  conx,
		buf:                   new(bytes.Buffer),
		serializationType:     serializerType,
		binaryProtocolVersion: svrProtocolNum,
		serializationVersion:  byte(0), // default is 0 // TODO: need to detect if server is using a higher version
		sessionId:             NoSessionID,
		RecordSerDes:          []ORecordSerializer{serdeV0},
	}

	return dbc, nil
}

func (dbc *DBClient) Close() error {
	if dbc.currDB != nil {
		// ignoring any error here, since closing the conx also terminates the session
		CloseDatabase(dbc)
	}
	return dbc.conx.Close()
}

func (dbc *DBClient) String() string {
	if dbc.currDB == nil {
		return "DBClient<not-connected-to-db>"
	}
	return fmt.Sprintf("DBClient<connected-to: %v of type %v with %d clusters; sessionId: %v\n  CurrDB Details: %v>",
		dbc.currDB.Name, dbc.currDB.Typ, len(dbc.currDB.Clusters), dbc.sessionId, dbc.currDB)
}
