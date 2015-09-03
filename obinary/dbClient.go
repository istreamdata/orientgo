package obinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

func init() {
	orient.RegisterProto(orient.ProtoBinary, func(addr string) (orient.DBConnection, error) {
		return NewClient(addr)
	})
}

// Client encapsulates the active TCP connection to an OrientDB server
// to be used with the Network Binary Protocol.
// It also may be connected to up to one database at a time.
// Do not create a Client struct directly.  You should use NewClient,
// followed immediately by ConnectToServer, to connect to the OrientDB server,
// or OpenDatabase, to connect to a database on the server.
type Client struct {
	opts ClientOptions
	conx net.Conn
	//buf                   *bytes.Buffer
	sessionId             int32
	token                 []byte     // orientdb token when not using sessionId
	currDb                *ODatabase // only one db session open at a time
	serializationType     string
	binaryProtocolVersion int16
	serializationVersion  byte
	mutex                 sync.Mutex
	RecordSerDes          []ORecordSerializer
}

func (dbc *Client) GetCurrDB() *ODatabase {
	return dbc.currDb
}

func (dbc *Client) GetCurDB() *orient.ODatabase {
	if dbc == nil || dbc.currDb == nil {
		return nil
	}
	return &orient.ODatabase{
		Name:    dbc.currDb.Name,
		Type:    dbc.currDb.Type,
		Classes: dbc.currDb.Classes,
	}
}

func (dbc *Client) GetClasses() map[string]*oschema.OClass {
	return dbc.GetCurrDB().Classes
}

func (dbc *Client) GetSessionId() int32 {
	return dbc.sessionId
}

// NewDBClient creates a new DBClient after contacting the OrientDB server
// specified in the ClientOptions and validating that the server and client
// speak the same binary protocol version.
// The DBClient returned is ready to make calls to the OrientDB but has not
// yet established a database session or a session with the OrientDB server.
// After this, the user needs to call either OpenDatabase or CreateServerSession.
func NewClient(addr string) (*Client, error) {
	opts := ClientOptions{Addr: addr}
	var (
		host, port string
	)
	if addr != "" {
		var err error
		host, port, err = net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
	}
	if host == "" {
		host = "localhost"
	}
	// binary port range is: 2424-2430
	if port == "" {
		port = "2424"
	}
	addr = net.JoinHostPort(host, port)
	conx, err := net.Dial("tcp", addr)
	if err != nil {
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
		serde          ORecordSerializer
		serializerType string
	)
	binary.Read(buf, binary.BigEndian, &svrProtocolNum)
	if svrProtocolNum < MinSupportedBinaryProtocolVersion || svrProtocolNum > MaxSupportedBinaryProtocolVersion {
		return nil, ErrUnsupportedVersion{serverVersion: svrProtocolNum}
	}

	serializerType = serializeTypeBinary
	serde = &ORecordSerializerV0{}
	if svrProtocolNum < minBinarySerializerVersion {
		serializerType = serializeTypeCsv
		return nil, fmt.Errorf("server binary protocol version `%d` is outside client supported version range: >%d",
			svrProtocolNum, minBinarySerializerVersion)
	}

	dbc := &Client{
		opts: opts,
		conx: conx,
		//buf:                   new(bytes.Buffer),
		serializationType:     serializerType,
		binaryProtocolVersion: svrProtocolNum,
		serializationVersion:  byte(0), // default is 0 // TODO: need to detect if server is using a higher version
		sessionId:             noSessionId,
		RecordSerDes:          []ORecordSerializer{serde},
	}

	return dbc, nil
}

func (dbc *Client) Close() error {
	if dbc == nil {
		return nil
	}
	if dbc.currDb != nil {
		// ignoring any error here, since closing the conx also terminates the session
		dbc.CloseDatabase()
	}
	return dbc.conx.Close()
}

func (dbc *Client) String() string {
	if dbc.currDb == nil {
		return "DBClient<not-connected-to-db>"
	}
	return fmt.Sprintf("DBClient<connected-to: %v of type %v with %d clusters; sessionId: %v\n  CurrDB Details: %v>",
		dbc.currDb.Name, dbc.currDb.Type, len(dbc.currDb.Clusters), dbc.sessionId, dbc.currDb)
}

func (dbc *Client) Size() (int64, error) {
	return dbc.getDbSize()
}

func (dbc *Client) prepareBuffer(cmd byte) *bytes.Buffer {
	buf := dbc.writeCommandAndSessionId(cmd)
	mode := byte('s') // synchronous only supported for now
	rw.WriteByte(buf, mode)
	return buf
}

func (dbc *Client) readSingleRecord(r io.Reader) orient.Record {
	resultType := rw.ReadShort(r)
	switch resultType {
	case RecordNull:
		return NullRecord{}
	case RecordRID:
		rid := readRID(r)
		return RIDRecord{RID: rid, dbc: dbc}
	case 0:
		return dbc.readRecord(r)
	default:
		panic(fmt.Errorf("unexpected result type: %v", resultType))
	}
}

func (dbc *Client) readRecord(r io.Reader) orient.Record {
	// if get here then have a full record, which can be in one of three formats:
	//  - "flat data"
	//  - "raw bytes"
	//  - "document"
	recType := rw.ReadByte(r)
	switch tp := rune(recType); tp {
	case 'd':
		return dbc.readSingleDocument(r)
	case 'f':
		return dbc.readFlatDataRecord(r)
	case 'b':
		return dbc.readRawBytesRecord(r)
	default:
		panic(fmt.Errorf("unexpected record type: '%v'", tp))
	}
}

func (dbc *Client) readSingleDocument(r io.Reader) (doc *RecordData) {
	rid := readRID(r)
	recVersion := rw.ReadInt(r)
	recBytes := rw.ReadBytes(r)
	return &RecordData{RID: rid, Version: recVersion, Data: recBytes, dbc: dbc}
}

func (dbc *Client) readFlatDataRecord(r io.Reader) orient.Record {
	panic(fmt.Errorf("readFlatDataRecord: Non implemented")) // TODO: need example from server to know how to handle this
}

func (dbc *Client) readRawBytesRecord(r io.Reader) orient.Record {
	panic(fmt.Errorf("readRawBytesRecord: Non implemented")) // TODO: need example from server to know how to handle this
}

func (dbc *Client) readResultSet(r io.Reader) orient.Records {
	// next val is: (collection-size:int)
	// and then each record is serialized according to format:
	// (0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)
	resultSetSize := int(rw.ReadInt(r))
	docs := make(orient.Records, resultSetSize)
	for i := range docs {
		docs[i] = dbc.readSingleRecord(r)
	}
	return docs
}

func readRID(r io.Reader) oschema.ORID {
	// svr response: (-3:short)(cluster-id:short)(cluster-position:long)
	clusterID := rw.ReadShort(r)
	clusterPos := rw.ReadLong(r)
	return oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}
}

func (dbc *Client) createDocumentFromBytes(rid oschema.ORID, recVersion int32, serializedDoc []byte) (*oschema.ODocument, error) {
	var doc *oschema.ODocument
	doc = oschema.NewDocument("") // don't know classname yet (in serialized record)
	doc.RID = rid
	doc.Version = recVersion
	if len(serializedDoc) > 0 {
		// the first byte specifies record serialization version
		// use it to look up serializer and strip off that byte
		serde := dbc.currDb.RecordSerDes[int(serializedDoc[0])]
		err := serde.Deserialize(dbc, doc, bytes.NewReader(serializedDoc[1:]))
		if err != nil {
			return nil, fmt.Errorf("ERROR in Deserialize for rid %v: %v\n", rid, err)
		}
	}
	return doc, nil
}

func (dbc *Client) createMapFromBytes(rid oschema.ORID, serializedDoc []byte) (map[string]interface{}, error) {
	// the first byte specifies record serialization version
	// use it to look up serializer and strip off that byte
	serde := dbc.currDb.RecordSerDes[int(serializedDoc[0])]
	m, err := serde.ToMap(dbc, bytes.NewReader(serializedDoc[1:]))
	if err != nil {
		return nil, fmt.Errorf("ERROR in converting to map for rid %v: %v\n", rid, err)
	}
	return m, nil
}
