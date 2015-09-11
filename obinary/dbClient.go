package obinary

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
	"time"
)

func init() {
	orient.RegisterProto(orient.ProtoBinary, func(addr string) (orient.DBConnection, error) {
		return Dial(addr)
	})
}

func validateAddr(addr string) (string, error) {
	var host, port string
	if addr != "" {
		var err error
		host, port, err = net.SplitHostPort(addr)
		if err != nil {
			return "", err
		}
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "2424" // binary port range is: 2424-2430
	}
	return net.JoinHostPort(host, port), nil
}

// Dial creates a new binary connection to OrientDB server.
// The Client returned is ready to make calls to the OrientDB but has not
// yet established a database session or a session with the OrientDB server.
// After this, the user needs to call either OpenDatabase or CreateServerSession.
func Dial(addr string) (*Client, error) {
	addr, err := validateAddr(addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", addr, time.Minute)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr: addr, conn: conn,
		serializer: serializer(curSerializerVersion), // TODO: need to detect if server is using a higher version
	}
	if err := c.handshakeVersion(); err != nil {
		conn.Close()
		return nil, err
	}
	c.sess = make(map[int32]*session)
	c.root = c.newSess(noSessionId)
	go c.run()
	return c, nil
}

// Client encapsulates the active TCP connection to an OrientDB server
// to be used with the Network Binary Protocol.
// It also may be connected to up to one database at a time.
// Do not create a Client struct directly.  You should use NewClient,
// followed immediately by ConnectToServer, to connect to the OrientDB server,
// or OpenDatabase, to connect to a database on the server.
type Client struct {
	addr string

	conn net.Conn
	cmuw sync.Mutex

	root *session

	sessmu sync.RWMutex
	sess   map[int32]*session

	currmu sync.RWMutex
	currdb *Database // only one db session open at a time

	protoVers  int16
	serializer ORecordSerializer
}

func (c *Client) handshakeVersion() (err error) {
	defer catch(&err)

	c.conn.SetReadDeadline(time.Now().Add(time.Second * 5))
	defer c.conn.SetReadDeadline(time.Time{})

	c.protoVers = rw.ReadShort(c.conn)
	if c.protoVers < MinProtocolVersion || c.protoVers > MaxProtocolVersion {
		return ErrUnsupportedVersion(c.protoVers)
	} else if c.protoVers < minBinarySerializerVersion { // may switch to CSV serialization, but we don't care for now
		return ErrUnsupportedVersion(c.protoVers)
	}
	return nil
}

func (c *Client) write(data io.Reader) {
	c.cmuw.Lock()
	defer c.cmuw.Unlock()
	rw.Copy(c.conn, data)
}

func (c *Client) newSess(id int32) *session {
	c.sessmu.Lock()
	s := c.sess[id]
	if s == nil {
		s = &session{id: id, cli: c, in: make(chan resp)}
		c.sess[id] = s
	}
	c.sessmu.Unlock()
	return s
}

func (c *Client) closeSess(id int32, ref *Database) {
	c.sessmu.Lock()
	delete(c.sess, id)
	c.sessmu.Unlock()
	c.currmu.Lock()
	if c.currdb == ref {
		c.currdb = nil
	}
	c.currmu.Unlock()
}

func newReadChanCloser(r io.Reader, ch chan struct{}) *readChanCloser {
	return &readChanCloser{
		r: r, done: ch,
	}
}

type readChanCloser struct {
	r    io.Reader
	done chan struct{}
}

func (r *readChanCloser) Read(p []byte) (int, error) {
	select {
	case <-r.done:
		return 0, ErrClosedConnection
	default:
		return r.r.Read(p)
	}
}
func (r *readChanCloser) Close() error {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
	return nil
}

func (c *Client) pushResp(id int32, r io.Reader, e error) {
	var to <-chan time.Time
	c.sessmu.Lock()
	s := c.sess[id]
	c.sessmu.Unlock()
	if s == nil {
		to = time.After(time.Second)
		s = c.newSess(id)
	}
	if r == nil { // no reader, error returned
		select {
		case <-to:
		case s.in <- resp{err: e}:
		}
		return
	}
	done := make(chan struct{})
	select {
	case <-to: // connection expects that response will be read, so stream is broken
		panic(ErrBrokenProtocol{fmt.Errorf("no session %d found", id)})
	case s.in <- resp{ReadCloser: newReadChanCloser(r, done)}:
		<-done
	}
}

// ReadErrorResponse reads an "Exception" message from the OrientDB server.
// The OrientDB server can return multiple exceptions, all of which are
// incorporated into a single OServerException Error struct.
// If error (the second return arg) is not nil, then there was a
// problem reading the server exception on the wire.
func readErrorResponse(r io.Reader) (serverException error) {
	var (
		exClass, exMsg string
	)
	exc := make([]orient.Exception, 0, 1) // usually only one ?
	for {
		// before class/message combo there is a 1 (continue) or 0 (no more)
		marker := rw.ReadByte(r)
		if marker == byte(0) {
			break
		}
		exClass = rw.ReadString(r)
		exMsg = rw.ReadString(r)
		exc = append(exc, orient.UnknownException{Class: exClass, Message: exMsg})
	}

	// Next there *may* a serialized exception of bytes, but it is only
	// useful to Java clients, so read and ignore if present.
	// If there is no serialized exception, EOF will be returned
	_ = rw.ReadBytes(r) // TODO: catch EOFs?

	for _, e := range exc {
		switch e.ExcClass() {
		case "com.orientechnologies.orient.core.storage.ORecordDuplicatedException":
			return ODuplicatedRecordException{OServerException: orient.OServerException{Exceptions: exc}}
		}
	}
	return orient.OServerException{Exceptions: exc}
}

func (c *Client) run() (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch rr := r.(type) {
			case ErrBrokenProtocol:
				panic(r)
			case error:
				err = rr
			default:
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	var (
		status byte
		sessId int32
	)
	for { // TODO: close safely
		status = rw.ReadByte(c.conn)
		sessId = rw.ReadInt(c.conn)
		switch status {
		case responseStatusOk:
			c.pushResp(sessId, c.conn, nil)
		case responseStatusError:
			e := readErrorResponse(c.conn)
			c.pushResp(sessId, nil, e)
		case responseStatusPush:
			return ErrBrokenProtocol{fmt.Errorf("server push is not supported yet")}
		default:
			return ErrBrokenProtocol{fmt.Errorf("unknown resp status: %d", status)}
		}
	}
	//return
}

type resp struct {
	io.ReadCloser
	err error
}

type session struct {
	mu  sync.Mutex
	id  int32
	in  chan resp
	cli *Client
}

func (s *session) catch(err *error) {
	if r := recover(); r != nil {
		switch rr := r.(type) {
		case error:
			*err = rr
		default:
			*err = fmt.Errorf("%v", r)
		}
		go s.cli.Close() // panic means that stream is likely to be broken
	}
}

func (s *session) sendCmd(op byte, wr func(io.Writer), rd func(io.Reader)) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.catch(&err)
	buf := bytes.NewBuffer(nil)
	rw.WriteByte(buf, op)
	rw.WriteInt(buf, s.id)
	if wr != nil {
		wr(buf)
	}
	s.cli.write(buf)
	if op == requestDbClose {
		return
	}
	resp, ok := <-s.in
	if !ok {
		return ErrClosedConnection
	} else if resp.err != nil {
		return resp.err
	}
	defer resp.Close()
	if rd != nil {
		rd(resp)
	}
	return nil
}

func (dbc *Client) getCurrDB() *Database {
	dbc.currmu.RLock()
	defer dbc.currmu.RUnlock()
	return dbc.currdb
}

func (db *Database) GetCurDB() *orient.ODatabase {
	if db == nil || db.db == nil {
		return nil
	}
	return &orient.ODatabase{
		Name:    db.db.Name,
		Type:    db.db.Type,
		Classes: db.db.Classes,
	}
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if db := c.getCurrDB(); db != nil {
		// ignoring any error here, since closing the conx also terminates the session
		db.Close()
	}
	return c.conn.Close()
}

//func (dbc *Client) String() string {
//	if db := dbc.getCurrDB(); db == nil {
//		return "DBClient<not-connected-to-db>"
//	} else {
//		return fmt.Sprintf("DBClient<connected-to: %v of type %v with %d clusters; sessionId: %v\n  CurrDB Details: %v>",
//			db.Name, db.Type, len(db.Clusters), dbc.sessionId, db)
//	}
//}

func (db *Database) readSingleRecord(r io.Reader) orient.Record {
	resultType := rw.ReadShort(r)
	switch resultType {
	case RecordNull:
		return nil
	case RecordRID:
		rid := readRID(r)
		return RIDRecord{RID: rid, db: db}
	case 0:
		return db.readRecord(r)
	default:
		panic(fmt.Errorf("unexpected result type: %v", resultType))
	}
}

func (db *Database) readRecord(r io.Reader) orient.Record {
	// if get here then have a full record, which can be in one of three formats:
	//  - "flat data"
	//  - "raw bytes"
	//  - "document"
	recType := rw.ReadByte(r)
	switch tp := rune(recType); tp {
	case 'd':
		return db.readSingleDocument(r)
	case 'f':
		return db.readFlatDataRecord(r)
	case 'b':
		return db.readRawBytesRecord(r)
	default:
		panic(fmt.Errorf("unexpected record type: '%v'", tp))
	}
}

func (db *Database) readSingleDocument(r io.Reader) (doc *RecordData) {
	rid := readRID(r)
	recVersion := rw.ReadInt(r)
	recBytes := rw.ReadBytes(r)
	return &RecordData{RID: rid, Version: recVersion, Data: recBytes, db: db}
}

func (db *Database) readFlatDataRecord(r io.Reader) orient.Record {
	panic(fmt.Errorf("readFlatDataRecord: Non implemented")) // TODO: need example from server to know how to handle this
}

func (db *Database) readRawBytesRecord(r io.Reader) orient.Record {
	return RawRecord(rw.ReadBytes(r))
}

func (db *Database) readResultSet(r io.Reader) orient.Records {
	// next val is: (collection-size:int)
	// and then each record is serialized according to format:
	// (0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)
	resultSetSize := int(rw.ReadInt(r))
	recs := make(orient.Records, 0, resultSetSize)
	for i := 0; i < resultSetSize; i++ {
		if rec := db.readSingleRecord(r); rec != nil {
			recs = append(recs, rec)
		}
	}
	return recs
}

func readRID(r io.Reader) oschema.RID {
	// svr response: (-3:short)(cluster-id:short)(cluster-position:long)
	clusterID := rw.ReadShort(r)
	clusterPos := rw.ReadLong(r)
	return oschema.RID{ClusterID: clusterID, ClusterPos: clusterPos}
}

func extractSerializerVersion(data []byte) (ser ORecordSerializer, out []byte) {
	if len(data) == 0 {
		panic(io.ErrUnexpectedEOF)
	}
	vers := data[0]
	ser = serializer(vers)
	out = data[1:] // need this for pointer arithmetic to work in serialized document
	return
}

func (db *Database) createDocumentFromBytes(rid oschema.RID, recVersion int32, serializedDoc []byte) (doc *oschema.ODocument, err error) {
	defer catch(&err)
	if len(serializedDoc) == 0 {
		err = io.ErrUnexpectedEOF
		return
	}
	doc = oschema.NewDocument("") // don't know classname yet (in serialized record)
	doc.RID = rid
	doc.Version = recVersion
	ser, data := extractSerializerVersion(serializedDoc)
	err = ser.Deserialize(db, doc, bytes.NewReader(data))
	return
}

func (db *Database) createMapFromBytes(rid oschema.RID, serializedDoc []byte) (m map[string]interface{}, err error) {
	defer catch(&err)
	if len(serializedDoc) == 0 {
		err = io.ErrUnexpectedEOF
		return
	}
	ser, data := extractSerializerVersion(serializedDoc) // TODO: serialize to document first?
	m, err = ser.ToMap(db, bytes.NewReader(data))
	return
}
