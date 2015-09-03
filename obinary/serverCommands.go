package obinary

import (
	"errors"
	"fmt"
	"io"

	"bytes"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oerror"
	"github.com/istreamdata/orientgo/oschema"
)

/// In the Java client the "server command" functionality is encapsulated
/// the OServerAdmin class.  TODO: may want to follow suit rather than
/// using the same DBClient for both server-commands and db-commands,
/// especially since (I think) they have separate logins.

// ConnectToServer logs into the OrientDB server with the appropriate
// admin privileges in order to execute server-level commands (as opposed
// to database-level commands). This must be called to establish a server
// session before any other server-level commands. The username and password
// required are for the server (admin) not any particular database.
func (dbc *Client) ConnectToServer(adminUser, adminPassw string) (err error) {
	defer catch(&err)
	buf := dbc.writeBuffer()

	// first byte specifies request type
	rw.WriteByte(buf, requestConnect)

	// session-id - send a negative number to create a new server-side conx
	rw.WriteInt(buf, requestNewSession)

	rw.WriteStrings(buf, driverName, driverVersion)

	rw.WriteShort(buf, dbc.binaryProtocolVersion)

	// dbclient id - send as null, but cannot be null if clustered config
	// TODO: change to use dbc.clusteredConfig once that is added
	rw.WriteNull(buf)

	// serialization-impl
	rw.WriteString(buf, dbc.serializationType)

	// token-session  // TODO: hardcoded as false for now -> change later based on ClientOptions settings
	rw.WriteBool(buf, false)

	// admin username, password
	rw.WriteStrings(buf, adminUser, adminPassw)

	// send to OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Server Response ]---

	// first byte indicates success/error
	status := rw.ReadByte(dbc.conx)

	// the first int returned is the session id sent - which was the `RequestNewSession` sentinel
	sessionValSent := rw.ReadInt(dbc.conx)

	if sessionValSent != requestNewSession {
		return errors.New("Unexpected Error: Server did not return expected session-request-val that was sent")
	}

	// if status returned was ERROR, then the rest of server data is the exception info
	if status != responseStatusOk {
		return rw.ReadErrorResponse(dbc.conx)
	}

	// for the REQUEST_CONNECT case, another int is returned which is the new sessionId
	sessionId := rw.ReadInt(dbc.conx)

	// TODO: this assumes you can only have one sessionId - but perhaps can have a server sessionid
	//       and one or more database sessions open at the same time ?????
	dbc.sessionId = sessionId

	tokenBytes := rw.ReadBytes(dbc.conx)

	dbc.token = tokenBytes
	return nil
}

// CreateDatabase will create a `remote` database of the type and storageType specified.
// dbType must be type DocumentDBType or GraphDBType.
// storageType must type PersistentStorageType or VolatileStorageType.
func (dbc *Client) CreateDatabase(dbname string, dbtype orient.DatabaseType, storageType orient.StorageType) (err error) {
	defer catch(&err)

	buf := dbc.writeBuffer()

	// TODO: may need to change this to serverSessionid (can the "sessionId" be used for both server connections and db conx?)
	if dbc.sessionId == noSessionId {
		return oerror.SessionNotInitialized{}
	}

	// cmd
	rw.WriteByte(buf, requestDbCreate)

	// session id
	rw.WriteInt(buf, dbc.sessionId)

	rw.WriteStrings(buf, dbname, string(dbtype), string(storageType))

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ read response from server ]---

	status := rw.ReadByte(dbc.conx)

	if err = readAndValidateSessionId(dbc.conx, dbc.sessionId); err != nil {
		return err
	}

	if status == responseStatusError {
		return rw.ReadErrorResponse(dbc.conx)
	}

	return nil
}

// DropDatabase drops the specified database. The caller must provide
// both the name and the type of the database.  The type should either:
//
//     obinary.DocumentDBType
//     obinary.GraphDBType
//
// This is a "server" command, so you must have already called
// ConnectToServer before calling this function.
func (dbc *Client) DropDatabase(dbname string, dbtype orient.StorageType) (err error) {
	defer catch(&err)

	buf := dbc.writeBuffer()

	if dbc.sessionId == noSessionId {
		return oerror.SessionNotInitialized{}
	}

	// cmd
	rw.WriteByte(buf, requestDbDrop)

	// session id
	rw.WriteInt(buf, dbc.sessionId)

	// database name, storage-type
	rw.WriteStrings(buf, dbname, string(dbtype))

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ read response from server ]---

	status := rw.ReadByte(dbc.conx)

	if err = readAndValidateSessionId(dbc.conx, dbc.sessionId); err != nil {
		return err
	}

	if status == responseStatusError {
		return rw.ReadErrorResponse(dbc.conx)
	}

	return nil
}

// DatabaseExists is a server-level command, so must be preceded by calling
// ConnectToServer, otherwise an authorization error will be returned.
// The storageType param must be either PersistentStorageType or VolatileStorageType.
func (dbc *Client) DatabaseExists(dbname string, storageType orient.StorageType) (val bool, err error) {
	defer catch(&err)

	buf := dbc.writeBuffer()

	if dbc.sessionId == noSessionId {
		return false, oerror.SessionNotInitialized{}
	}

	// cmd
	rw.WriteByte(buf, requestDbExists)

	// session id
	rw.WriteInt(buf, dbc.sessionId)

	// database name, storage-type
	rw.WriteStrings(buf, dbname, string(storageType))

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response From Server ]---

	status := rw.ReadByte(dbc.conx)

	if err = readAndValidateSessionId(dbc.conx, dbc.sessionId); err != nil {
		return false, err
	}

	if status == responseStatusError {
		err = rw.ReadErrorResponse(dbc.conx)
		return
	}

	// the answer to the query
	dbexists := rw.ReadBool(dbc.conx)

	return dbexists, nil
}

// RequestDBList works like the "list databases" command from the OrientDB client.
// The result is put into a map, where the key of the map is the name of the
// database and the value is the type concatenated with the path, like so:
//
//     key:  cars
//     val:  plocal:/path/to/orientdb-community-2.0.1/databases/cars
func (dbc *Client) ListDatabases() (list map[string]string, err error) {
	defer catch(&err)

	buf := dbc.writeBuffer()

	if dbc.sessionId == noSessionId {
		return nil, oerror.SessionNotInitialized{}
	}

	// cmd
	rw.WriteByte(buf, requestDbLIST)

	// session id
	rw.WriteInt(buf, dbc.sessionId)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	status := rw.ReadByte(dbc.conx)

	if err = readAndValidateSessionId(dbc.conx, dbc.sessionId); err != nil {
		return nil, err
	}

	if status == responseStatusError {
		err = rw.ReadErrorResponse(dbc.conx)
		return
	}

	// the bytes returned as a serialized EMBEDDEDMAP, so send it to the SerDe
	responseBytes := rw.ReadBytes(dbc.conx)

	serde := dbc.RecordSerDes[int(responseBytes[0])]
	doc := oschema.NewDocument("")
	err = serde.Deserialize(dbc, doc, bytes.NewReader(responseBytes[1:]))
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	fldMap := doc.GetField("databases").Value.(map[string]interface{})
	for k, v := range fldMap {
		m[k] = v.(string)
	}

	return m, nil
}

func readAndValidateSessionId(rdr io.Reader, currentSessionId int32) error {
	sessionId := rw.ReadInt(rdr)
	if sessionId != currentSessionId {
		return fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, currentSessionId)
	}
	return nil
}
