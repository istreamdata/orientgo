package obinary

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/oschema"
)

///
/// In the Java client the "server command" functionality is encapsulated
/// the OServerAdmin class.  TODO: may want to follow suit rather than
/// using the same DBClient for both server-commands and db-commands,
/// especially since (I think) they have separate logins.
///

//
// ConnectToServer logs into the OrientDB server with the appropriate
// admin privileges in order to execute server-level commands (as opposed
// to database-level commands). This must be called to establish a server
// session before any other server-level commands. The username and password
// required are for the server (admin) not any particular database.
//
func ConnectToServer(dbc *DBClient, adminUser, adminPassw string) error {
	buf := dbc.buf
	buf.Reset()

	// first byte specifies request type
	err := rw.WriteByte(buf, REQUEST_CONNECT)
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

	// admin username, password
	err = rw.WriteStrings(buf, adminUser, adminPassw)
	if err != nil {
		return err
	}

	// send to OrientDB server
	_, err = dbc.conx.Write(buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ Read Server Response ]--- */

	// first byte indicates success/error
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

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

	// for the REQUEST_CONNECT case, another int is returned which is the new sessionId
	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	// TODO: this assumes you can only have one sessionId - but perhaps can have a server sessionid
	//       and one or more database sessions open at the same time ?????
	dbc.sessionId = sessionId

	tokenBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	dbc.token = tokenBytes
	return nil
}

//
// CreateDatabase will create a `remote` database of the type and storageType specified.
// dbType must be type DocumentDbType or GraphDbType.
// storageType must type PersistentStorageType or VolatileStorageType.
//
func CreateDatabase(dbc *DBClient, dbname string, dbtype constants.DatabaseType, storageType constants.StorageType) error {
	dbc.buf.Reset()

	/* ---[ precondition checks ]--- */

	// TODO: may need to change this to serverSessionid (can the "sessionId" be used for both server connections and db conx?)
	if dbc.sessionId == NoSessionId {
		return oerror.SessionNotInitialized{}
	}

	/* ---[ build request and send to server ]--- */

	// cmd
	err := rw.WriteByte(dbc.buf, REQUEST_DB_CREATE)
	if err != nil {
		return err
	}

	// session id
	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return err
	}

	err = rw.WriteStrings(dbc.buf, dbname, string(dbtype), string(storageType))
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ read response from server ]--- */

	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	err = readAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return err
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

//
// DropDatabase drops the specified database. The caller must provide
// both the name and the type of the database.  The type should either:
//
//     obinary.DocumentDbType
//     obinary.GraphDbType
//
// This is a "server" command, so you must have already called
// ConnectToServer before calling this function.
//
func DropDatabase(dbc *DBClient, dbname string, dbtype constants.DatabaseType) error {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return oerror.SessionNotInitialized{}
	}

	// cmd
	err := rw.WriteByte(dbc.buf, REQUEST_DB_DROP)
	if err != nil {
		return err
	}

	// session id
	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return err
	}

	// database name, storage-type
	err = rw.WriteStrings(dbc.buf, dbname, string(dbtype))
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ read response from server ]--- */

	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	err = readAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return err
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

//
// DatabaseExists is a server-level command, so must be preceded by calling
// ConnectToServer, otherwise an authorization error will be returned.
// The storageType param must be either PersistentStorageType or VolatileStorageType.
//
func DatabaseExists(dbc *DBClient, dbname string, storageType constants.StorageType) (bool, error) {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return false, oerror.SessionNotInitialized{}
	}

	// cmd
	err := rw.WriteByte(dbc.buf, REQUEST_DB_EXIST)
	if err != nil {
		return false, err
	}

	// session id
	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return false, err
	}

	// database name, storage-type
	err = rw.WriteStrings(dbc.buf, dbname, string(storageType))
	if err != nil {
		return false, err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return false, err
	}

	/* ---[ Read Response From Server ]--- */

	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return false, err
	}

	err = readAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return false, err
	}

	if status == RESPONSE_STATUS_ERROR {
		serverExceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	// the answer to the query
	dbexists, err := rw.ReadBool(dbc.conx)
	if err != nil {
		return false, err
	}

	return dbexists, nil
}

//
// RequestDbList works like the "list databases" command from the OrientDB client.
// The result is put into a map, where the key of the map is the name of the
// database and the value is the type concatenated with the path, like so:
//
//     key:  cars
//     val:  plocal:/path/to/orientdb-community-2.0.1/databases/cars
//
func RequestDBList(dbc *DBClient) (map[string]string, error) {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return nil, oerror.SessionNotInitialized{}
	}

	// cmd
	err := rw.WriteByte(dbc.buf, REQUEST_DB_LIST)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// session id
	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = readAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	if status == RESPONSE_STATUS_ERROR {
		serverExceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		return nil, fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	// the bytes returned as a serialized EMBEDDEDMAP, so send it to the SerDe
	responseBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	serde := dbc.RecordSerDes[int(responseBytes[0])]
	buf := bytes.NewBuffer(responseBytes[1:])
	doc := oschema.NewDocument("")
	err = serde.Deserialize(doc, buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	m := make(map[string]string)
	fldMap := doc.Fields["databases"].Value.(map[string]interface{})
	for k, v := range fldMap {
		m[k] = v.(string)
	}

	return m, nil
}

func readAndValidateSessionId(rdr io.Reader, currentSessionId int32) error {
	sessionId, err := rw.ReadInt(rdr)
	if err != nil {
		return err
	}
	if sessionId != currentSessionId {
		return fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, currentSessionId)
	}
	return nil
}
