package obinary

import (
	"errors"
	"fmt"
	"io"

	"github.com/quux00/ogonori/obinary/rw"
)

///
/// In the Java client the "server command" functionality is encapsulated
/// the OServerAdmin class.  TODO: may want to follow suit rather than
/// using the same DbClient for both server-commands and db-commands,
/// especially since (I think) they have separate logins.
///

//
// CreateServerSession logs into the OrientDB server with the appropriate
// admin privileges in order to execute server-level commands (as opposed
// to database-level commands). This must be called to establish a server
// session before any other server-level commands. The username and password
// required are for the server (admin) not any particular database.
//
func CreateServerSession(dbc *DbClient, adminUser, adminPassw string) error {
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

	// TODO: up to this point, the calls have been the same between REQUEST_CONNECT and REQUEST_DB_OPEN
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
	fmt.Printf("sessionId just set to: %v\n", dbc.sessionId) // DEBUG

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
func CreateDatabase(dbc *DbClient, dbname, dbtype, storageType string) error {
	dbc.buf.Reset()

	/* ---[ precondition checks ]--- */

	// TODO: may need to change this to serverSessionid
	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	if !validStorageType(storageType) {
		return InvalidStorageType{storageType}
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

	err = rw.WriteStrings(dbc.buf, dbname, dbtype, storageType)
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

func DropDatabase(dbc *DbClient, dbname, dbtype string) error {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	if !validDbType(dbtype) {
		return InvalidDatabaseType{dbtype}
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
	err = rw.WriteStrings(dbc.buf, dbname, dbtype)
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
// CreateServerSession, otherwise an authorization error will be returned.
// The storageType param must be either PersistentStorageType or VolatileStorageType.
//
func DatabaseExists(dbc *DbClient, dbname, storageType string) (bool, error) {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return false, SessionNotInitialized{}
	}

	if !validStorageType(storageType) {
		return false, InvalidStorageType{storageType}
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
	err = rw.WriteStrings(dbc.buf, dbname, storageType)
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

// TODO: this is not fully implemented since I don't understand what data is being returned:
// Reading byte (1 byte)... [OChannelBinaryServer]
// Read byte: 74 [OChannelBinaryServer]
// Reading int (4 bytes)... [OChannelBinaryServer]
// Read int: 184 [OChannelBinaryServer]
// Writing byte (1 byte): 0 [OChannelBinaryServer]
// Writing int (4 bytes): 184 [OChannelBinaryServer]
// Writing bytes (4+219=223 bytes): [0, 0, 18, 100, 97, 116, 97, 98, 97, 115, 101, 115, 0, 0, 0, 18, 12, 0, 4, 7, 8, 99, 97, 114, 115, 0, 0, 0, 57, 7, 7, 40, 71, 114, 97, 116, 101, 102, 117, 108, 68, 101, 97, 100, 67, 111, 110, 99, 101, 114, 116, 115, 0, 0, 0, -126, 7, -114, 1, 112, 108, 111, 99, 97, 108, 58, 47, 104, 111, 109, 101, 47, 109, 105, 100, 112, 101, 116, 101, 114, 52, 52, 52, 47, 97, 112, 112, 115, 47, 111, 114, 105, 101, 110, 116, 100, 98, 45, 99, 111, 109, 109, 117, 110, 105, 116, 121, 45, 50, 46, 48, 45, 114, 99, 50, 47, 100, 97, 116, 97, 98, 97, 115, 101, 115, 47, 99, 97, 114, 115, -82, 1, 112, 108, 111, 99, 97, 108, 58, 47, 104, 111, 109, 101, 47, 109, 105, 100, 112, 101, 116, 101, 114, 52, 52, 52, 47, 97, 112, 112, 115, 47, 111, 114, 105, 101, 110, 116, 100, 98, 45, 99, 111, 109, 109, 117, 110, 105, 116, 121, 45, 50, 46, 48, 45, 114, 99, 50, 47, 100, 97, 116, 97, 98, 97, 115, 101, 115, 47, 71, 114, 97, 116, 101, 102, 117, 108, 68, 101, 97, 100, 67, 111, 110, 99, 101, 114, 116, 115] [OChannelBinaryServer]
func RequestDbList(dbc *DbClient) error {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	// cmd
	err := rw.WriteByte(dbc.buf, REQUEST_DB_LIST)
	if err != nil {
		return err
	}

	// session id
	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

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

	// TODO: have to figure out how to read the bytes returned
	responseBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	fmt.Printf("DB_LIST response size: %d; as str: %v\n", len(responseBytes),
		string(responseBytes)) // DEBUG

	return nil
}

func readAndValidateSessionId(rdr io.Reader, currentSessionId int) error {
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
