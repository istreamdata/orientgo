package obinary

import (
	"errors"
	"fmt"
)

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
	err := WriteByte(buf, REQUEST_CONNECT)
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

	// TODO: up to this point, the calls have been the same between REQUEST_CONNECT and REQUEST_DB_OPEN
	// admin username, password
	err = WriteStrings(buf, adminUser, adminPassw)
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
	status, err := ReadByte(dbc.conx)
	if err != nil {
		return err
	}

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

	// for the REQUEST_CONNECT case, another int is returned which is the new sessionId
	sessionId, err := ReadInt(dbc.conx)
	if err != nil {
		return err
	}
	// TODO: this assumes you can only have one sessionId - but perhaps can have a server sessionid
	//       and one or more database sessions open at the same time ?????
	dbc.sessionId = sessionId
	fmt.Printf("sessionId just set to: %v\n", dbc.sessionId) // DEBUG

	tokenBytes, err := ReadBytes(dbc.conx)
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
	err := WriteByte(dbc.buf, REQUEST_DB_CREATE)
	if err != nil {
		return err
	}

	// session id
	err = WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return err
	}

	err = WriteStrings(dbc.buf, dbname, dbtype, storageType)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ read response from server ]--- */

	status, err := ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	err = ReadAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return err
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

func DropDatabase(dbc *DbClient, dbname, dbtype string) error {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	if !validDbType(dbtype) {
		return InvalidDatabaseType{dbtype}
	}

	// cmd
	err := WriteByte(dbc.buf, REQUEST_DB_DROP)
	if err != nil {
		return err
	}

	// session id
	err = WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return err
	}

	// database name, storage-type
	err = WriteStrings(dbc.buf, dbname, dbtype)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ read response from server ]--- */

	status, err := ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	err = ReadAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return err
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

	err = ReadAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return false, err
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	// the answer to the query
	dbexists, err := ReadBool(dbc.conx)
	if err != nil {
		return false, err
	}

	return dbexists, nil
}

func RequestDbList(dbc *DbClient) error {
	dbc.buf.Reset()

	if dbc.sessionId == NoSessionId {
		return SessionNotInitialized{}
	}

	// cmd
	err := WriteByte(dbc.buf, REQUEST_DB_LIST)
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

	status, err := ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	err = ReadAndValidateSessionId(dbc.conx, dbc.sessionId)
	if err != nil {
		return err
	}

	if status == ERROR {
		serverExceptions, err := ReadErrorResponse(dbc.conx)
		if err != nil {
			return err
		}
		return fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	// TODO: have to figure out how to read the bytes returned
	responseBytes, err := ReadBytes(dbc.conx)
	if err != nil {
		return err
	}
	fmt.Printf("DB_LIST response size: %d; as str: %v\n", len(responseBytes),
		string(responseBytes)) // DEBUG

	return nil
}
