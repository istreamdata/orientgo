package obinary

import (
	"errors"
	"fmt"
)

func CreateServerSession(dbc *DbClient, adminUser, adminPassw string) error {
	buf := dbc.buf
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
// DatabaseExists is a server-level command, so must be preceded by calling
// CreateServerSession, otherwise an authorization error will be returned.
// The storageType param must be either PersistentStorageType or VolatileStorageType.
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
