package obinary

import (
	"errors"
	"fmt"
)

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
