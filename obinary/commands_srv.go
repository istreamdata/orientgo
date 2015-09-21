package obinary

import (
	"io"

	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
)

type Manager struct {
	sess *session
}

/// In the Java client the "server command" functionality is encapsulated
/// the OServerAdmin class.  TODO: may want to follow suit rather than
/// using the same DBClient for both server-commands and db-commands,
/// especially since (I think) they have separate logins.

// ConnectToServer logs into the OrientDB server with the appropriate
// admin privileges in order to execute server-level commands (as opposed
// to database-level commands). This must be called to establish a server
// session before any other server-level commands. The username and password
// required are for the server (admin) not any particular database.
func (c *Client) ConnectToServer(adminUser, adminPassw string) (mgr *Manager, err error) {
	var (
		sessId int32
		//token []byte
	)
	err = c.root.sendCmd(requestConnect, func(w *rw.Writer) error {
		w.WriteStrings(driverName, driverVersion)
		w.WriteShort(int16(c.curProtoVers))
		w.WriteNull() // dbclient id - only for cluster config // TODO: change to use dbc.clusteredConfig once that is added
		w.WriteString(c.recordFormat.String())
		w.WriteBool(false) // use token (true) or session (false)
		w.WriteStrings(adminUser, adminPassw)
		return w.Err()
	}, func(r *rw.Reader) error {
		sessId = r.ReadInt()
		_ = r.ReadBytes() // token - ignore for now
		return r.Err()
	})
	if err != nil {
		return
	}
	mgr = &Manager{sess: c.newSess(sessId)}
	return
}

func (c *Client) Auth(adminUser, adminPassw string) (orient.DBAdmin, error) {
	return c.ConnectToServer(adminUser, adminPassw)
}

// CreateDatabase will create a `remote` database of the type and storageType specified.
// dbType must be type DocumentDBType or GraphDBType.
// storageType must type PersistentStorageType or VolatileStorageType.
func (m *Manager) CreateDatabase(dbname string, dbtype orient.DatabaseType, storageType orient.StorageType) error {
	return m.sess.sendCmd(requestDbCreate, func(w *rw.Writer) error {
		return w.WriteStrings(dbname, string(dbtype), string(storageType))
	}, nil)
}

// DropDatabase drops the specified database. The caller must provide
// both the name and the type of the database.  The type should either:
//
//     obinary.DocumentDBType
//     obinary.GraphDBType
//
// This is a "server" command, so you must have already called
// ConnectToServer before calling this function.
func (m *Manager) DropDatabase(dbname string, dbtype orient.StorageType) (err error) {
	return m.sess.sendCmd(requestDbDrop, func(w *rw.Writer) error {
		return w.WriteStrings(dbname, string(dbtype))
	}, nil)
}

// DatabaseExists is a server-level command, so must be preceded by calling
// ConnectToServer, otherwise an authorization error will be returned.
// The storageType param must be either PersistentStorageType or VolatileStorageType.
func (m *Manager) DatabaseExists(dbname string, storageType orient.StorageType) (val bool, err error) {
	err = m.sess.sendCmd(requestDbExists, func(w *rw.Writer) error {
		return w.WriteStrings(dbname, string(storageType))
	}, func(r *rw.Reader) error {
		val = r.ReadBool()
		return r.Err()
	})
	return
}

// RequestDBList works like the "list databases" command from the OrientDB client.
// The result is put into a map, where the key of the map is the name of the
// database and the value is the type concatenated with the path, like so:
//
//     key:  cars
//     val:  plocal:/path/to/orientdb-community-2.0.1/databases/cars
func (m *Manager) ListDatabases() (list map[string]string, err error) {
	var data []byte
	err = m.sess.sendCmd(requestDbLIST, nil, func(r *rw.Reader) error {
		data = r.ReadBytes()
		return r.Err()
	})
	if err != nil {
		return
	} else if len(data) == 0 {
		err = io.ErrUnexpectedEOF
		return
	}
	// the bytes returned as a serialized EMBEDDEDMAP, so send it to the SerDe
	ser := m.sess.cli.recordFormat

	var (
		o interface{}
	)
	o, err = ser.FromStream(data)
	if err != nil {
		return
	}
	doc := o.(*orient.Document)

	list = doc.GetField("databases").Value.(map[string]string)
	return
}

func (m *Manager) Close() error {
	// TODO: what can we do?
	return m.sess.cli.Close()
}
