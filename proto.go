package orient

// Default protocols
const (
	ProtoBinary = "binary"
)

// DatabaseType defines database access type (Document or Graph)
type DatabaseType string

// List of database access types
const (
	DocumentDB DatabaseType = "document"
	GraphDB    DatabaseType = "graph"
)

// StorageType defines supported database storage types
type StorageType string

const (
	// Persistent type represents on-disk database
	Persistent StorageType = "plocal"
	// Volatile type represents in-memory database
	Volatile StorageType = "memory"
)

var (
	protos = make(map[string]func(addr string) (DBConnection, error))
)

// RegisterProto registers a new protocol for Dial command
func RegisterProto(name string, dial func(addr string) (DBConnection, error)) {
	protos[name] = dial
}

// ODatabase stores database metadata
type ODatabase struct {
	Name    string
	Type    DatabaseType
	Classes map[string]*OClass
}

// DBAdmin is a minimal interface for database management API implementation
type DBAdmin interface {
	DatabaseExists(name string, storageType StorageType) (bool, error)
	CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error
	DropDatabase(name string, storageType StorageType) error
	ListDatabases() (map[string]string, error)
	Close() error
}

// DBSession is a minimal interface for database API implementation
type DBSession interface {
	Close() error
	Size() (int64, error)
	ReloadSchema() error
	GetCurDB() *ODatabase

	AddClusterWithID(clusterName string, id int16) (clusterID int16, err error)
	DropCluster(clusterName string) (err error)
	GetClusterDataRange(clusterName string) (begin, end int64, err error)
	ClustersCount(withDeleted bool, clusterNames ...string) (int64, error)

	CreateRecord(rec ORecord) (err error)
	DeleteRecordByRID(rid RID, recVersion int) error
	GetRecordByRID(rid RID, fetchPlan FetchPlan, ignoreCache bool) (rec ORecord, err error)
	UpdateRecord(rec ORecord) error
	CountRecords() (int64, error)

	Command(cmd CustomSerializable) (result interface{}, err error)
}

// DBConnection is a minimal interface for OrientDB server API implementation
type DBConnection interface {
	Auth(user, pass string) (DBAdmin, error)
	Open(name string, dbType DatabaseType, user, pass string) (DBSession, error)
	Close() error
}
