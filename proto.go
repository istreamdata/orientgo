package orient

import (
	"github.com/istreamdata/orientgo/oschema"
)

const (
	ProtoBinary = "binary"
)

type DatabaseType string
type StorageType string

const (
	DocumentDB DatabaseType = "document"
	GraphDB    DatabaseType = "graph"

	Persistent StorageType = "plocal"
	Volatile   StorageType = "memory"
)

var (
	protos = make(map[string]func(addr string) (DBConnection, error))
)

func RegisterProto(name string, dial func(addr string) (DBConnection, error)) {
	protos[name] = dial
}

type ODatabase struct {
	Name    string
	Type    DatabaseType
	Classes map[string]*oschema.OClass
}

type DBManager interface {
	DatabaseExists(name string, storageType StorageType) (bool, error)
	CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error
	DropDatabase(name string, storageType StorageType) error
	ListDatabases() (map[string]string, error)
	Close() error
}

type DBSession interface {
	Close() error
	Size() (int64, error)
	ReloadSchema() error
	GetCurDB() *ODatabase

	AddCluster(clusterName string) (clusterID int16, err error)
	DropCluster(clusterName string) (err error)
	GetClusterDataRange(clusterName string) (begin, end int64, err error)
	ClustersCount(withDeleted bool, clusterNames ...string) (int64, error)

	CreateRecord(doc *oschema.ODocument) (err error)
	DeleteRecordByRID(rid oschema.RID, recVersion int32) error
	GetRecordByRID(rid oschema.RID, fetchPlan string, ignoreCache, loadTombstones bool) (recs Records, err error)
	UpdateRecord(doc *oschema.ODocument) error
	CountRecords() (int64, error)

	SQLQuery(fetchPlan *FetchPlan, sql string, params ...interface{}) (recs Records, err error)
	SQLCommand(sql string, params ...interface{}) (recs Records, err error)
	ExecScript(lang ScriptLang, script string, params ...interface{}) (recs Records, err error)
}

type DBConnection interface {
	Auth(user, pass string) (DBManager, error)
	Open(name string, dbType DatabaseType, user, pass string) (DBSession, error)
	Close() error
}
