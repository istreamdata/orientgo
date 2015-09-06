package obinary

import (
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/oschema"
)

type ODatabase struct {
	Name             string
	Type             orient.DatabaseType
	Clusters         []OCluster
	ClustCfg         []byte                // TODO: why is this a byte array? Just placeholder? What is it in the Java client?
	StorageCfg       OStorageConfiguration // TODO: redundant to ClustCfg ??
	SchemaVersion    int32
	GlobalProperties map[int]oschema.OGlobalProperty
	Classes          map[string]*oschema.OClass
}

func NewDatabase(name string, dbtype orient.DatabaseType) *ODatabase {
	return &ODatabase{
		Name:             name,
		Type:             dbtype,
		SchemaVersion:    -1,
		GlobalProperties: make(map[int]oschema.OGlobalProperty),
		Classes:          make(map[string]*oschema.OClass),
	}
}

// OStorageConfiguration holds (some of) the information in the "Config Record"
// #0:0.  At this time, I'm throwing away a lot of the info in record #0:0
// until proven that the ogonori client needs them.
type OStorageConfiguration struct {
	version       byte // TODO: of what? (=14 for OrientDB 2.1)
	name          string
	schemaRID     oschema.ORID // usually #0:1
	dictionaryRID string
	idxMgrRID     oschema.ORID // usually #0:2
	localeLang    string
	localeCountry string
	dateFmt       string
	dateTimeFmt   string
	timezone      string
}

type OCluster struct {
	Name string
	Id   int16
}
