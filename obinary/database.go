package obinary // TODO: these types need to move into package odb ??

import (
	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary/binserde"
	"github.com/quux00/ogonori/oschema"
)

// --------

type ODatabase struct {
	Name             string
	Typ              constants.DatabaseType
	Clusters         []OCluster
	ClustCfg         []byte                // TODO: why is this a byte array? Just placeholder? What is it in the Java client?
	StorageCfg       OStorageConfiguration // TODO: redundant to ClustCfg ??
	SchemaVersion    int32
	RecordSerDes     []binserde.ORecordSerializer    // serdes w/ global properties - for db-level cmds
	GlobalProperties map[int]oschema.OGlobalProperty // key: property-id (aka field-id)
	Classes          map[string]*oschema.OClass      // key: class name (TODO: check how Java client does it)
	//
	// The map of GlobalProperties is shared between the ODatabase struct and the ORecordSerializer
	// objects - it is the same map.  So changes to one will change the other.
	//
}

func NewDatabase(name string, dbtype constants.DatabaseType) *ODatabase {
	gp := make(map[int]oschema.OGlobalProperty)

	serdeV0 := &binserde.ORecordSerializerV0{gp}

	return &ODatabase{
		Name:             name,
		Typ:              dbtype,
		SchemaVersion:    -1,
		GlobalProperties: gp,
		RecordSerDes:     []binserde.ORecordSerializer{serdeV0},
		Classes:          make(map[string]*oschema.OClass),
	}
}

//
// OStorageConfiguration holds (some of) the information in the "Config Record"
// #0:0.  At this time, I'm throwing away a lot of the info in record #0:0
// until proven that the ogonori client needs them.
//
type OStorageConfiguration struct {
	version       byte // TODO: of what? (=14 for OrientDB 2.1)
	name          string
	schemaRID     string // usually #0:1
	dictionaryRID string
	idxMgrRID     string // usually #0:2
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

// --------

//
// ClientOptions should be created by the end user to configure details and
// options needed when opening a database or connecting to the OrientDB server
//
type ClientOptions struct {
	ServerHost      string
	ServerPort      string
	ClusteredConfig string // TODO: needs research - what goes here?; currently not used
}

// --------
