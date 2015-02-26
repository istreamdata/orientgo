package obinary

import "github.com/quux00/ogonori/oschema"

// --------

type ODatabase struct {
	Name             string
	Typ              string // DocumentDbType or GraphDbType
	Clusters         []OCluster
	ClustCfg         []byte // TODO: why is this a byte array? Just placeholder? What is it in the Java client?
	SchemaVersion    int32
	GlobalProperties map[int]oschema.OGlobalProperty // key: property-id (aka field-id)
	Classes          map[string]*oschema.OClass      // key: class name (TODO: check how Java client does it)
}

func NewDatabase(name, dbtype string) *ODatabase {
	gp := make(map[int]oschema.OGlobalProperty)

	return &ODatabase{Name: name, Typ: dbtype, SchemaVersion: -1, GlobalProperties: gp}
}

type OCluster struct {
	Name string
	Id   int16 // TODO: maybe change to int?
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
