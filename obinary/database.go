package obinary

import (
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/oschema"
	"strconv"
	"strings"
	"sync"
)

type ODatabase struct {
	Name             string
	Type             orient.DatabaseType
	Clusters         []OCluster
	ClustCfg         []byte // TODO: why is this a byte array? Just placeholder? What is it in the Java client?
	SchemaVersion    int
	Classes          map[string]*oschema.OClass
	storageMu        sync.RWMutex
	StorageCfg       OStorageConfiguration // TODO: redundant to ClustCfg ??
	globalPropMu     sync.RWMutex
	globalProperties map[int]oschema.OGlobalProperty
}

func (db *ODatabase) SetGlobalProperty(id int, p oschema.OGlobalProperty) {
	if db == nil {
		return
	}
	db.globalPropMu.Lock()
	if db.globalProperties == nil {
		db.globalProperties = make(map[int]oschema.OGlobalProperty)
	}
	db.globalProperties[id] = p
	db.globalPropMu.Unlock()
}
func (db *ODatabase) GetGlobalProperty(id int) (p oschema.OGlobalProperty, ok bool) {
	if db == nil {
		ok = false
		return
	}
	db.globalPropMu.RLock()
	if db.globalProperties != nil {
		p, ok = db.globalProperties[id]
	}
	db.globalPropMu.RUnlock()
	return
}

func NewDatabase(name string, dbtype orient.DatabaseType) *ODatabase {
	return &ODatabase{
		Name:          name,
		Type:          dbtype,
		SchemaVersion: -1,
		Classes:       make(map[string]*oschema.OClass),
	}
}

// OStorageConfiguration holds (some of) the information in the "Config Record"
// #0:0.  At this time, I'm throwing away a lot of the info in record #0:0
// until proven that the ogonori client needs them.
type OStorageConfiguration struct {
	version       byte // TODO: of what? (=14 for OrientDB 2.1)
	name          string
	schemaRID     oschema.RID // usually #0:1
	dictionaryRID string
	idxMgrRID     oschema.RID // usually #0:2
	localeLang    string
	localeCountry string
	dateFmt       string
	dateTimeFmt   string
	timezone      string
}

// parseConfigRecord takes the pipe-separate values that comes back
// from reading record #0:0 and turns it into an OStorageConfiguration
// object, which it adds to the db database object.
func (sc *OStorageConfiguration) parse(psvData string) error {
	toks := strings.Split(psvData, "|")

	version, err := strconv.ParseInt(toks[0], 10, 8)
	if err != nil {
		return err
	}

	sc.version = byte(version)
	sc.name = strings.TrimSpace(toks[1])
	sc.schemaRID = oschema.MustParseRID(toks[2])
	sc.dictionaryRID = strings.TrimSpace(toks[3])
	sc.idxMgrRID = oschema.MustParseRID(toks[4])
	sc.localeLang = strings.TrimSpace(toks[5])
	sc.localeCountry = strings.TrimSpace(toks[6])
	sc.dateFmt = strings.TrimSpace(toks[7])
	sc.dateTimeFmt = strings.TrimSpace(toks[8])
	sc.timezone = strings.TrimSpace(toks[9])

	return nil
}

type OCluster struct {
	Name string
	Id   int16
}
