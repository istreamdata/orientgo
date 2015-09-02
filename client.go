package orient

import (
	"encoding/json"
	"fmt"
	"github.com/dyy18/orientgo/oschema"
	"strings"
)

type FetchPlan struct {
	Plan string
}

var (
	DefaultFetchPlan        = &FetchPlan{"*:0"}
	FetchPlanFollowAllLinks = &FetchPlan{"*:-1"}
)

type Manager interface {
	DatabaseExists(name string, storageType StorageType) (bool, error)
	CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error
	DropDatabase(name string, storageType StorageType) error
	ListDatabases() (map[string]string, error)
}

type Client interface {
	Auth(user, pass string) (Manager, error)
	Open(name string, dbType DatabaseType, user, pass string) (Database, error)
	Close() error
}

type Database interface {
	Size() (int64, error)
	Close() error
	ReloadSchema() error
	GetClasses() map[string]*oschema.OClass

	AddCluster(clusterName string) (clusterID int16, err error)
	DropCluster(clusterName string) (err error)

	CreateRecord(doc *oschema.ODocument) (err error)
	DeleteRecordByRID(rid string, recVersion int32) error
	DeleteRecordByRIDAsync(rid string, recVersion int32) error
	GetRecordByRID(rid oschema.ORID, fetchPlan string) (docs []*oschema.ODocument, err error)
	UpdateRecord(doc *oschema.ODocument) error
	CountRecords() (int64, error)

	CreateScriptFunc(fnc Function) error
	DeleteScriptFunc(name string) error
	UpdateScriptFunc(name string, script string) error
	CallScriptFunc(result interface{}, name string, params ...interface{}) (Records, error)
	CallScriptFuncJSON(result interface{}, name string, params ...interface{}) error
	InitScriptFunc(fncs ...Function) (err error)

	SQLQuery(result interface{}, fetchPlan *FetchPlan, sql string, params ...interface{}) (recs Records, err error)
	SQLCommand(result interface{}, sql string, params ...interface{}) (recs Records, err error)
	SQLBatch(result interface{}, sql string, params ...interface{}) (Records, error)

	SQLQueryOne(result interface{}, sql string, params ...interface{}) (Record, error)
	SQLCommandExpect(expected int, sql string, params ...interface{}) error
	SQLCommandOne(result interface{}, sql string, params ...interface{}) (Record, error)
	SQLBatchExpect(expected int, sql string, params ...interface{}) error
	SQLBatchOne(result interface{}, sql string, params ...interface{}) (Record, error)

	ExecScript(result interface{}, lang ScriptLang, script string, params ...interface{}) (recs Records, err error)
}

func Dial(addr string) (Client, error) {
	dial := protos[ProtoBinary]
	if dial == nil {
		return nil, fmt.Errorf("orientgo: no protocols are active; import obinary package?")
	}
	cli, err := protos[ProtoBinary](addr)
	if err != nil {
		return nil, err
	}
	return &client{cli}, nil
}

type client struct {
	c DBConnection
}

func (c *client) Auth(user, pass string) (Manager, error) {
	if err := c.c.ConnectToServer(user, pass); err != nil {
		return nil, err
	}
	return &manager{c}, nil
}
func (c *client) Open(name string, dbType DatabaseType, user, pass string) (Database, error) {
	if err := c.c.OpenDatabase(name, dbType, user, pass); err != nil {
		return nil, err
	}
	return &database{c}, nil
}
func (c *client) Close() error {
	return c.c.Close()
}

type manager struct {
	cli *client
}

func (mgr *manager) DatabaseExists(name string, storageType StorageType) (bool, error) {
	return mgr.cli.c.DatabaseExists(name, storageType)
}
func (mgr *manager) CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error {
	return mgr.cli.c.CreateDatabase(name, dbType, storageType)
}
func (mgr *manager) DropDatabase(name string, storageType StorageType) error {
	return mgr.cli.c.DropDatabase(name, storageType)
}
func (mgr *manager) ListDatabases() (map[string]string, error) {
	return mgr.cli.c.ListDatabases()
}

type database struct {
	cli *client
}

func (db *database) Size() (int64, error) {
	return db.cli.c.Size()
}
func (db *database) Close() error {
	return db.cli.c.CloseDatabase()
}
func (db *database) ReloadSchema() error {
	return db.cli.c.ReloadSchema()
}
func (db *database) GetClasses() map[string]*oschema.OClass {
	return db.cli.c.GetClasses()
}

func (db *database) AddCluster(name string) (int16, error) {
	return db.cli.c.AddCluster(name)
}
func (db *database) DropCluster(name string) error {
	return db.cli.c.DropCluster(name)
}

func (db *database) CreateRecord(doc *oschema.ODocument) error {
	return db.cli.c.CreateRecord(doc)
}
func (db *database) DeleteRecordByRID(rid string, recVersion int32) error {
	return db.cli.c.DeleteRecordByRID(rid, recVersion)
}
func (db *database) DeleteRecordByRIDAsync(rid string, recVersion int32) error {
	return db.cli.c.DeleteRecordByRIDAsync(rid, recVersion)
}
func (db *database) GetRecordByRID(rid oschema.ORID, fetchPlan string) ([]*oschema.ODocument, error) {
	return db.cli.c.GetRecordByRID(rid, fetchPlan)
}
func (db *database) UpdateRecord(doc *oschema.ODocument) error {
	return db.cli.c.UpdateRecord(doc)
}
func (db *database) CountRecords() (int64, error) {
	return db.cli.c.CountRecords()
}

func (db *database) CreateScriptFunc(fnc Function) error {
	return db.cli.c.CreateScriptFunc(fnc)
}
func (db *database) DeleteScriptFunc(name string) error {
	return db.cli.c.DeleteScriptFunc(name)
}
func (db *database) UpdateScriptFunc(name string, script string) error {
	return db.cli.c.UpdateScriptFunc(name, script)
}
func (db *database) CallScriptFunc(result interface{}, name string, params ...interface{}) (Records, error) {
	return db.cli.c.CallScriptFunc(result, name, params...)
}

// CallScriptFuncJSON is a workaround for driver bug. It allow to return pure JS objects from DB functions.
func (db *database) CallScriptFuncJSON(result interface{}, name string, params ...interface{}) error {
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)
		sparams = append(sparams, string(data))
	}
	var jsonData string
	_, err := db.ExecScript(&jsonData, LangJS, `JSON.stringify(`+name+`(`+strings.Join(sparams, ",")+`))`)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(jsonData), result)
}
func (db *database) InitScriptFunc(fncs ...Function) (err error) {
	for _, fnc := range fncs {
		if fnc.Lang == "" {
			err = fmt.Errorf("no language provided for function '%s'", fnc.Name)
			return
		}
		db.DeleteScriptFunc(fnc.Name)
		err = db.CreateScriptFunc(fnc)
		if err != nil && !strings.Contains(err.Error(), "found duplicated key") {
			return
		}
	}
	return nil
}

func (db *database) SQLQuery(result interface{}, fetchPlan *FetchPlan, sql string, params ...interface{}) (Records, error) {
	return db.cli.c.SQLQuery(result, fetchPlan, sql, params...)
}
func (db *database) SQLQueryOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLQuery(result, nil, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}
func (db *database) SQLCommand(result interface{}, sql string, params ...interface{}) (Records, error) {
	return db.cli.c.SQLCommand(result, sql, params...)
}
func (db *database) SQLCommandExpect(expected int, sql string, params ...interface{}) error {
	return checkExpected(expected)(db.SQLCommand(nil, sql, params...))
}
func (db *database) SQLCommandOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLCommand(result, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}
func (db *database) SQLBatch(result interface{}, sql string, params ...interface{}) (Records, error) {
	return db.ExecScript(result, LangSQL, sql, params...)
}
func (db *database) SQLBatchExpect(expected int, sql string, params ...interface{}) error {
	return checkExpected(expected)(db.SQLBatch(nil, sql, params...))
}
func (db *database) SQLBatchOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLBatch(result, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}

func (db *database) ExecScript(result interface{}, lang ScriptLang, script string, params ...interface{}) (Records, error) {
	return db.cli.c.ExecScript(result, lang, script, params...)
}

type ErrUnexpectedResultCount struct {
	Expected int
	Count    int
}

func (e ErrUnexpectedResultCount) Error() string {
	return fmt.Sprintf("expected %d record to be modified, but got %d", e.Expected, e.Count)
}

func checkExpected(expected int) func(Records, error) error {
	return func(recs Records, err error) error {
		if err != nil {
			return err
		}
		var mod int
		if err = recs.DeserializeAll(&mod); err != nil {
			return err
		}
		if expected >= 0 && expected != mod {
			err = ErrUnexpectedResultCount{Expected: expected, Count: mod}
		} else if expected < 0 && mod == 0 {
			err = ErrUnexpectedResultCount{Expected: expected, Count: mod}
		}
		return err
	}
}

func MarshalContent(o interface{}) string {
	data, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return string(data)
}
