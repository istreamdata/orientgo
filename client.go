package orient

import (
	"encoding/json"
	"fmt"
	"github.com/dyy18/orientgo/oschema"
	"strings"
	"sync"
)

const poolLimit = 10

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
	Close() error
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
	GetClusterDataRange(clusterName string) (begin, end int64, err error)
	CountClusters(withDeleted bool, clusterNames ...string) (int64, error)

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
	cli := &client{
		addr:  addr,
		conns: make(map[string]chan DBConnection, 2),
	}
	conn, err := cli.getConn("")
	if err != nil {
		return nil, err
	}
	cli.putConn("", conn)
	return cli, nil
}

type client struct {
	addr  string
	mu    sync.Mutex
	conns map[string]chan DBConnection
}

func (c *client) getConn(name string) (DBConnection, error) {
	c.mu.Lock()
	ch := c.conns[name]
	c.mu.Unlock()
	select {
	case conn := <-ch:
		return conn, nil
	default:
		dial := protos[ProtoBinary]
		if dial == nil {
			return nil, fmt.Errorf("orientgo: no protocols are active; forgot to import obinary package?")
		}
		conn, err := dial(c.addr)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func (c *client) putConn(name string, conn DBConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.conns[name]
	if ch == nil {
		ch = make(chan DBConnection, poolLimit)
		c.conns[name] = ch
	}
	select {
	case ch <- conn:
	default:
		conn.Close()
	}
}

func (c *client) Auth(user, pass string) (Manager, error) {
	conn, err := c.getConn("")
	if err != nil {
		return nil, err
	}
	if err := conn.ConnectToServer(user, pass); err != nil {
		conn.Close()
		return nil, err
	}
	return &manager{conn, c}, nil
}
func (c *client) Open(name string, dbType DatabaseType, user, pass string) (Database, error) {
	conn, err := c.getConn(name)
	if err != nil {
		return nil, err
	}
	if err := conn.OpenDatabase(name, dbType, user, pass); err != nil {
		return nil, err
	}
	return &database{name, conn, c}, nil
}
func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, ch := range c.conns {
		close(ch)
		for conn := range ch {
			conn.Close()
		}
		delete(c.conns, name)
	}
	return nil
}

type manager struct {
	conn DBConnection
	cli  *client
}

func (mgr *manager) DatabaseExists(name string, storageType StorageType) (bool, error) {
	return mgr.conn.DatabaseExists(name, storageType)
}
func (mgr *manager) CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error {
	return mgr.conn.CreateDatabase(name, dbType, storageType)
}
func (mgr *manager) DropDatabase(name string, storageType StorageType) error {
	return mgr.conn.DropDatabase(name, storageType)
}
func (mgr *manager) ListDatabases() (map[string]string, error) {
	return mgr.conn.ListDatabases()
}
func (mgr *manager) Close() error {
	mgr.cli.putConn("", mgr.conn)
	mgr.conn = nil
	return nil
}

type database struct {
	name string
	conn DBConnection
	cli  *client
}

func (db *database) Size() (int64, error) {
	return db.conn.Size()
}
func (db *database) Close() error {
	//return db.conn.CloseDatabase()
	db.cli.putConn(db.name, db.conn)
	db.conn = nil
	return nil
}
func (db *database) ReloadSchema() error {
	return db.conn.ReloadSchema()
}
func (db *database) GetClasses() map[string]*oschema.OClass {
	return db.conn.GetClasses()
}

func (db *database) AddCluster(name string) (int16, error) {
	return db.conn.AddCluster(name)
}
func (db *database) DropCluster(name string) error {
	return db.conn.DropCluster(name)
}
func (db *database) GetClusterDataRange(clusterName string) (begin, end int64, err error) {
	return db.conn.GetClusterDataRange(clusterName)
}
func (db *database) CountClusters(withDeleted bool, clusterNames ...string) (int64, error) {
	return db.conn.CountClusters(withDeleted, clusterNames...)
}

func (db *database) CreateRecord(doc *oschema.ODocument) error {
	return db.conn.CreateRecord(doc)
}
func (db *database) DeleteRecordByRID(rid string, recVersion int32) error {
	return db.conn.DeleteRecordByRID(rid, recVersion)
}
func (db *database) DeleteRecordByRIDAsync(rid string, recVersion int32) error {
	return db.conn.DeleteRecordByRIDAsync(rid, recVersion)
}
func (db *database) GetRecordByRID(rid oschema.ORID, fetchPlan string) ([]*oschema.ODocument, error) {
	return db.conn.GetRecordByRID(rid, fetchPlan)
}
func (db *database) UpdateRecord(doc *oschema.ODocument) error {
	return db.conn.UpdateRecord(doc)
}
func (db *database) CountRecords() (int64, error) {
	return db.conn.CountRecords()
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
	return db.conn.SQLQuery(result, fetchPlan, sql, params...)
}
func (db *database) SQLQueryOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLQuery(result, nil, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}
func (db *database) SQLCommand(result interface{}, sql string, params ...interface{}) (Records, error) {
	return db.conn.SQLCommand(result, sql, params...)
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
	return db.conn.ExecScript(result, lang, script, params...)
}

func sqlEscape(s string) string { // TODO: escape things in a right way
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `"`, `\"`, -1)
	return `"` + s + `"`
}

func (db *database) CreateScriptFunc(fnc Function) error {
	sql := `CREATE FUNCTION ` + fnc.Name + ` ` + sqlEscape(fnc.Code)
	if len(fnc.Params) > 0 {
		sql += ` PARAMETERS [` + strings.Join(fnc.Params, ", ") + `]`
	}
	sql += ` IDEMPOTENT ` + fmt.Sprint(fnc.Idemp)
	if fnc.Lang != "" {
		sql += ` LANGUAGE ` + string(fnc.Lang)
	}
	_, err := db.SQLCommand(nil, sql)
	return err
}

func (db *database) DeleteScriptFunc(name string) error {
	_, err := db.SQLCommand(nil, `DELETE FROM OFunction WHERE name = ?`, name)
	return err
}

func (db *database) UpdateScriptFunc(name string, script string) error {
	_, err := db.SQLCommand(nil, `UPDATE OFunction SET code = ? WHERE name = ?`, script, name)
	return err
}

func (db *database) CallScriptFunc(result interface{}, name string, params ...interface{}) (Records, error) {
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)
		sparams = append(sparams, string(data))
	}
	return db.ExecScript(result, LangJS, name+`(`+strings.Join(sparams, ",")+`)`)
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
