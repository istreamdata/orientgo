package orient

import (
	"encoding/json"
	"fmt"
	"github.com/istreamdata/orientgo/oschema"
	"strings"
)

const poolLimit = 6

type FetchPlan struct {
	Plan string
}

var (
	DefaultFetchPlan        = &FetchPlan{"*:0"}
	FetchPlanFollowAllLinks = &FetchPlan{"*:-1"}
)

func Dial(addr string) (*Client, error) {
	dial := protos[ProtoBinary]
	if dial == nil {
		return nil, fmt.Errorf("orientgo: no protocols are active; forgot to import obinary package?")
	}
	cli := &Client{
		dial: func() (DBConnection, error) {
			return dial(addr)
		},
	}
	conn, err := cli.dial()
	if err != nil {
		return nil, err
	}
	cli.mconn = conn
	return cli, nil
}

func newConnPool(size int, dial func() (DBSession, error)) *connPool {
	if size <= 0 {
		size = poolLimit
	}
	p := &connPool{
		dial: dial,
		ch:   make(chan DBSession, size),
		toks: make(chan struct{}, size),
	}
	for i := 0; i < size; i++ {
		p.toks <- struct{}{}
	}
	return p
}

type connPool struct {
	dial func() (DBSession, error)
	ch   chan DBSession
	toks chan struct{}
}

func (p *connPool) getConn() (DBSession, error) {
	select {
	case conn := <-p.ch:
		return conn, nil
	case <-p.toks:
		if p.dial == nil {
			return nil, nil
		}
		conn, err := p.dial()
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}
func (p *connPool) putConn(conn DBSession) {
	select {
	case p.ch <- conn:
	default:
		select {
		case p.toks <- struct{}{}:
		default:
		}
		conn.Close()
	}
}
func (p *connPool) clear() {
loop:
	for {
		select {
		case conn := <-p.ch:
			if conn != nil {
				conn.Close()
			}
		case <-p.toks:
		default:
			break loop
		}
	}
	for len(p.toks) < cap(p.toks) {
		p.toks <- struct{}{}
	}
}

type Client struct {
	mconn DBConnection
	dial  func() (DBConnection, error)
}

func (c *Client) Auth(user, pass string) (*Manager, error) {
	if c.mconn == nil {
		conn, err := c.dial()
		if err != nil {
			return nil, err
		}
		c.mconn = conn
	}
	m, err := c.mconn.Auth(user, pass)
	if err != nil {
		return nil, err
	}
	return &Manager{c, m}, nil
}
func (c *Client) Open(name string, dbType DatabaseType, user, pass string) (*Database, error) {
	db := &Database{newConnPool(poolLimit, func() (DBSession, error) {
		conn, err := c.dial()
		if err != nil {
			return nil, err
		}
		ds, err := conn.Open(name, dbType, user, pass)
		if err != nil {
			conn.Close()
			return nil, err
		}
		return ds, nil
	}), c}
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	db.pool.putConn(conn)
	return db, nil
}
func (c *Client) Close() error {
	if c.mconn != nil {
		c.mconn.Close()
	}
	return nil
}

type Manager struct {
	cli *Client
	m   DBManager
}

func (mgr *Manager) DatabaseExists(name string, storageType StorageType) (bool, error) {
	return mgr.m.DatabaseExists(name, storageType)
}
func (mgr *Manager) CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error {
	return mgr.m.CreateDatabase(name, dbType, storageType)
}
func (mgr *Manager) DropDatabase(name string, storageType StorageType) error {
	return mgr.m.DropDatabase(name, storageType)
}
func (mgr *Manager) ListDatabases() (map[string]string, error) {
	return mgr.m.ListDatabases()
}
func (mgr *Manager) Close() error {
	return mgr.m.Close()
}

type Database struct {
	pool *connPool
	cli  *Client
}

func (db *Database) Size() (int64, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.Size()
}
func (db *Database) Close() error {
	db.pool.clear()
	return nil
}
func (db *Database) ReloadSchema() error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.ReloadSchema()
}
func (db *Database) GetCurDB() *ODatabase {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil
	}
	defer db.pool.putConn(conn)
	return conn.GetCurDB()
}

func (db *Database) AddCluster(name string) (int16, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.AddCluster(name)
}
func (db *Database) DropCluster(name string) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.DropCluster(name)
}
func (db *Database) GetClusterDataRange(clusterName string) (begin, end int64, err error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, 0, err
	}
	defer db.pool.putConn(conn)
	return conn.GetClusterDataRange(clusterName)
}
func (db *Database) ClustersCount(withDeleted bool, clusterNames ...string) (int64, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.ClustersCount(withDeleted, clusterNames...)
}

func (db *Database) CreateRecord(doc *oschema.ODocument) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.CreateRecord(doc)
}
func (db *Database) DeleteRecordByRID(rid oschema.RID, recVersion int32) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.DeleteRecordByRID(rid, recVersion)
}
func (db *Database) GetRecordByRID(rid oschema.RID, fetchPlan string, ignoreCache, loadTombstones bool) (Records, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	defer db.pool.putConn(conn)
	return conn.GetRecordByRID(rid, fetchPlan, ignoreCache, loadTombstones)
}
func (db *Database) UpdateRecord(doc *oschema.ODocument) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.UpdateRecord(doc)
}
func (db *Database) CountRecords() (int64, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.CountRecords()
}
func (db *Database) SQLQuery(result interface{}, fetchPlan *FetchPlan, sql string, params ...interface{}) (Records, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	defer db.pool.putConn(conn)
	recs, err := conn.SQLQuery(fetchPlan, sql, params...)
	if err != nil {
		return recs, err
	}
	if result != nil {
		err = recs.DeserializeAll(result)
	}
	return recs, err
}
func (db *Database) SQLCommand(result interface{}, sql string, params ...interface{}) (Records, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	defer db.pool.putConn(conn)
	recs, err := conn.SQLCommand(sql, params...)
	if err != nil {
		return recs, err
	}
	if result != nil {
		err = recs.DeserializeAll(result)
	}
	return recs, err
}

func (db *Database) ExecScript(result interface{}, lang ScriptLang, script string, params ...interface{}) (Records, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	defer db.pool.putConn(conn)
	recs, err := conn.ExecScript(lang, script, params...)
	if err != nil {
		return recs, err
	}
	if result != nil {
		err = recs.DeserializeAll(result)
	}
	return recs, err
}

func (db *Database) SQLQueryOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLQuery(result, nil, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}
func (db *Database) SQLCommandExpect(expected int, sql string, params ...interface{}) error {
	return checkExpected(expected)(db.SQLCommand(nil, sql, params...))
}
func (db *Database) SQLCommandOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLCommand(result, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}
func (db *Database) SQLBatch(result interface{}, sql string, params ...interface{}) (Records, error) {
	return db.ExecScript(result, LangSQL, sql, params...)
}
func (db *Database) SQLBatchExpect(expected int, sql string, params ...interface{}) error {
	return checkExpected(expected)(db.SQLBatch(nil, sql, params...))
}
func (db *Database) SQLBatchOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := db.SQLBatch(result, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}

func sqlEscape(s string) string { // TODO: escape things in a right way
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `"`, `\"`, -1)
	return `"` + s + `"`
}

func (db *Database) CreateScriptFunc(fnc Function) error {
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

func (db *Database) DeleteScriptFunc(name string) error {
	_, err := db.SQLCommand(nil, `DELETE FROM OFunction WHERE name = ?`, name)
	return err
}

func (db *Database) UpdateScriptFunc(name string, script string) error {
	_, err := db.SQLCommand(nil, `UPDATE OFunction SET code = ? WHERE name = ?`, script, name)
	return err
}

func (db *Database) CallScriptFunc(result interface{}, name string, params ...interface{}) (Records, error) {
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)

		sparams = append(sparams, string(data))
	}
	//return db.ExecScript(result, LangSQL, `SELECT `+name+`(`+strings.Join(sparams, ",")+`)`)
	return db.ExecScript(result, LangJS, fmt.Sprintf(`var out = %s(%s); (typeof(out) == "object" && out.toString() == "[object Object]" ? (new com.orientechnologies.orient.core.record.impl.ODocument()).fromJSON(JSON.stringify(out)) : out)`,
		name, strings.Join(sparams, ",")))
}

// CallScriptFuncJSON is a workaround for driver bug. It allow to return pure JS objects from DB functions.
//
// DEPRECATED. Use CallScriptFunc instead.
func (db *Database) CallScriptFuncJSON(result interface{}, name string, params ...interface{}) error {
	_, err := db.CallScriptFunc(result, name, params...)
	return err
}
func (db *Database) InitScriptFunc(fncs ...Function) (err error) {
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
