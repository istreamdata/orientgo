package orient_test

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"testing"
	"time"

	"github.com/fsouza/go-dockerclient"
	"gopkg.in/istreamdata/orientgo.v2"
	_ "gopkg.in/istreamdata/orientgo.v2/obinary"
)

var orientVersion = "2.1"

const (
	dbName = "default"
	dbUser = "admin"
	dbPass = "admin"

	srvUser = "root"
	srvPass = "root"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	if vers := os.Getenv("ORIENT_VERS"); vers != "" {
		orientVersion = vers
	}
	fmt.Printf("Testing against OrientDB %s\n", orientVersion)
	go func() {
		fmt.Println("pprof: ", http.ListenAndServe(":6060", nil))
	}()
}

func TestBuild(t *testing.T) {

}

func TestNewDB(t *testing.T) {
	_, closer := SpinOrient(t)
	defer closer()
}

func TestDBAuth(t *testing.T) {
	db, closer := SpinOrient(t)
	defer closer()
	if _, err := db.Auth(srvUser, srvPass); err != nil {
		t.Fatal("Connection to database failed")
	}
}

func TestDBAuthWrong(t *testing.T) {
	db, closer := SpinOrient(t)
	defer closer()
	if _, err := db.Auth(srvUser, srvPass+"pass"); err == nil {
		t.Fatal("auth error expected")
	}
}

func SpinOrientServer(t *testing.T) (string, func()) {
	const port = 2424

	dport_api := docker.Port("2424/tcp")
	dport_web := docker.Port("2480/tcp")
	binds := make(map[docker.Port][]docker.PortBinding)
	//	binds[dport_api] = []docker.PortBinding{docker.PortBinding{HostPort: fmt.Sprint(port)}}
	//	binds[dport_web] = []docker.PortBinding{docker.PortBinding{HostPort: fmt.Sprint(port + 1)}}

	cl, err := docker.NewClient("unix:///var/run/docker.sock")
	if err != nil {
		t.Skip(err)
	}
	cont, err := cl.CreateContainer(docker.CreateContainerOptions{
		Config: &docker.Config{
			OpenStdin: true, Tty: true,
			ExposedPorts: map[docker.Port]struct{}{dport_api: struct{}{}, dport_web: struct{}{}},
			Image:        `dennwc/orientdb:` + orientVersion,
		}, HostConfig: &docker.HostConfig{
			PortBindings: binds,
		},
	})
	if err != nil {
		t.Skip(err)
	}

	rm := func() {
		cl.RemoveContainer(docker.RemoveContainerOptions{ID: cont.ID, Force: true})
	}

	if err := cl.StartContainer(cont.ID, &docker.HostConfig{PortBindings: binds}); err != nil {
		rm()
		t.Skip(err)
	}

	info, err := cl.InspectContainer(cont.ID)
	if err != nil {
		rm()
		t.Skip(err)
	}
	{
		start := time.Now()
		ok := false
		for time.Since(start) < time.Second*5 {
			conn, err := net.DialTimeout("tcp", net.JoinHostPort(info.NetworkSettings.IPAddress, fmt.Sprint(port)), time.Second/4)
			if err == nil {
				ok = true
				conn.Close()
				break
			}
		}
		if !ok {
			rm()
			t.Fatal("Orient container did not come up in time")
		}
	}

	return fmt.Sprintf("%s:%d", info.NetworkSettings.IPAddress, port), rm
}

func SpinOrient(t *testing.T) (*orient.Client, func()) {
	addr, rm := SpinOrientServer(t)
	cli, err := orient.Dial(addr)
	if err != nil {
		rm()
		t.Fatal(err)
	}
	return cli, func() {
		cli.Close()
		rm()
	}
}

func SpinOrientAndOpenDB(t *testing.T, graph bool) (*orient.Database, func()) {
	cli, closer := SpinOrient(t)
	tp := orient.DocumentDB
	if graph {
		tp = orient.GraphDB
	}
	db, err := cli.Open(dbName, tp, dbUser, dbPass)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return db, closer
}

var DocumentDBSeeds = []string{
	"CREATE CLASS Animal",
	"CREATE property Animal.name string",
	"CREATE property Animal.age integer",
	"CREATE CLASS Cat extends Animal",
	"CREATE property Cat.caretaker string",
	"INSERT INTO Cat (name, age, caretaker) VALUES ('Linus', 15, 'Michael'), ('Keiko', 10, 'Anna')",
}

func SeedDB(t *testing.T, db *orient.Database) {
	for _, seed := range DocumentDBSeeds {
		if err := db.Command(orient.NewSQLCommand(seed)).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func testOUserCommand(t *testing.T, fnc func(*orient.Database) orient.Results) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()

	var docs []orient.OIdentifiable
	err := fnc(cli).All(&docs)
	if err != nil {
		t.Fatal(err)
	} else if len(docs) != 3 {
		t.Error("wrong docs count")
	}
	//t.Logf("docs[%d]: %+v", len(docs), docs)
}

func TestSelect(t *testing.T) {
	testOUserCommand(t, func(cli *orient.Database) orient.Results {
		if orientVersion < "2.1" {
			return cli.Command(orient.NewSQLQuery("SELECT FROM OUser LIMIT 3"))
		}
		return cli.Command(orient.NewSQLQuery("SELECT FROM OUser LIMIT ?", 3))
	})
}

func TestSelectCommand(t *testing.T) {
	testOUserCommand(t, func(cli *orient.Database) orient.Results {
		if orientVersion < "2.1" {
			return cli.Command(orient.NewSQLCommand("SELECT FROM OUser LIMIT 3"))
		}
		return cli.Command(orient.NewSQLCommand("SELECT FROM OUser LIMIT ?", 3))
	})
}

func TestSelectScript(t *testing.T) {
	testOUserCommand(t, func(cli *orient.Database) orient.Results {
		return cli.Command(orient.NewScriptCommand(orient.LangSQL, "SELECT FROM OUser"))
	})
}

func TestSelectScriptJS(t *testing.T) {
	testOUserCommand(t, func(cli *orient.Database) orient.Results {
		return cli.Command(orient.NewScriptCommand(orient.LangJS, `var docs = db.query('SELECT FROM OUser'); docs`))
	})
}

func TestSelectSaveFunc(t *testing.T) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()

	name := "tempFuncOne"
	code := `
	var param = one
	if (param != "some") {
		response.send(500, "err", "text/plain", "err" )
	}
	if (typeof(two) != "object") {
		response.send(500, "err2", "text/plain", "err2" )
	} else if (two.Name != "one") {
		response.send(500, "err3", "text/plain", "err3" )
	}
	var unused = "\\"
	var tbl = 'OUser'
	var docs = db.query("SELECT FROM "+tbl)
	return docs
	`
	if err := cli.CreateScriptFunc(orient.Function{
		Name: name, Code: code, Idemp: false,
		Lang: orient.LangJS, Params: []string{"one", "two"},
	}); err != nil {
		t.Fatal(err)
	}

	var fnc []struct {
		Name string
		Code string
	}
	if err := cli.Command(orient.NewSQLQuery("SELECT FROM OFunction")).All(&fnc); err != nil {
		t.Fatal(err)
	} else if len(fnc) != 1 {
		t.Fatal("wrong func count")
	} else if fnc[0].Name != name {
		t.Fatalf("wrong func name: '%v' vs '%v'", fnc[0].Name, name)
	} else if fnc[0].Code != code {
		t.Fatal(fmt.Errorf("wrong func code:\n\n%s\nvs\n%s\n", fnc[0].Code, code))
	}

	var o interface{}
	err := cli.CallScriptFunc(name, "some", struct{ Name string }{"one"}).All(&o)
	if err != nil {
		t.Fatal(err)
	} else if docs, ok := o.([]orient.OIdentifiable); !ok {
		t.Errorf("expected list, got: %T", o)
	} else if len(docs) != 3 {
		t.Error("wrong docs count")
	}
	//t.Logf("docs[%d]: %+v", len(recs), recs)
}

func TestSelectSaveFunc2(t *testing.T) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()

	name := "tempFuncTwo"
	code := `return {"params": [one, two]}`
	if err := cli.CreateScriptFunc(orient.Function{
		Name: name, Code: code, Idemp: false,
		Lang: orient.LangJS, Params: []string{"one", "two"},
	}); err != nil {
		t.Fatal(err)
	}

	var fnc []struct {
		Name string
		Code string
	}
	if err := cli.Command(orient.NewSQLQuery("SELECT FROM OFunction")).All(&fnc); err != nil {
		t.Fatal(err)
	} else if len(fnc) != 1 {
		t.Fatal("wrong func count")
	} else if fnc[0].Name != name {
		t.Fatal("wrong func name")
	} else if fnc[0].Code != code {
		t.Fatal(fmt.Errorf("wrong func code:\n\n%s\nvs\n%s\n", fnc[0].Code, code))
	}

	var res struct {
		Params []string
	}
	err := cli.CallScriptFunc(name, "some", "one").All(&res)
	if err != nil {
		t.Fatal(err)
	} else if len(res.Params) != 2 {
		t.Error("wrong result count")
	}
}

func TestSelectSaveFuncResult(t *testing.T) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()

	name := "tempFuncOne"
	code := `return {"name":"ori","props":{"data":"ok","num":10,"custom":one}}`
	if err := cli.CreateScriptFunc(orient.Function{
		Name: name, Code: code, Idemp: false,
		Lang: orient.LangJS, Params: []string{"one"},
	}); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Name  string
		Props map[string]interface{}
	}
	err := cli.CallScriptFunc(name, "some").All(&result)
	if err != nil {
		t.Fatal(err)
	} else if result.Name != "ori" {
		t.Fatal("wrong object name property")
	} else if len(result.Props) == 0 {
		t.Fatal("empty object props")
	}
	//t.Logf("doc: %+v", result)
}

func TestScriptParams(t *testing.T) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()

	name := "tempFuncOne"
	code := `return {"aaa": one, "bbb": two}`
	if err := cli.CreateScriptFunc(orient.Function{
		Name: name, Code: code, Idemp: false,
		Lang: orient.LangJS, Params: []string{"one", "two"},
	}); err != nil {
		t.Fatal(err)
	}
	var o map[string]interface{}
	err := cli.CallScriptFunc(name, map[string]string{"one": "first"}, "two").All(&o)
	if err != nil {
		t.Fatal(err)
	} else if len(o) != 2 {
		t.Fatal("wrong map leng")
	} else if av, ok := o["aaa"]; !ok {
		t.Fatal("'a' value not found")
	} else if bv, ok := o["bbb"]; !ok {
		t.Fatal("'b' value not found")
	} else if am, ok := av.(map[string]string); !ok {
		t.Fatal("wrong type for 'a' value")
	} else if len(am) != 1 {
		t.Fatal("wrong value for 'a'") // TODO: check data
	} else if bs, ok := bv.(string); !ok {
		t.Fatal("wrong type for 'b' value")
	} else if bs != "two" {
		t.Fatal("wrong value for 'b'")
	}
	t.Logf("%+v(%T)\n", o, o)
}

func TestScriptJSMap(t *testing.T) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()

	var o []orient.OIdentifiable
	err := cli.Command(orient.NewScriptCommand(orient.LangJS, `var a = {"aaa":"one","bbb": 2}; a`)).All(&o)
	if err != nil {
		t.Fatal(err)
	} else if len(o) != 1 {
		t.Skipf("wrong array leng: %+v(%d)", o, len(o))
	} else if o[0] == nil {
		t.Skipf("nil record: %+v(%T)", o, o)
	}
}

func TestCommandStringQuotes(t *testing.T) {
	cli, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	const name = `Para Que Peliar "Besame"`
	doc := orient.NewEmptyDocument()
	doc.SetField("name", name)
	doc.SetClassNameIfExists("V")
	err := cli.CreateRecord(doc)
	if err != nil {
		t.Fatal(err)
	}
	var odoc *orient.Document
	err = cli.Command(orient.NewSQLQuery(`SELECT FROM V WHERE name = ?`, name)).All(&odoc)
	if err != nil {
		t.Fatal(err)
	} else if val := odoc.GetField("name").Value.(string); val != name {
		t.Fatalf("strings are different: %v vs %v", val, name)
	}
}

func TestSQLQueryParams(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	var doc *orient.Document
	err := db.Command(orient.NewSQLQuery(`SELECT FROM Cat WHERE name=? AND age=?`, "Linus", 15)).All(&doc)
	if err != nil {
		t.Fatal(err)
	} else if doc.GetField("name").Value.(string) != "Linus" {
		t.Fatal("wrong field value")
	}
}

func TestSQLCommandParams(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	var doc *orient.Document
	err := db.Command(orient.NewSQLCommand(`SELECT FROM Cat WHERE name=? AND age=?`, "Linus", 15)).All(&doc)
	if err != nil {
		t.Fatal(err)
	} else if doc.GetField("name").Value.(string) != "Linus" {
		t.Fatal("wrong field value")
	}
}

func TestSQLCommandParamsCustomType(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	type Age int
	type Name string

	var doc *orient.Document
	err := db.Command(orient.NewSQLCommand(`SELECT FROM Cat WHERE name=? AND age=?`, Name("Linus"), Age(15))).All(&doc)
	if err != nil {
		t.Fatal(err)
	} else if doc.GetField("name").Value.(string) != "Linus" {
		t.Fatal("wrong field value")
	} else if doc.GetField("age").Value.(int32) != 15 {
		t.Fatal("wrong field value")
	}

	var cat *struct {
		Name Name
		Age  Age
	}
	err = db.Command(orient.NewSQLCommand(`SELECT FROM Cat WHERE name=? AND age=?`, Name("Linus"), Age(15))).All(&cat)
	if err != nil {
		t.Fatal(err)
	} else if cat.Name != "Linus" {
		t.Fatal("wrong field value")
	} else if cat.Age != 15 {
		t.Fatal("wrong field value")
	}
}

func TestSQLInnerStruct(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	type Inner struct {
		Name string
	}
	type Item struct {
		One   Inner
		Inner []Inner
	}

	one, two := Inner{Name: "one"}, Inner{Name: "two"}

	err := db.Command(orient.NewSQLCommand(`CREATE VERTEX V CONTENT ` + orient.MarshalContent(Item{One: one, Inner: []Inner{one, two}}))).Err()
	if err != nil {
		t.Fatal(err)
	}
	var obj *Item
	err = db.Command(orient.NewSQLQuery(`SELECT FROM V WHERE One IS NOT NULL`)).All(&obj)
	if err != nil {
		t.Fatal(err)
	} else if obj.One != one {
		t.Fatal("wrong value")
	} else if len(obj.Inner) != 2 || obj.Inner[0] != one || obj.Inner[1] != two {
		t.Fatal("wrong list value")
	}
}

func TestNativeInnerStruct(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	type Inner struct {
		Name string
	}
	type Item struct {
		One   Inner
		Inner []Inner
	}

	one, two := Inner{Name: "one"}, Inner{Name: "two"}

	doc := orient.NewDocument("V")
	err := doc.From(Item{One: one, Inner: []Inner{one, two}})
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateRecord(doc)
	if err != nil {
		t.Fatal(err)
	}

	var obj *Item
	err = db.Command(orient.NewSQLQuery(`SELECT FROM V WHERE One IS NOT NULL`)).All(&obj)
	if err != nil {
		t.Fatal(err)
	} else if obj.One != one {
		t.Fatal("wrong value")
	} else if len(obj.Inner) != 2 || obj.Inner[0] != one || obj.Inner[1] != two {
		t.Fatal("wrong list value")
	}
}

func TestSQLBatchParams(t *testing.T) {
	notShort(t)
	db, closer := SpinOrientAndOpenDB(t, false)
	defer closer()
	defer catch()
	SeedDB(t, db)

	var doc *orient.Document
	err := db.Command(orient.NewScriptCommand(orient.LangSQL, `
	LET cat = SELECT FROM Cat WHERE name=? AND age=?
	RETURN $cat
	`, "Linus", 15)).All(&doc)
	if err != nil {
		t.Fatal(err)
	} else if doc.GetField("name").Value.(string) != "Linus" {
		t.Fatal("wrong field value")
	}
}
