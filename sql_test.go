package orient_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/golang/glog"
	"github.com/istreamdata/orientgo"
	_ "github.com/istreamdata/orientgo/obinary"
	"github.com/istreamdata/orientgo/oschema"
	"strconv"
)

func TestSQLDriver(t *testing.T) {
	addr, rm := SpinOrientServer(t)
	defer rm()
	defer catch()

	dsn := dbUser + `@` + dbPass + `:` + addr + `/` + dbName

	{
		ndb, err := orient.DialDSN(dsn)
		Nil(t, err)
		SeedDB(t, ndb)
		ndb.Close()
	}

	// ---[ OPEN ]---
	db, err := sql.Open(orient.DriverNameSQL, dsn)
	Nil(t, err)
	defer db.Close()

	err = db.Ping()
	Nil(t, err)

	// ---[ DELETE #1 ]---
	// should not delete any rows
	delcmd := "delete from Cat where name ='Jared'"
	res, err := db.Exec(delcmd)
	Nil(t, err)
	nrows, _ := res.RowsAffected()
	glog.Infof(">> RES num rows affected: %v\n", nrows)
	Equals(t, int64(0), nrows)

	// ---[ INSERT #1 ]---
	// insert with no params
	insertSQL := "insert into Cat (name, age, caretaker) values('Jared', 11, 'The Subway Guy')"
	glog.Infoln(insertSQL, "=> 'Jared', 11, 'The Subway Guy'")
	res, err = db.Exec(insertSQL)
	Nil(t, err)

	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ := res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	// ---[ INSERT #2 ]---
	// insert with no params
	insertSQL = "insert into Cat (name, age, caretaker) values(?, ?, ?)"
	glog.Infoln(insertSQL, "=> 'Filo', 4, 'Greek'")
	res, err = db.Exec(insertSQL, "Filo", 4, "Greek")
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	// ---[ QUERY #1: QueryRow ]---
	// it is safe to query properties -> not sure how to return docs yet
	querySQL := "select name, age from Cat where caretaker = 'Greek'"
	row := db.QueryRow(querySQL)

	var retname string
	var retage int64
	err = row.Scan(&retname, &retage)
	Nil(t, err)
	Equals(t, "Filo", retname)
	Equals(t, int64(4), retage)

	// ---[ QUERY #2: Query (multiple rows returned) ]---

	querySQL = "select name, age, caretaker from Cat order by age"

	var rName, rCaretaker string
	var rAge int64

	names := make([]string, 0, 4)
	ctakers := make([]string, 0, 4)
	ages := make([]int64, 0, 4)
	rows, err := db.Query(querySQL)
	for rows.Next() {
		err = rows.Scan(&rName, &rAge, &rCaretaker)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 4, len(names))
	Equals(t, 4, len(ctakers))
	Equals(t, 4, len(ages))

	Equals(t, []string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals(t, []string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(t, int64(4), ages[0])
	Equals(t, int64(10), ages[1])
	Equals(t, int64(11), ages[2])
	Equals(t, int64(15), ages[3])

	// ---[ QUERY #3: Same Query as above but change property order ]---

	querySQL = "select age, caretaker, name from Cat order by age"

	names = make([]string, 0, 4)
	ctakers = make([]string, 0, 4)
	ages = make([]int64, 0, 4)
	rows, err = db.Query(querySQL)
	for rows.Next() {
		err = rows.Scan(&rAge, &rCaretaker, &rName)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 4, len(names))
	Equals(t, 4, len(ctakers))
	Equals(t, 4, len(ages))

	Equals(t, []string{"Filo", "Keiko", "Jared", "Linus"}, names)
	Equals(t, []string{"Greek", "Anna", "The Subway Guy", "Michael"}, ctakers)
	Equals(t, int64(4), ages[0])
	Equals(t, int64(10), ages[1])
	Equals(t, int64(11), ages[2])
	Equals(t, int64(15), ages[3])

	// ---[ QUERY #4: Property query using parameterized SQL ]---
	querySQL = "select caretaker, name, age from Cat where age >= ? order by age desc"

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	rows, err = db.Query(querySQL, "11")
	for rows.Next() {
		err = rows.Scan(&rCaretaker, &rName, &rAge)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	Equals(t, 2, len(names))
	Equals(t, "Linus", names[0])
	Equals(t, "Jared", names[1])

	Equals(t, 2, len(ctakers))
	Equals(t, "Michael", ctakers[0])
	Equals(t, "The Subway Guy", ctakers[1])

	Equals(t, 2, len(ages))
	Equals(t, int64(15), ages[0])
	Equals(t, int64(11), ages[1])

	// ---[ DELETE #2 ]---
	res, err = db.Exec(delcmd)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.Infof(">> DEL2 RES num rows affected: %v\n", nrows)
	Equals(t, int64(1), nrows)

	// ---[ DELETE #3 ]---
	res, err = db.Exec(delcmd)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.Infof(">> DEL3 RES num rows affected: %v\n", nrows)
	Equals(t, int64(0), nrows)

	// ---[ DELETE #4 ]---
	delcmd = "delete from Cat where name <> 'Linus' AND name <> 'Keiko'"
	res, err = db.Exec(delcmd)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.Infof(">> DEL4 RES num rows affected: %v\n", nrows)
	Equals(t, int64(1), nrows)

	// ---[ Full ODocument Queries with database/sql ]---
	// ---[ QueryRow ]---
	glog.Infoln(">>>>>>>>> QueryRow of full ODocument<<<<<<<<<<<")
	querySQL = "select from Cat where name = 'Linus'"

	row = db.QueryRow(querySQL)

	var retdoc oschema.ODocument
	err = row.Scan(&retdoc)
	Nil(t, err)
	Equals(t, "Cat", retdoc.Classname)
	Equals(t, 3, len(retdoc.FieldNames()))
	Equals(t, "Linus", retdoc.GetField("name").Value)
	Equals(t, int32(15), retdoc.GetField("age").Value)
	Equals(t, "Michael", retdoc.GetField("caretaker").Value)

	// ---[ Query (return multiple rows) ]---
	querySQL = "select from Cat order by caretaker desc"
	rows, err = db.Query(querySQL)
	rowdocs := make([]*oschema.ODocument, 0, 2)
	for rows.Next() {
		var newdoc oschema.ODocument
		err = rows.Scan(&newdoc)
		rowdocs = append(rowdocs, &newdoc)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 2, len(rowdocs))
	Equals(t, "Cat", rowdocs[0].Classname)
	Equals(t, "Linus", rowdocs[0].GetField("name").Value)
	Equals(t, "Keiko", rowdocs[1].GetField("name").Value)
	Equals(t, "Anna", rowdocs[1].GetField("caretaker").Value)
}

func TestSQLDriverPrepare(t *testing.T) {
	addr, rm := SpinOrientServer(t)
	defer rm()
	defer catch()

	dsn := dbUser + `@` + dbPass + `:` + addr + `/` + dbName

	{
		ndb, err := orient.DialDSN(dsn)
		Nil(t, err)
		SeedDB(t, ndb)
		ndb.Close()
	}

	// ---[ OPEN ]---
	db, err := sql.Open(orient.DriverNameSQL, dsn)
	Nil(t, err)
	defer db.Close()

	querySQL := "select caretaker, name, age from Cat where age >= ? order by age desc"

	stmt, err := db.Prepare(querySQL)
	Nil(t, err)
	defer stmt.Close()

	names := make([]string, 0, 2)
	ctakers := make([]string, 0, 2)
	ages := make([]int64, 0, 2)

	var (
		rCaretaker, rName string
		rAge              int64
	)

	// ---[ First use ]---
	rows, err := stmt.Query("10")
	Nil(t, err)
	for rows.Next() {
		err = rows.Scan(&rCaretaker, &rName, &rAge)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	Equals(t, 2, len(names))
	Equals(t, "Linus", names[0])
	Equals(t, "Keiko", names[1])

	Equals(t, 2, len(ctakers))
	Equals(t, "Michael", ctakers[0])
	Equals(t, "Anna", ctakers[1])

	Equals(t, 2, len(ages))
	Equals(t, int64(15), ages[0])
	Equals(t, int64(10), ages[1])

	// ---[ Second use ]---
	rows, err = stmt.Query("14")

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	for rows.Next() {
		err = rows.Scan(&rCaretaker, &rName, &rAge)
		names = append(names, rName)
		ctakers = append(ctakers, rCaretaker)
		ages = append(ages, rAge)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	Equals(t, 1, len(names))
	Equals(t, "Linus", names[0])
	Equals(t, int64(15), ages[0])
	Equals(t, "Michael", ctakers[0])

	// ---[ Third use ]---
	rows, err = stmt.Query("100")

	names = make([]string, 0, 2)
	ctakers = make([]string, 0, 2)
	ages = make([]int64, 0, 2)

	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	Equals(t, 0, len(names))
	Equals(t, 0, len(ages))
	Equals(t, 0, len(ctakers))

	stmt.Close()

	// ---[ Now prepare Command, not query ]---
	cmdStmt, err := db.Prepare("INSERT INTO Cat (age, caretaker, name) VALUES(?, ?, ?)")
	Nil(t, err)
	defer cmdStmt.Close()

	// use once
	result, err := cmdStmt.Exec(1, "Ralph", "Max")
	Nil(t, err)
	nrows, err := result.RowsAffected()
	Nil(t, err)
	Equals(t, 1, int(nrows))
	insertID, err := result.LastInsertId()
	Nil(t, err)
	True(t, int(insertID) >= 0, "insertId was: "+strconv.Itoa(int(insertID)))

	// use again
	result, err = cmdStmt.Exec(2, "Jimmy", "John")
	Nil(t, err)
	nrows, err = result.RowsAffected()
	Nil(t, err)
	Equals(t, 1, int(nrows))
	insertID2, err := result.LastInsertId()
	Nil(t, err)
	True(t, insertID != insertID2, "insertID was: "+strconv.Itoa(int(insertID)))

	row := db.QueryRow("select count(*) from Cat")
	var cnt int64
	err = row.Scan(&cnt)
	Nil(t, err)
	Equals(t, 4, int(cnt))

	cmdStmt.Close()

	// ---[ Prepare DELETE command ]---
	delStmt, err := db.Prepare("DELETE from Cat where name = ? OR caretaker = ?")
	Nil(t, err)
	defer delStmt.Close()
	result, err = delStmt.Exec("Max", "Jimmy")
	Nil(t, err)
	nrows, err = result.RowsAffected()
	Nil(t, err)
	Equals(t, 2, int(nrows))
	insertID3, err := result.LastInsertId()
	Nil(t, err)
	True(t, int(insertID3) < 0, "should have negative insertId for a DELETE")

}

func TestSQlDriverGraph(t *testing.T) {
	addr, rm := SpinOrientServer(t)
	defer rm()
	defer catch()

	dsn := dbUser + `@` + dbPass + `:` + addr + `/` + dbName

	{
		ndb, err := orient.DialDSN(dsn)
		Nil(t, err)
		SeedDB(t, ndb)
		err = ndb.Command(orient.NewSQLCommand(`CREATE CLASS Person extends V`)).Err()
		Nil(t, err)
		err = ndb.Command(orient.NewSQLCommand(`CREATE CLASS Friend extends E`)).Err()
		Nil(t, err)
		ndb.Close()
	}

	// ---[ OPEN ]---
	db, err := sql.Open(orient.DriverNameSQL, dsn)
	Nil(t, err)
	defer db.Close()

	err = db.Ping()
	Nil(t, err)

	insertSQL := "insert into Person SET firstName='Joe', lastName='Namath'"
	res, err := db.Exec(insertSQL)
	Nil(t, err)

	nrows, _ := res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ := res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	//True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID)) // TODO: fix

	createVtxSQL := `CREATE VERTEX Person SET firstName = 'Terry', lastName = 'Bradshaw'`
	res, err = db.Exec(createVtxSQL)
	Nil(t, err)

	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

	sql := `CREATE EDGE Friend FROM
            (SELECT FROM Person where firstName = 'Joe' AND lastName = 'Namath')
            TO
            (SELECT FROM Person where firstName = 'Terry' AND lastName = 'Bradshaw')`
	res, err = db.Exec(sql)
	Nil(t, err)
	nrows, _ = res.RowsAffected()
	glog.V(10).Infof("nrows: %v\n", nrows)
	lastID, _ = res.LastInsertId()
	glog.V(10).Infof("last insert id: %v\n", lastID)
	Equals(t, int64(1), nrows)
	//True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID)) // TODO: fix

	sql = `select from Friend order by @rid desc LIMIT 1`
	rows, err := db.Query(sql)
	rowdocs := make([]*oschema.ODocument, 0, 1)
	for rows.Next() {
		var newdoc oschema.ODocument
		err = rows.Scan(&newdoc)
		rowdocs = append(rowdocs, &newdoc)
	}
	err = rows.Err()
	Nil(t, err)

	Equals(t, 1, len(rowdocs))
	Equals(t, "Friend", rowdocs[0].Classname)
	friendOutLink := rowdocs[0].GetField("out").Value.(oschema.OIdentifiable)
	True(t, friendOutLink.GetRecord() == nil, "should be nil")

	glog.V(10).Infof("friendOutLink: %v\n", friendOutLink)

	// REMOVE THE STUFF BELOW since can't specify fetchPlain in SQL (??? => ask on user group)'
	// sql = `select from Friend order by @rid desc LIMIT 1 fetchPlan=*:-1`
	// rows, err = db.Query(sql)
	// rowdocs = make([]*oschema.ODocument, 0, 1)
	// for rows.Next() {
	// 	var newdoc oschema.ODocument
	// 	err = rows.Scan(&newdoc)
	// 	rowdocs = append(rowdocs, &newdoc)
	// }
	// err = rows.Err()
	// Nil(t, err)

	// Equals(t, 1, len(rowdocs))
	// Equals(t, "Friend", rowdocs[0].Classname)
	// friendOutLink = rowdocs[0].GetField("out").Value.(*oschema.OLink)
	// // True(t, friendOutLink.Record != nil, "should NOT be nil") // FAILS: looks like you cannot put a fetchplain in an SQL query itself?

	// nrows, _ = res.RowsAffected()
	// glog.V(10).Infof("nrows: %v\n", nrows)
	// lastID, _ = res.LastInsertId()
	// glog.V(10).Infof("last insert id: %v\n", lastID)
	// Equals(t, int64(1), nrows)
	// True(t, lastID > int64(0), fmt.Sprintf("LastInsertId: %v", lastID))

}
