package orient_test

/*
import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/dyy18/orientgo/constants"
	"github.com/dyy18/orientgo/obinary"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func testConcurrentClients() {
	var (
		dbc1, dbc2, dbc3, dbc4 *obinary.DBClient
		err                    error
	)

	const nclients = 4

	runtime.GOMAXPROCS(nclients)

	dbc1, err = obinary.NewDBClient(obinary.ClientOptions{})
	assert.Nil(t, err)
	defer dbc1.Close()

	dbc2, err = obinary.NewDBClient(obinary.ClientOptions{})
	assert.Nil(t, err)
	defer dbc2.Close()

	dbc3, err = obinary.NewDBClient(obinary.ClientOptions{})
	assert.Nil(t, err)
	defer dbc3.Close()

	dbc4, err = obinary.NewDBClient(obinary.ClientOptions{})
	assert.Nil(t, err)
	defer dbc4.Close()

	err = obinary.OpenDatabase(dbc1, dbDocumentName, constants.DocumentDB, "admin", "admin")
	assert.Nil(t, err)
	defer obinary.CloseDatabase(dbc1)

	err = obinary.OpenDatabase(dbc2, dbDocumentName, constants.DocumentDB, "admin", "admin")
	assert.Nil(t, err)
	defer obinary.CloseDatabase(dbc2)

	err = obinary.OpenDatabase(dbc3, dbDocumentName, constants.DocumentDB, "admin", "admin")
	assert.Nil(t, err)
	defer obinary.CloseDatabase(dbc3)

	err = obinary.OpenDatabase(dbc4, dbDocumentName, constants.DocumentDB, "admin", "admin")
	assert.Nil(t, err)
	defer obinary.CloseDatabase(dbc4)

	// ---[ queries and insertions concurrently ]---

	var wg sync.WaitGroup
	wg.Add(nclients)

	sql := `select count(*) from Cat where caretaker like 'Eva%'`
	docs, err := obinary.SQLQuery(dbc1, sql, "")
	assert.Nil(t, err)
	beforeCount := toInt(docs[0].GetField("count").Value)

	go doQueriesAndInsertions(&wg, dbc1, 1)
	go doQueriesAndInsertions(&wg, dbc2, 2)
	go doQueriesAndInsertions(&wg, dbc3, 3)
	go doQueriesAndInsertions(&wg, dbc4, 4)

	wg.Wait()

	sql = `select count(*) from Cat where caretaker like 'Eva%'`
	docs, err = obinary.SQLQuery(dbc1, sql, "")
	assert.Nil(t, err)
	afterCount := toInt(docs[0].GetField("count").Value)
	Equals(t, beforeCount, afterCount)

	fmt.Println(afterCount)
}

func doQueriesAndInsertions(wg *sync.WaitGroup, dbc *obinary.DBClient, id int) {
	defer wg.Done()

	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	nreps := 1000
	ridsToDelete := make([]string, 0, nreps)

	for i := 0; i < nreps; i++ {
		randInt := rnd.Intn(3)
		if randInt > 0 {
			time.Sleep(time.Duration(randInt) * time.Millisecond)
		}

		if (i+randInt)%2 == 0 {
			sql := fmt.Sprintf(`insert into Cat set name="Bar", age=%d, caretaker="Eva%d"`, 20+id, id)
			_, docs, err := obinary.SQLCommand(dbc, sql)
			assert.Nil(t, err)
			Equals(t, 1, len(docs))
			ridsToDelete = append(ridsToDelete, docs[0].RID.String())

		} else {
			sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
			docs, err := obinary.SQLQuery(dbc, sql, "")
			assert.Nil(t, err)
			Equals(t, toInt(docs[0].GetField("count").Value), len(ridsToDelete))
		}
	}

	fmt.Printf("records insert by goroutine %d: %v\n", id, len(ridsToDelete))

	// ---[ clean up ]---

	for _, rid := range ridsToDelete {
		_, _, err := obinary.SQLCommand(dbc, `delete from Cat where @rid=`+rid)
		assert.Nil(t, err)
	}
	sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
	docs, err := obinary.SQLQuery(dbc, sql, "")
	assert.Nil(t, err)
	Equals(t, toInt(docs[0].GetField("count").Value), 0)
}
*/
