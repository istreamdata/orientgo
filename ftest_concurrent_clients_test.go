package orient_test

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/istreamdata/orientgo"
	"testing"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func TestConcurrentClients(t *testing.T) {
	const N = 5
	db, closer := SpinOrientAndOpenDB(t)
	defer closer()
	SeedDB(t, db)

	// ---[ queries and insertions concurrently ]---

	var wg sync.WaitGroup

	sql := `select count(*) from Cat where caretaker like 'Eva%'`
	recs, err := db.SQLQuery(nil, nil, sql)
	Nil(t, err)
	docs, err := recs.AsDocuments()
	Nil(t, err)
	beforeCount := toInt(docs[0].GetField("count").Value)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doQueriesAndInsertions(t, db, i)
		}(i)
	}

	wg.Wait()

	sql = `select count(*) from Cat where caretaker like 'Eva%'`
	recs, err = db.SQLQuery(nil, nil, sql)
	Nil(t, err)
	docs, err = recs.AsDocuments()
	Nil(t, err)
	afterCount := toInt(docs[0].GetField("count").Value)
	Equals(t, beforeCount, afterCount)
}

func doQueriesAndInsertions(t *testing.T, db orient.Database, id int) {
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
			recs, err := db.SQLCommand(nil, sql)
			Nil(t, err)
			docs, err := recs.AsDocuments()
			Nil(t, err)
			Equals(t, 1, len(docs))
			ridsToDelete = append(ridsToDelete, docs[0].RID.String())
		} else {
			sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
			recs, err := db.SQLQuery(nil, nil, sql)
			Nil(t, err)
			docs, err := recs.AsDocuments()
			Nil(t, err)
			Equals(t, toInt(docs[0].GetField("count").Value), len(ridsToDelete))
		}
	}

	//t.Logf("records insert by goroutine %d: %v", id, len(ridsToDelete))

	// ---[ clean up ]---

	for _, rid := range ridsToDelete {
		_, err := db.SQLCommand(nil, `delete from Cat where @rid=`+rid)
		Nil(t, err)
	}
	sql := fmt.Sprintf(`select count(*) from Cat where caretaker="Eva%d"`, id)
	recs, err := db.SQLQuery(nil, nil, sql)
	Nil(t, err)
	docs, err := recs.AsDocuments()
	Nil(t, err)
	Equals(t, toInt(docs[0].GetField("count").Value), 0)
}
