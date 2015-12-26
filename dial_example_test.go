package orient_test

import (
	"testing"

	"gopkg.in/istreamdata/orientgo.v2"
	_ "gopkg.in/istreamdata/orientgo.v2/obinary"
)

// Dial example
func TestDial(t *testing.T) {
	addr, _ := SpinOrientServer(t)
	testDbName := "test_db_example"
	testDbUser := "root"
	testDbPass := "root"

	client, err := orient.Dial(addr)
	if err != nil {
		panic(err)
	}

	admin, err := client.Auth(testDbUser, testDbPass)
	if err != nil {
		panic(err)
	}

	// There are 2 options
	// 1. orient.Persistent - represents on-disk database
	// 2. orient.Volatile - represents in-memory database
	ok, err := admin.DatabaseExists(testDbName, orient.Persistent)
	if err != nil {
		panic(err)
	}

	// If database does not exist let's create it
	if !ok {
		// There are 2 options
		// 1. orient.GraphDB - graph database
		// 2. orient.DocumentDB - document database
		err = admin.CreateDatabase(testDbName, orient.GraphDB, orient.Persistent)
		if err != nil {
			panic(err)
		}
	}

	// Open wanted database & operate further
	database, err := client.Open(testDbName, orient.GraphDB, testDbUser, testDbPass)
	if err != nil {
		panic(err)
	}
	defer database.Close()
}
