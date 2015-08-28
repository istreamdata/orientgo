package obinary_test

import (
	//"os"
	"testing"

	"github.com/dyy18/orientgo/constants"
	"github.com/dyy18/orientgo/obinary"
)

func TestNewDB(t *testing.T) {
	_, closer := SpinOrient(t)
	defer closer()
}

func TestDBConnectionToServer(t *testing.T) {
	db, closer := SpinOrient(t)
	defer closer()
	if err := db.ConnectToServer(dbUser, dbPass); err != nil {
		t.Fatal("Connection to database failed")
	}
}

func ConnectToGraphDatabase(t *testing.T) (*obinary.Client, func()) {
	db, closer := SpinOrient(t)
	if err := db.OpenDatabase(dbGraphName, constants.GraphDB, "admin", "admin"); err != nil {
		closer()
		t.Fatal(err)
	}
	return db, closer
}

func ConnectToDocumentDatabase(t *testing.T) (*obinary.Client, func()) {
	db, closer := SpinOrient(t)
	if err := db.OpenDatabase(dbDocumentName, constants.DocumentDB, "admin", "admin"); err != nil {
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

func SetUp(db *obinary.Client) {
	if err := db.CreateDatabase(dbDocumentName, constants.DocumentDB, constants.Persistent); err != nil {
		panic(err)
	}
	if err := db.CreateDatabase(dbGraphName, constants.GraphDB, constants.Persistent); err != nil {
		panic(err)
	}
}

func TearDown(db *obinary.Client) {
	db.DropDatabase(dbDocumentName, constants.Persistent)
	db.DropDatabase(dbGraphName, constants.Persistent)
}

func Seed(t *testing.T) {
	db, closer := ConnectToDocumentDatabase(t)
	defer closer()

	var err error

	for _, seed := range DocumentDBSeeds {
		if _, err = db.SQLCommand(nil, seed); err != nil {
			panic(err) // Programming error
		}
	}
}

func CleanupSeed(t *testing.T) {
	documentDB, closer := ConnectToDocumentDatabase(t)
	defer closer()
	documentDB.SQLCommand(nil, "TRUNCATE Animal")
	documentDB.SQLCommand(nil, "TRUNCATE Cat")

	graphDB, closer := ConnectToGraphDatabase(t)
	defer closer()
	graphDB.SQLCommand(nil, "TRUNCATE Person")
	graphDB.SQLCommand(nil, "TRUNCATE Friend")
}

// Runs before all tests
//func TestMain(m *testing.M) {
//	db, err := NewDB()
//	if err != nil {
//		panic(err)
//	}
//	defer db.Close()
//
//	// Test DB connection
//	if err := db.ConnectToServer(dbUser, dbPass); err != nil {
//		panic(err)
//	}
//	// Clean up tests database
//	TearDown(db)
//
//	// Set up test graph & document databases
//	SetUp(db)
//
//	Seed()
//
//	// Run Tests
//	ret := m.Run()
//
//	// Clean up tests database
//	TearDown(db)
//	os.Exit(ret)
//}
