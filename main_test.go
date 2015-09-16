package main

import (
	"os"
	"testing"

	"gopkg.in/istreamdata/orientgo.v1/constants"
	"gopkg.in/istreamdata/orientgo.v1/obinary"
)

var DocumentDBSeeds = []string{
	"CREATE CLASS Animal",
	"CREATE property Animal.name string",
	"CREATE property Animal.age integer",
	"CREATE CLASS Cat extends Animal",
	"CREATE property Cat.caretaker string",
	"INSERT INTO Cat (name, age, caretaker) VALUES ('Linus', 15, 'Michael'), ('Keiko', 10, 'Anna')",
}

func SetUp(db *obinary.DBClient) {
	if err := obinary.CreateDatabase(db, dbDocumentName, constants.DocumentDB, constants.Persistent); err != nil {
		panic(err)
	}
	if err := obinary.CreateDatabase(db, dbGraphName, constants.GraphDB, constants.Persistent); err != nil {
		panic(err)
	}
}

func TearDown(db *obinary.DBClient) {
	obinary.DropDatabase(db, dbDocumentName, constants.DocumentDB)
	obinary.DropDatabase(db, dbGraphName, constants.GraphDB)
}

func Seed() {
	db := ConnectToDocumentDatabase()
	defer db.Close()

	var err error

	for _, seed := range DocumentDBSeeds {
		if _, _, err = obinary.SQLCommand(db, seed); err != nil {
			panic(err) // Programming error
		}
	}
}

func CleanupSeed() {
	documentDB := ConnectToDocumentDatabase()
	defer documentDB.Close()
	obinary.SQLCommand(documentDB, "TRUNCATE Animal")
	obinary.SQLCommand(documentDB, "TRUNCATE Cat")

	graphDB := ConnectToGraphDatabase()
	defer graphDB.Close()
	obinary.SQLCommand(graphDB, "TRUNCATE Person")
	obinary.SQLCommand(graphDB, "TRUNCATE Friend")
}

// Runs before all tests
func TestMain(m *testing.M) {
	db, _ := NewDB()
	defer db.Close()

	// Test DB connection
	if err := obinary.ConnectToServer(db, dbUser, dbPass); err != nil {
		panic(err)
	}
	// Clean up tests database
	TearDown(db)

	// Set up test graph & document databases
	SetUp(db)

	Seed()

	// Run Tests
	ret := m.Run()

	// Clean up tests database
	TearDown(db)
	os.Exit(ret)
}
