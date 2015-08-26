package main

import (
	"testing"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary"
)

func NewDB() (*obinary.DBClient, error) {
	d, err := obinary.NewDBClient(obinary.ClientOptions{})
	if err != nil {
		return nil, err
	}
	return d, nil
}

func TestNewDB(t *testing.T) {
	db, err := NewDB()
	defer db.Close()
	if err != nil {
		t.Fatalf("Unexpected erorr %s", err)
	}
}

func TestDBConnectionToServer(t *testing.T) {
	db, _ := NewDB()
	defer db.Close()
	if err := obinary.ConnectToServer(db, dbUser, dbPass); err != nil {
		t.Fatal("Connection to database failed")
	}
}

func ConnectToGraphDatabase() *obinary.DBClient {
	db, _ := NewDB()
	obinary.OpenDatabase(db, dbGraphName, constants.GraphDB, "admin", "admin")
	return db
}

func ConnectToDocumentDatabase() *obinary.DBClient {
	db, _ := NewDB()
	obinary.OpenDatabase(db, dbDocumentName, constants.DocumentDB, "admin", "admin")
	return db
}
