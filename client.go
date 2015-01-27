package main

import (
	"fmt"
	"log"
	"ogo/obinary"
	"os"
)

func main() {
	var (
		dbc *obinary.DbClient
		err error
	)
	fmt.Println("ConnectToServer")
	dbc, err = obinary.NewDbClient(obinary.ClientOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer dbc.Close()

	fmt.Println("OpenDatabase")
	err = obinary.OpenDatabase(dbc, "cars", obinary.DocumentDbType, "admin", "admin")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", dbc) // DEBUG

	size, err := obinary.GetDatabaseSize(dbc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return
	}
	fmt.Printf("cars database size: %v\n", size)
	obinary.CloseDatabase(dbc)

	fmt.Println("-------- server commands --------")

	err = obinary.CreateServerSession(dbc, "root", "A406A900E578DC7094FBA78001A45BE611DB06B25F593026CCDF737A31A9D0E9")
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN s1: %v\n", err)
		return
	}

	var status bool
	status, err = obinary.DatabaseExists(dbc, "cars", obinary.PersistentStorageType)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("`cars` persistent db exists?: %v\n", status)

	status, err = obinary.DatabaseExists(dbc, "cars", obinary.VolatileStorageType)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("`cars` volatile db exists?: %v\n", status)

	status, err = obinary.DatabaseExists(dbc, "foobar", obinary.PersistentStorageType)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("`foobar` volatile db exists?: %v\n", status)

}
