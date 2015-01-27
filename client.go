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
	defer obinary.CloseDatabase(dbc)

	fmt.Printf("%v\n", dbc) // DEBUG

	// var status bool
	// status, err = obinary.DatabaseExists(dbc, "cars", obinary.PersistentStorageType)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("`cars` persistent db exists?: %v\n", status)

	// status, err = obinary.DatabaseExists(dbc, "cars", obinary.VolatileStorageType)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("`cars` volatile db exists?: %v\n", status)

	size, err := obinary.GetDatabaseSize(dbc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return
	}
	fmt.Printf("cars database size: %v\n", size)
}
