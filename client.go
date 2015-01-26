package main

import (
	"fmt"
	"log"
	"ogo/obinary"
)

func main() {
	var (
		dbc *obinary.DbClient
		err error
	)
	fmt.Println("ConnectToServer")
	dbc, err = obinary.ConnectToServer(obinary.ClientOptions{})
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

	obinary.CloseDatabase(dbc)
	if err != nil {
		log.Fatal(err)
	}
}
