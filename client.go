package main

import (
	"fmt"
	"log"
	"ogonori/obinary"
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

	// fmt.Println("OpenDatabase")
	// err = obinary.OpenDatabase(dbc, "cars", obinary.DocumentDbType, "admin", "admin")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("%v\n", dbc) // DEBUG

	// // size, err := obinary.GetDatabaseSize(dbc)
	// // if err != nil {
	// // 	fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
	// // 	return
	// // }
	// // fmt.Printf("cars database size: %v\n", size)

	// obinary.CloseDatabase(dbc)

	fmt.Println("\n-------- server commands --------")

	err = obinary.CreateServerSession(dbc, "root", "A406A900E578DC7094FBA78001A45BE611DB06B25F593026CCDF737A31A9D0E9")
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN s1: %v\n", err)
		return
	}

	err = obinary.RequestDbList(dbc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN s2: %v\n", err)
		return
	}

	// var status bool
	// status, err = obinary.DatabaseExists(dbc, "cars", obinary.PersistentStorageType)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Printf("`cars` persistent db exists?: %v\n", status)
	// }

	// dbexists, err := obinary.DatabaseExists(dbc, "cars", obinary.VolatileStorageType)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("`cars` volatile db exists?: %v\n", dbexists)

	// dbexists, err = obinary.DatabaseExists(dbc, "clammy", obinary.VolatileStorageType)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("`clammy` volatile db exists?: %v\n", dbexists)

	// if !dbexists {
	// 	fmt.Println("attemping to create clammy db ... ")
	// 	err = obinary.CreateDatabase(dbc, "clammy", obinary.DocumentDbType, obinary.VolatileStorageType)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println("clammy db created ... ")
	// }

	// status, err = obinary.DatabaseExists(dbc, "clammy", obinary.VolatileStorageType)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("`clammy` volatile db exists?: %v\n", status)

	// err = obinary.DropDatabase(dbc, "clammy", obinary.PersistentStorageType)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	fmt.Println("DONE")
}
