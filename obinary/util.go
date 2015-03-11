package obinary

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/quux00/ogonori/constants"
)

func validStorageType(storageType string) bool {
	return storageType == constants.PersistentStorageType || storageType == constants.VolatileStorageType
}

func validDbType(dbtype string) bool {
	return dbtype == constants.DocumentDbType || dbtype == constants.GraphDbType
}

//
// Should only be used during development of the library.
// TODO: remove me
//
func fatal(err error) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
	os.Exit(1)
}
