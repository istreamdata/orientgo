package obinary

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func validStorageType(storageType string) bool {
	return storageType == PersistentStorageType || storageType == VolatileStorageType
}

func validDbType(dbtype string) bool {
	return dbtype == DocumentDbType || dbtype == GraphDbType
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
