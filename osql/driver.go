//
// Go OrientDB Driver - An OrientDB-Driver for Go's database/sql package
//
// The driver should be used via the database/sql package:
//
// import "database/sql"
// import _ "github.com/quux00/ogonori/osql"
//
// db, err := sql.Open("ogonori", ""admin@admin:127.0.0.1/ogonoriTest")
//
package osql

import (
	"database/sql"
	"database/sql/driver"
	_ "net"
)

//
// Implements the Go sql/driver.Driver interface.
//
type OgonoriDriver struct{}

func init() {
	sql.Register("ogonori", &OgonoriDriver{})
}

//
// Open returns a new connection to the database.
// The dsn (drive-specific name) is a string in a driver-specific format.
//
func (d *OgonoriDriver) Open(dsn string) (driver.Conn, error) {
	return nil, nil
}
