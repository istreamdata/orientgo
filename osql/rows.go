package osql

import (
	_ "database/sql"
	"database/sql/driver"
	"io"

	"github.com/quux00/ogonori/obinary"
)

//
// ogonoriRows implements the sql/driver.Rows interface.
//
type ogonoriRows struct {
	dbc *obinary.DBClient // TODO: review - following the mysql driver lead
	// TODO: maybe a reference to the appropriate schema is needed here?
}

//
// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice.  If a particular column name isn't known, an empty
// string should be returned for that entry.
//
func (rw *ogonoriRows) Columns() []string {
	return nil
}

//
// Close closes the rows iterator.
//
func (rw *ogonoriRows) Close() error {
	return nil
}

//
// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// The dest slice may be populated only with
// a driver Value type, but excluding string.
// All string values must be converted to []byte.
//
// Next should return io.EOF when there are no more rows.
//
func (rw *ogonoriRows) Next(dest []driver.Value) error {
	return io.EOF
}
