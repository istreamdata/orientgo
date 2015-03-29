package osql

import (
	_ "database/sql"
	"database/sql/driver"
	"io"

	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
)

//
// ogonoriRows implements the sql/driver.Rows interface.
//
type ogonoriRows struct {
	pos     int // index of next row (doc) to return
	docs    []*oschema.ODocument
	cols    []string
	fulldoc bool // whether query returned a full document or just properties
	// TODO: maybe a reference to the appropriate schema is needed here?
}

func NewRows(docs []*oschema.ODocument) *ogonoriRows {
	var cols []string
	if docs == nil || len(docs) == 0 {
		cols = []string{}
		return &ogonoriRows{docs: docs, cols: cols}
	}

	var fulldoc bool
	if docs[0].Classname == "" {
		cols = make([]string, 0, len(docs[0].FieldNames()))
		for _, fname := range docs[0].FieldNames() {
			cols = append(cols, fname)
		}
	} else {
		fulldoc = true
		// if Classname is set then the user queried for a full document
		// not individual properties of a Document/Class
		cols = []string{docs[0].Classname}
	}

	ogl.Printf("COLSCOLS: %v\n", cols)
	return &ogonoriRows{docs: docs, cols: cols, fulldoc: fulldoc}
}

//
// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice.  If a particular column name isn't known, an empty
// string should be returned for that entry.
//
func (rows *ogonoriRows) Columns() []string {
	ogl.Printf("** ogonoriRows.Columns = %v\n", rows.cols)
	return rows.cols
}

//
// Close closes the rows iterator.
//
func (rows *ogonoriRows) Close() error {
	ogl.Println("** ogonoriRows.Close")
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
func (rows *ogonoriRows) Next(dest []driver.Value) error {
	ogl.Println("** ogonoriRows.Next")
	if rows.pos >= len(rows.docs) {
		return io.EOF
	}
	// TODO: may need to do a type switch here -> what else can come in from a query in OrientDB
	//       besides an ODocument ??
	currdoc := rows.docs[rows.pos]
	if rows.fulldoc {
		dest[0] = currdoc

	} else {
		// was a property only query
		for i := range dest {
			// TODO: need to check field.Type and see if it is one that can map to Value
			//       what will I do for types that don't map to Value (e.g., EmbeddedRecord, EmbeddedMap) ??
			field := currdoc.GetField(rows.cols[i])
			dest[i] = field.Value
		}
	}

	rows.pos++
	// TODO: this is where we need to return any errors that occur
	return nil
}
