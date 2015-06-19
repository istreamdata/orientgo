package oschema

import (
	"encoding/json"
	"fmt"
)

type ODataType byte

// in alignment with: https://github.com/orientechnologies/orientdb/wiki/Types
// TODO: change to type ODataType
const (
	BOOLEAN        = 0
	INTEGER        = 1
	SHORT          = 2
	LONG           = 3
	FLOAT          = 4
	DOUBLE         = 5
	DATETIME       = 6
	STRING         = 7
	BINARY         = 8 // means []byte
	EMBEDDEDRECORD = 9
	EMBEDDEDLIST   = 10
	EMBEDDEDSET    = 11
	EMBEDDEDMAP    = 12
	LINK           = 13
	LINKLIST       = 14
	LINKSET        = 15
	LINKMAP        = 16
	BYTE           = 17
	TRANSIENT      = 18
	DATE           = 19
	CUSTOM         = 20
	DECIMAL        = 21
	LINKBAG        = 22
	ANY            = 23  // BTW: ANY == UNKNOWN/UNSPECIFIED
	UNKNOWN        = 255 // my addition
)

func ODataTypeNameFor(dt ODataType) string {
	switch dt {
	case BOOLEAN:
		return "BOOLEAN"
	case INTEGER:
		return "INTEGER"
	case LONG:
		return "LONG"
	case FLOAT:
		return "FLOAT"
	case DOUBLE:
		return "DOUBLE"
	case DATETIME:
		return "DATETIME"
	case STRING:
		return "STRING"
	case BINARY:
		return "BINARY"
	case EMBEDDEDRECORD:
		return "EMBEDDEDRECORD"
	case EMBEDDEDLIST:
		return "EMBEDDEDLIST"
	case EMBEDDEDSET:
		return "EMBEDDEDSET"
	case EMBEDDEDMAP:
		return "EMBEDDEDMAP"
	case LINK:
		return "LINK"
	case LINKLIST:
		return "LINKLIST"
	case LINKSET:
		return "LINKSET"
	case LINKMAP:
		return "LINKMAP"
	case BYTE:
		return "BYTE"
	case TRANSIENT:
		return "TRANSIENT"
	case DATE:
		return "DATE"
	case CUSTOM:
		return "CUSTOM"
	case DECIMAL:
		return "DECIMAL"
	case LINKBAG:
		return "LINKBAG"
	case ANY:
		return "ANY"
	default:
		return "UNKNOWN"
	}

}

//
// OField is a generic data holder that goes in ODocuments.
//
type OField struct {
	Id    int32 // TODO: is the size specified in OrientDB docs?
	Name  string
	Typ   byte // corresponds to one of the type constants above
	Value interface{}
}

//
// Testing out JSON marshalling -> this method may change to something else
//
func (fld *OField) ToJSON() ([]byte, error) {
	return json.Marshal(fld)
}

//
// *OField implements Stringer interface
//
func (fld *OField) String() string {
	return fmt.Sprintf("OField<id: %d; name: %s; datatype: %d; value: %v>",
		fld.Id, fld.Name, fld.Typ, fld.Value)
}
