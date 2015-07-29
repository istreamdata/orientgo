package oschema

import (
	"encoding/json"
	"fmt"
)

type ODataType byte

// in alignment with: https://github.com/orientechnologies/orientdb/wiki/Types
// TODO: change to type ODataType
const (
	BOOLEAN      ODataType = 0
	INTEGER      ODataType = 1
	SHORT        ODataType = 2
	LONG         ODataType = 3
	FLOAT        ODataType = 4
	DOUBLE       ODataType = 5
	DATETIME     ODataType = 6
	STRING       ODataType = 7
	BINARY       ODataType = 8 // means []byte
	EMBEDDED     ODataType = 9 // was: EMBEDDEDRECORD
	EMBEDDEDLIST ODataType = 10
	EMBEDDEDSET  ODataType = 11
	EMBEDDEDMAP  ODataType = 12
	LINK         ODataType = 13
	LINKLIST     ODataType = 14
	LINKSET      ODataType = 15
	LINKMAP      ODataType = 16
	BYTE         ODataType = 17
	TRANSIENT    ODataType = 18
	DATE         ODataType = 19
	CUSTOM       ODataType = 20
	DECIMAL      ODataType = 21
	LINKBAG      ODataType = 22
	ANY          ODataType = 23  // BTW: ANY == UNKNOWN/UNSPECIFIED
	UNKNOWN      ODataType = 255 // my addition
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
	case EMBEDDED:
		return "EMBEDDED"
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
	Typ   ODataType
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
