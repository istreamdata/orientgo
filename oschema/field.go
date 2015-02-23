package oschema

import "fmt"

// in alignment with: https://github.com/orientechnologies/orientdb/wiki/Types
// Note: I'm treating these as type byte - they are Enum objects in the Java code
const (
	BOOLEAN         = 0
	INTEGER         = 1
	SHORT           = 2
	LONG            = 3
	FLOAT           = 4
	DOUBLE          = 5
	DATETIME        = 6
	STRING          = 7
	BINARY          = 8 // means []byte
	EMBEDDED_RECORD = 9
	EMBEDDED_LIST   = 10
	EMBEDDED_SET    = 11
	EMBEDDED_MAP    = 12
	LINK            = 13
	LINK_LIST       = 14
	LINK_SET        = 15
	LINK_MAP        = 16
	BYTE            = 17
	TRANSIENT       = 18
	DATE            = 19
	CUSTOM          = 20
	DECIMAL         = 21
	LINK_BAG        = 22
	ANY             = 23 // BTW: ANY == UNKNOWN/UNSPECIFIED
)

//
// OField roughly corresponds to OProperty in Java client.
//
type OField struct {
	Id       int32 // TODO: is the size specified in OrientDB docs?
	Name     string
	Fullname string // Classname.propertyName
	Typ      byte   // corresponds to one of the type constants above
	Value    interface{}
}

//
// String implements Stringer interface
//
func (fld *OField) String() string {
	return fmt.Sprintf("OField[id: %d; name: %s; fullname: %s, datatype: %d; value: %v]",
		fld.Id, fld.Name, fld.Fullname, fld.Typ, fld.Value)
}
