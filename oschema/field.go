package oschema

import (
	"fmt"
	"github.com/golang/glog"
	"reflect"
)

// OType is an enum for the various data types supported by OrientDB.
type OType byte

// in alignment with: http://orientdb.com/docs/last/Types.html
const (
	BOOLEAN      OType = 0
	INTEGER      OType = 1
	SHORT        OType = 2
	LONG         OType = 3
	FLOAT        OType = 4
	DOUBLE       OType = 5
	DATETIME     OType = 6
	STRING       OType = 7
	BINARY       OType = 8 // means []byte
	EMBEDDED     OType = 9
	EMBEDDEDLIST OType = 10
	EMBEDDEDSET  OType = 11
	EMBEDDEDMAP  OType = 12
	LINK         OType = 13
	LINKLIST     OType = 14
	LINKSET      OType = 15
	LINKMAP      OType = 16
	BYTE         OType = 17
	TRANSIENT    OType = 18
	DATE         OType = 19
	CUSTOM       OType = 20
	DECIMAL      OType = 21
	LINKBAG      OType = 22
	ANY          OType = 23
	UNKNOWN      OType = 255 // driver addition
)

func (t OType) String() string { // do not change - it may be used as field type for SQL queries
	switch t {
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

func OTypeForValue(val interface{}) (ftype OType) {
	ftype = UNKNOWN
	// TODO: need to add more types: LINKSET, LINKLIST, LINKBAG, etc. ...
	switch val.(type) {
	case string:
		ftype = STRING
	case bool:
		ftype = BOOLEAN
	case int32:
		ftype = INTEGER
	case int, int64:
		ftype = LONG
	case int16:
		ftype = SHORT
	case byte, int8:
		ftype = BYTE
	case *ODocument: // TODO: and DocumentSerializable?
		ftype = EMBEDDED
	case float32:
		ftype = FLOAT
	case float64:
		ftype = DOUBLE
	case []byte:
		ftype = BINARY
	case OEmbeddedList:
		ftype = EMBEDDEDLIST
	case OEmbeddedMap:
		ftype = EMBEDDEDMAP
	case OIdentifiable:
		ftype = LINK
	case []*OLink, []OIdentifiable:
		ftype = LINKLIST
	// TODO: more types need to be added
	default:
		switch reflect.TypeOf(val).Kind() {
		case reflect.Map:
			ftype = EMBEDDEDMAP
		case reflect.Slice, reflect.Array:
			ftype = EMBEDDEDLIST
		default:
			glog.Warningf("unknown type in serialization: %T", val)
		}
	}
	return
}

func OTypeFromString(typ string) OType {
	switch typ {
	case "BOOLEAN":
		return BOOLEAN
	case "INTEGER":
		return INTEGER
	case "SHORT":
		return SHORT
	case "LONG":
		return LONG
	case "FLOAT":
		return FLOAT
	case "DOUBLE":
		return DOUBLE
	case "DATETIME":
		return DATETIME
	case "STRING":
		return STRING
	case "BINARY":
		return BINARY
	case "EMBEDDED":
		return EMBEDDED
	case "EMBEDDEDLIST":
		return EMBEDDEDLIST
	case "EMBEDDEDSET":
		return EMBEDDEDSET
	case "EMBEDDEDMAP":
		return EMBEDDEDMAP
	case "LINK":
		return LINK
	case "LINKLIST":
		return LINKLIST
	case "LINKSET":
		return LINKSET
	case "LINKMAP":
		return LINKMAP
	case "BYTE":
		return BYTE
	case "TRANSIENT":
		return TRANSIENT
	case "DATE":
		return DATE
	case "CUSTOM":
		return CUSTOM
	case "DECIMAL":
		return DECIMAL
	case "LINKBAG":
		return LINKBAG
	case "ANY":
		return ANY
	default:
		panic("Unkwown type: " + typ)
	}
}

// OField is a generic data holder that goes in ODocuments.
type OField struct {
	Id    int32
	Name  string
	Type  OType
	Value interface{}
}

func (fld *OField) String() string {
	return fmt.Sprintf("OField<id: %d; name: %s; datatype: %d; value: %v>", fld.Id, fld.Name, fld.Type, fld.Value)
}
