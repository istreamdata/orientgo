package oschema

//
// OGlobalProperty is used by OrientDB to efficiently store "property" (field)
// types and names (but not values) across all clusters in a database
// These are stored in record #0:1 of a database and loaded when the DBClient
// starts up.  (TODO: it will also need to be updated when new fields are added
// at runtime)
//
type OGlobalProperty struct {
	Id   int32
	Name string
	Type byte // datatype: from the constants list in oschema/field.go
}

//
// based on how the Java client does it ; TODO: document usage
//
func NewGlobalPropertyFromDocument(doc *ODocument) OGlobalProperty {
	// set defaults
	id := int32(-1)
	name := ""
	typ := byte(ANY) // TODO: this may not be the right choice - might need to create UNKNOWN ?

	if fld := doc.GetField("id"); fld != nil {
		id = fld.Value.(int32)
	}
	if fld := doc.GetField("name"); fld != nil {
		name = fld.Value.(string)
	}
	if fld := doc.GetField("type"); fld != nil {
		typ = typeFromString(fld.Value.(string))
	}

	return OGlobalProperty{id, name, typ}
}

func typeFromString(typ string) byte {
	switch typ {
	case "BOOLEAN":
		return BOOLEAN
	case "INTEGER":
		return INTEGER
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
	case "EMBEDDEDRECORD":
		return EMBEDDEDRECORD
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

// TODO: Java client also has `toDocument`
// func (gp OGlobalProperty) ToDocument() *ODocument {
// 	// TODO: impl me ???
// }
