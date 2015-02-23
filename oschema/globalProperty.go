package oschema

//
// OGlobalProperty is used by OrientDB to efficiently store "property" (field)
// types and names (but not values) across all clusters in a database
// These are stored in record #0:1 of a database and loaded when the DbClient
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
func NewFromDocument(doc *ODocument) OGlobalProperty {
	// set defaults
	id := int32(-1)
	name := ""
	typ := byte(ANY) // TODO: this may not be the right choice - might need to create UNKNOWN ?

	if fld, ok := doc.Fields["id"]; ok {
		id = fld.Value.(int32)
	}
	if fld, ok := doc.Fields["name"]; ok {
		name = fld.Value.(string)
	}
	if fld, ok := doc.Fields["type"]; ok {
		typ = fld.Value.(byte)
	}

	return OGlobalProperty{id, name, typ}
}

// TODO: Java client also has `toDocument`
// func (gp OGlobalProperty) ToDocument() *ODocument {
// 	// TODO: impl me ???
// }
