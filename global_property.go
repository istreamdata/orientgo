package orient

// OGlobalProperty is used by OrientDB to efficiently store "property" (field)
// types and names (but not values) across all clusters in a database
// These are stored in record #0:1 of a database and loaded when the DBClient
// starts up.  (TODO: it will also need to be updated when new fields are added
// at runtime)
type OGlobalProperty struct {
	Id   int32 // TODO: change to int?
	Name string
	Type OType
}

// based on how the Java client does it ; TODO: document usage
func NewGlobalPropertyFromDocument(doc *Document) OGlobalProperty {
	// set defaults
	id := int32(-1)
	name := ""
	typ := ANY // TODO: this may not be the right choice - might need to create UNKNOWN ?

	if fld := doc.GetField("id"); fld != nil {
		id = fld.Value.(int32)
	}
	if fld := doc.GetField("name"); fld != nil {
		name = fld.Value.(string)
	}
	if fld := doc.GetField("type"); fld != nil {
		typ = OTypeFromString(fld.Value.(string))
	}

	return OGlobalProperty{id, name, typ}
}

// TODO: Java client also has `toDocument`
// func (gp OGlobalProperty) ToDocument() *Document {
// 	// TODO: impl me ???
// }
