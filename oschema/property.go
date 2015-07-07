package oschema

//
// OProperty roughly corresponds to OProperty in the Java client.
// It represents a property of a class in OrientDB.
// A property represents the metadata of a field. A field (OField)
// is the actual data of a field in an ODocument.
//
type OProperty struct {
	ID           int32 // TODO: is the size specified in OrientDB docs?
	Name         string
	Fullname     string // Classname.propertyName
	Type         byte   // corresponds to one of the type constants above
	NotNull      bool
	Collate      string // is OCollate in Java client
	Mandatory    bool
	Min          string
	Max          string
	Regexp       string
	CustomFields map[string]string
	Readonly     bool
}

//
// NewOPropertyFromDocument creates a new OProperty from an ODocument
// that was created after a load schema call to the OrientDB server.
//
func NewOPropertyFromDocument(doc *ODocument) *OProperty {
	oprop := &OProperty{}
	if fld := doc.GetField("globalId"); fld != nil && fld.Value != nil {
		oprop.ID = fld.Value.(int32)
	}
	if fld := doc.GetField("name"); fld != nil && fld.Value != nil {
		oprop.Name = fld.Value.(string)
	}
	if fld := doc.GetField("type"); fld != nil && fld.Value != nil {
		oprop.Type = byte(fld.Value.(int32))
	}
	if fld := doc.GetField("notNull"); fld != nil && fld.Value != nil {
		oprop.NotNull = fld.Value.(bool)
	}
	if fld := doc.GetField("collate"); fld != nil && fld.Value != nil {
		oprop.Collate = fld.Value.(string)
	}
	if fld := doc.GetField("mandatory"); fld != nil && fld.Value != nil {
		oprop.Mandatory = fld.Value.(bool)
	}
	if fld := doc.GetField("min"); fld != nil && fld.Value != nil {
		oprop.Min = fld.Value.(string)
	}
	if fld := doc.GetField("max"); fld != nil && fld.Value != nil {
		oprop.Max = fld.Value.(string)
	}
	if fld := doc.GetField("regexp"); fld != nil && fld.Value != nil {
		oprop.Regexp = fld.Value.(string)
	}
	if fld := doc.GetField("customFields"); fld != nil && fld.Value != nil {
		oprop.CustomFields = make(map[string]string)
		panic("customFields handling NOT IMPLEMENTED: Don't know what data structure is coming back from the server (need example)")
	}
	if fld := doc.GetField("readonly"); fld != nil && fld.Value != nil {
		oprop.Readonly = fld.Value.(bool)
	}

	return oprop
}
