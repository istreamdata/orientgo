package oschema

import "fmt"

// in alignment with: https://github.com/orientechnologies/orientdb/wiki/Types
// Note: I'm treating these as type byte - they are Enum objects in the Java code
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
	ANY            = 23 // BTW: ANY == UNKNOWN/UNSPECIFIED
)

//
// OField is a generic data holder that does in ODocuments
// This is a less specific concept that OProperty.
// TODO: need more clarification here
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

//
// OProperty roughly corresponds to OProperty in Java client.
// It represents a property of a class in OrientDb.
//
type OProperty struct {
	Id           int32 // TODO: is the size specified in OrientDB docs?
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

func NewOPropertyFromDocument(doc *ODocument) *OProperty {
	oprop := &OProperty{}
	if fld, ok := doc.Fields["globalId"]; ok && fld.Value != nil {
		oprop.Id = fld.Value.(int32)
	}
	if fld, ok := doc.Fields["name"]; ok && fld.Value != nil {
		oprop.Name = fld.Value.(string)
	}
	if fld, ok := doc.Fields["type"]; ok && fld.Value != nil {
		oprop.Type = byte(fld.Value.(int32))
	}
	if fld, ok := doc.Fields["notNull"]; ok && fld.Value != nil {
		oprop.NotNull = fld.Value.(bool)
	}
	if fld, ok := doc.Fields["collate"]; ok && fld.Value != nil {
		oprop.Collate = fld.Value.(string)
	}
	if fld, ok := doc.Fields["mandatory"]; ok && fld.Value != nil {
		oprop.Mandatory = fld.Value.(bool)
	}
	if fld, ok := doc.Fields["min"]; ok && fld.Value != nil {
		oprop.Min = fld.Value.(string)
	}
	if fld, ok := doc.Fields["max"]; ok && fld.Value != nil {
		oprop.Max = fld.Value.(string)
	}
	if fld, ok := doc.Fields["regexp"]; ok && fld.Value != nil {
		oprop.Regexp = fld.Value.(string)
	}
	if fld, ok := doc.Fields["customFields"]; ok && fld.Value != nil {
		oprop.CustomFields = make(map[string]string)
		panic("customFields handling NOT IMPLEMENTED: Don't know what data structure is coming back from the server (need example)")
	}
	if fld, ok := doc.Fields["readonly"]; ok && fld.Value != nil {
		oprop.Readonly = fld.Value.(bool)
	}

	return oprop
}
