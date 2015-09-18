// Key schema struct, constructors that are part of the
// OrientDB schema or support representing the schema.
package oschema

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"github.com/golang/glog"
	"time"
)

var _ OIdentifiable = (*ODocument)(nil)

// ODocEntry is a generic data holder that goes in ODocuments.
type ODocEntry struct {
	Name  string
	Type  OType
	Value interface{}
}

func (fld *ODocEntry) String() string {
	return fmt.Sprintf("Entry<%s(%d): %v>", fld.Name, fld.Type, fld.Value)
}

type ODocument struct {
	RID         RID
	Version     int
	fieldsOrder []string // field names in the order they were added to the ODocument
	Fields      map[string]*ODocEntry
	Classname   string // TODO: probably needs to change *OClass (once that is built)
	dirty       bool
}

// NewDocument should be called to create new ODocument objects,
// since some internal data structures need to be initialized
// before the ODocument is ready to use.
func NewDocument(className string) *ODocument {
	doc := NewEmptyDocument()
	doc.Classname = className
	return doc
}

// TODO: have this replace NewDocument and change NewDocument to take RID and Version (???)
func NewEmptyDocument() *ODocument {
	return &ODocument{
		Fields:  make(map[string]*ODocEntry),
		RID:     NewEmptyRID(),
		Version: -1,
	}
}

func (doc *ODocument) GetIdentity() RID {
	if doc == nil {
		return NewEmptyRID()
	}
	return doc.RID
}

func (doc *ODocument) GetRecord() interface{} {
	if doc == nil {
		return nil
	}
	return doc
}

// Implements database/sql.Scanner interface
func (doc *ODocument) Scan(src interface{}) error {
	switch v := src.(type) {
	case *ODocument:
		*doc = *v
	default:
		return fmt.Errorf("ODocument: cannot convert from %T to %T", src, doc)
	}
	return nil
}

// Implements database/sql/driver.Valuer interface
// TODO: haven't detected when this is called yet (probably when serializing ODocument for insertion into DB??)
func (doc *ODocument) Value() (driver.Value, error) {
	if glog.V(10) {
		glog.Infoln("** ODocument.Value")
	}
	return []byte(`{"b": 2}`), nil // FIXME: bogus
}

// Implements database/sql/driver.ValueConverter interface
// TODO: haven't detected when this is called yet
func (doc *ODocument) ConvertValue(v interface{}) (driver.Value, error) {
	if glog.V(10) {
		glog.Infof("** ODocument.ConvertValue: %T: %v", v, v)
	}
	return []byte(`{"a": 1}`), nil // FIXME: bogus
}

// FieldNames returns the names of all the fields currently in this ODocument
// in "entry order". These fields may not have already been committed to the database.
func (doc *ODocument) FieldNames() []string {
	names := make([]string, len(doc.fieldsOrder))
	copy(names, doc.fieldsOrder)
	return names
}

// GetFields return the OField objects in the Document in "entry order".
// There is some overhead to getting them in entry order, so if you
// don't care about that order, just access the Fields field of the
// ODocument struct directly.
func (doc *ODocument) GetFields() []*ODocEntry {
	fields := make([]*ODocEntry, len(doc.fieldsOrder))
	for i, name := range doc.fieldsOrder {
		fields[i] = doc.Fields[name]
	}
	return fields
}

// GetFieldByName looks up the OField in this document with the specified field.
// If no field is found with that name, nil is returned.
func (doc *ODocument) GetField(fname string) *ODocEntry {
	return doc.Fields[fname]
}

// AddField adds a fully created field directly rather than by some of its
// attributes, as the other "Field" methods do.
// The same *ODocument is returned to allow call chaining.
func (doc *ODocument) AddField(name string, field *ODocEntry) *ODocument {
	doc.Fields[name] = field
	doc.fieldsOrder = append(doc.fieldsOrder, name)
	doc.dirty = true
	return doc
}

func (doc *ODocument) SetDirty(b bool) {
	doc.dirty = b
}

// SetField is used to add a new field to a document. This will usually be done just
// before calling Save and sending it to the database.  The field type will be inferred
// via type switch analysis on `val`.  Use FieldWithType to specify the type directly.
// The same *ODocument is returned to allow call chaining.
func (doc *ODocument) SetField(name string, val interface{}) *ODocument {
	return doc.SetFieldWithType(name, val, OTypeForValue(val))
}

// FieldWithType is used to add a new field to a document. This will usually be done just
// before calling Save and sending it to the database. The `fieldType` must correspond
// one of the OrientDB type in the schema pkg constants.  It will follow the same list
// as: https://github.com/orientechnologies/orientdb/wiki/Types
// The same *ODocument is returned to allow call chaining.
func (doc *ODocument) SetFieldWithType(name string, val interface{}, fieldType OType) *ODocument {
	fld := &ODocEntry{
		Name:  name,
		Value: val,
		Type:  fieldType,
	}

	if fieldType == DATE {
		fld.Value = adjustDateToMidnight(val)
	} else if fieldType == DATETIME {
		fld.Value = roundDateTimeToMillis(val)
	}

	return doc.AddField(name, fld)
}

func (doc *ODocument) RawContainsField(name string) bool {
	return doc != nil && doc.Fields[name] != nil
}

func (doc *ODocument) RawSetField(name string, val interface{}, fieldType OType) {
	doc.SetFieldWithType(name, val, fieldType) // TODO: implement in a right way
}

// roundDateTimeToMillis zeros out the micro and nanoseconds of a
// time.Time object in order to match the precision with which
// the OrientDB stores DATETIME values
func roundDateTimeToMillis(val interface{}) interface{} {
	tm, ok := val.(time.Time)
	if !ok {
		// if the type is wrong, we will flag it as an error when the user tries
		// to save it, rather than here while buidling the document
		return val
	}

	return tm.Round(time.Millisecond)
}

// adjustDateToMidnight zeros out the hour, minute, second, etc.
// to set the time of a DATE to midnight.  This matches the
// precision with which the OrientDB stores DATE values.
func adjustDateToMidnight(val interface{}) interface{} {
	tm, ok := val.(time.Time)
	if !ok {
		// if the type is wrong, we will flag it as an error when the user tries
		// to save it, rather than here while buidling the document
		return val
	}
	tmMidnight := time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, tm.Location())
	return interface{}(tmMidnight)
}

func (doc *ODocument) String() string {
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(fmt.Sprintf("ODocument<Classname: %s; RID: %s; Version: %d; fields: \n",
		doc.Classname, doc.RID, doc.Version))
	if err != nil {
		panic(err)
	}

	for _, fld := range doc.Fields {
		_, err = buf.WriteString(fmt.Sprintf("  %s\n", fld.String()))
		if err != nil {
			panic(err)
		}
	}

	buf.Truncate(buf.Len() - 1)
	buf.WriteString(">\n")
	return buf.String()
}

// StringNoFields is a String() method that elides the fields.
// This is useful when the fields include links and there are
// circular links.
func (doc *ODocument) StringNoFields() string {
	return fmt.Sprintf("ODocument<Classname: %s; RID: %s; Version: %d; fields: [...]>",
		doc.Classname, doc.RID, doc.Version)
}

func (doc *ODocument) ToMap() (map[string]interface{}, error) {
	if doc == nil {
		return nil, nil
	}
	// TODO: add @rid and @class fields
	out := make(map[string]interface{}, len(doc.Fields))
	for name, fld := range doc.Fields {
		out[name] = fld.Value
	}
	return out, nil
}

func (doc *ODocument) FillClassNameIfNeeded(name string) {
	if doc.Classname == "" {
		doc.SetClassNameIfExists(name)
	}
}

func (doc *ODocument) SetClassNameIfExists(name string) {
	// TODO: implement class lookup
	//	_immutableClazz = null;
	//	_immutableSchemaVersion = -1;

	doc.Classname = name
	if name == "" {
		return
	}

	//    final ODatabaseDocument db = getDatabaseIfDefined();
	//    if (db != null) {
	//      final OClass _clazz = ((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().getClass(iClassName);
	//      if (_clazz != null) {
	//        _className = _clazz.getName();
	//        convertFieldsToClass(_clazz);
	//      }
	//    }
}
