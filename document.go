package orient

import (
	"bytes"
	"fmt"
	"time"

	//	"database/sql/driver"
	//	"github.com/golang/glog"
	"reflect"
	"strings"
)

var (
	_ OIdentifiable        = (*Document)(nil)
	_ DocumentSerializable = (*Document)(nil)
	_ MapSerializable      = (*Document)(nil)
	_ ORecord              = (*Document)(nil)
)

// DocEntry is a generic data holder that goes in Documents.
type DocEntry struct {
	Name  string
	Type  OType
	Value interface{}
}

func (fld *DocEntry) String() string {
	if id, ok := fld.Value.(OIdentifiable); ok {
		return fmt.Sprintf("{%s(%s): %v}", fld.Name, fld.Type, id.GetIdentity())
	}
	return fmt.Sprintf("{%s(%s): %v}", fld.Name, fld.Type, fld.Value)
}

type Document struct {
	BytesRecord
	serialized  bool
	fieldsOrder []string // field names in the order they were added to the Document
	fields      map[string]*DocEntry
	classname   string // TODO: probably needs to change *OClass (once that is built)
	dirty       bool
	ser         RecordSerializer
}

func (doc *Document) ClassName() string { return doc.classname }

// NewDocument should be called to create new Document objects,
// since some internal data structures need to be initialized
// before the Document is ready to use.
func NewDocument(className string) *Document {
	doc := NewEmptyDocument()
	doc.classname = className
	return doc
}

// TODO: have this replace NewDocument and change NewDocument to take RID and Version (???)
func NewEmptyDocument() *Document {
	return &Document{
		BytesRecord: BytesRecord{
			RID:  NewEmptyRID(),
			Vers: -1,
		},
		fields: make(map[string]*DocEntry),
		ser:    GetDefaultRecordSerializer(),
	}
}

func (doc *Document) ensureDecoded() error {
	if doc == nil {
		return fmt.Errorf("nil document")
	}
	if !doc.serialized {
		return nil
	}
	o, err := doc.ser.FromStream(doc.BytesRecord.Data)
	if err != nil {
		return err
	}
	ndoc, ok := o.(*Document)
	if !ok {
		return fmt.Errorf("expected document, got %T", o)
	}
	doc.classname = ndoc.classname
	doc.fields = ndoc.fields
	doc.fieldsOrder = ndoc.fieldsOrder
	doc.serialized = false
	return nil
}

func (doc *Document) Content() ([]byte, error) {
	// TODO: can track field changes and invalidate content if necessary - no need to serialize each time
	if doc.serialized {
		return doc.BytesRecord.Content()
	}
	buf := bytes.NewBuffer(nil)
	if err := doc.ser.ToStream(buf, doc); err != nil {
		return nil, err
	}
	doc.BytesRecord.Data = buf.Bytes()
	return doc.BytesRecord.Content()
}

func (doc *Document) GetIdentity() RID {
	if doc == nil {
		return NewEmptyRID()
	}
	return doc.BytesRecord.GetIdentity()
}

func (doc *Document) GetRecord() interface{} {
	if doc == nil {
		return nil
	}
	return doc
}

// FieldNames returns the names of all the fields currently in this Document
// in "entry order". These fields may not have already been committed to the database.
func (doc *Document) FieldNames() []string {
	doc.ensureDecoded()
	names := make([]string, len(doc.fieldsOrder))
	copy(names, doc.fieldsOrder)
	return names
}

func (doc *Document) Fields() map[string]*DocEntry {
	doc.ensureDecoded()
	return doc.fields // TODO: copy map?
}

// FieldsArray return the OField objects in the Document in "entry order".
// There is some overhead to getting them in entry order, so if you
// don't care about that order, just access the Fields field of the
// Document struct directly.
func (doc *Document) FieldsArray() []*DocEntry {
	doc.ensureDecoded()
	fields := make([]*DocEntry, len(doc.fieldsOrder))
	for i, name := range doc.fieldsOrder {
		fields[i] = doc.fields[name]
	}
	return fields
}

// GetFieldByName looks up the OField in this document with the specified field.
// If no field is found with that name, nil is returned.
func (doc *Document) GetField(fname string) *DocEntry {
	doc.ensureDecoded()
	return doc.fields[fname]
}

// AddField adds a fully created field directly rather than by some of its
// attributes, as the other "Field" methods do.
// The same *Document is returned to allow call chaining.
func (doc *Document) AddField(name string, field *DocEntry) *Document {
	doc.ensureDecoded()
	doc.fields[name] = field
	doc.fieldsOrder = append(doc.fieldsOrder, name)
	doc.dirty = true
	return doc
}

func (doc *Document) SetDirty(b bool) {
	doc.dirty = b
}

// SetField is used to add a new field to a document. This will usually be done just
// before calling Save and sending it to the database.  The field type will be inferred
// via type switch analysis on `val`.  Use FieldWithType to specify the type directly.
// The same *Document is returned to allow call chaining.
func (doc *Document) SetField(name string, val interface{}) *Document {
	doc.ensureDecoded()
	return doc.SetFieldWithType(name, val, OTypeForValue(val))
}

// FieldWithType is used to add a new field to a document. This will usually be done just
// before calling Save and sending it to the database. The `fieldType` must correspond
// one of the OrientDB type in the schema pkg constants.  It will follow the same list
// as: https://github.com/orientechnologies/orientdb/wiki/Types
// The same *Document is returned to allow call chaining.
func (doc *Document) SetFieldWithType(name string, val interface{}, fieldType OType) *Document {
	doc.ensureDecoded()
	fld := &DocEntry{
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

func (doc *Document) RawContainsField(name string) bool {
	doc.ensureDecoded()
	return doc != nil && doc.fields[name] != nil
}

func (doc *Document) RawSetField(name string, val interface{}, fieldType OType) {
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

func (doc *Document) String() string {
	class := doc.classname
	if class == "" {
		class = "<nil>"
	}
	if doc.serialized {
		return fmt.Sprintf("Document{Class: %s, RID: %s, Vers: %d, Fields: [serialized]}",
			class, doc.RID, doc.Vers)
	}
	buf := new(bytes.Buffer)
	_, err := buf.WriteString(fmt.Sprintf("Document{Class: %s, RID: %s, Vers: %d, Fields: [\n",
		class, doc.RID, doc.Vers))
	if err != nil {
		panic(err)
	}

	for _, fld := range doc.fields {
		_, err = buf.WriteString(fmt.Sprintf("  %s,\n", fld.String()))
		if err != nil {
			panic(err)
		}
	}

	buf.WriteString("]}\n")
	return buf.String()
}

func (doc *Document) ToMap() (map[string]interface{}, error) {
	if doc == nil {
		return nil, nil
	}
	if err := doc.ensureDecoded(); err != nil {
		return nil, err
	}
	out := make(map[string]interface{}, len(doc.fields))
	for name, fld := range doc.fields {
		out[name] = fld.Value
	}
	if doc.classname != "" {
		out["@class"] = doc.classname
	}
	if doc.RID.IsPersistent() { // TODO: is this correct?
		out["@rid"] = doc.RID
	}
	return out, nil
}

func (doc *Document) FillClassNameIfNeeded(name string) {
	if doc.classname == "" {
		doc.SetClassNameIfExists(name)
	}
}

func (doc *Document) SetClassNameIfExists(name string) {
	// TODO: implement class lookup
	//	_immutableClazz = null;
	//	_immutableSchemaVersion = -1;

	doc.classname = name
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

// SetSerializer sets RecordSerializer for encoding/decoding a Document
func (doc *Document) SetSerializer(ser RecordSerializer) {
	doc.ser = ser
}
func (doc *Document) Fill(rid RID, version int, content []byte) error {
	doc.serialized = doc.serialized || doc.BytesRecord.Data == nil || bytes.Compare(content, doc.BytesRecord.Data) != 0
	return doc.BytesRecord.Fill(rid, version, content)
}
func (doc *Document) RecordType() RecordType { return RecordTypeDocument }

// ToDocument implement DocumentSerializable interface. In this case, Document just returns itself.
func (doc *Document) ToDocument() (*Document, error) {
	return doc, nil
}

// ToStruct fills provided struct with content of a Document. Argument must be a pointer to structure.
func (doc *Document) ToStruct(o interface{}) error {
	mp, err := doc.ToMap()
	if err != nil {
		return err
	}
	return mapToStruct(mp, o)
}

func (doc *Document) setFieldsFrom(rv reflect.Value) error {
	switch rv.Kind() {
	case reflect.Struct:
		rt := rv.Type()
		for i := 0; i < rt.NumField(); i++ {
			fld := rt.Field(i)
			if !isExported(fld.Name) {
				continue
			}
			name := fld.Name
			tags := strings.Split(fld.Tag.Get(TagName), ",")
			if tags[0] == "-" {
				continue
			}
			if tags[0] != "" {
				name = tags[0]
			}
			squash := (len(tags) > 1 && tags[1] == "squash") // TODO: change default behavior to squash if field is anonymous
			if squash {
				if err := doc.setFieldsFrom(rv.Field(i)); err != nil {
					return fmt.Errorf("field '%s': %s", name, err)
				}
			} else {
				doc.SetField(name, rv.Field(i).Interface())
			}
		}
		return nil
	case reflect.Map:
		for _, key := range rv.MapKeys() {
			doc.SetField(fmt.Sprint(key.Interface()), rv.MapIndex(key).Interface())
		}
		return nil
	default:
		return fmt.Errorf("only maps and structs are supported, got: %T", rv.Interface())
	}
}

// From sets Document fields to values provided in argument (which can be a map or a struct).
//
// From uses TagName field tag to determine field name and conversion parameters.
// For now it supports only one special tag parameter: ",squash" which can be used to inline fields into parent struct.
func (doc *Document) From(o interface{}) error {
	// TODO: clear fields and serialized data
	if o == nil {
		return nil
	}
	rv := reflect.ValueOf(o)
	if rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	return doc.setFieldsFrom(rv)
}

/*
// Implements database/sql.Scanner interface
func (doc *Document) Scan(src interface{}) error {
	switch v := src.(type) {
	case *Document:
		*doc = *v
	default:
		return fmt.Errorf("Document: cannot convert from %T to %T", src, doc)
	}
	return nil
}

// Implements database/sql/driver.Valuer interface
// TODO: haven't detected when this is called yet (probably when serializing Document for insertion into DB??)
func (doc *Document) Value() (driver.Value, error) {
	if glog.V(10) {
		glog.Infoln("** Document.Value")
	}
	return []byte(`{"b": 2}`), nil // FIXME: bogus
}

// Implements database/sql/driver.ValueConverter interface
// TODO: haven't detected when this is called yet
func (doc *Document) ConvertValue(v interface{}) (driver.Value, error) {
	if glog.V(10) {
		glog.Infof("** Document.ConvertValue: %T: %v", v, v)
	}
	return []byte(`{"a": 1}`), nil // FIXME: bogus
}*/
