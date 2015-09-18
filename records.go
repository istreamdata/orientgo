package orient

import (
	"encoding/base64"
	"fmt"
	"io"
)

type ORecord interface {
	OIdentifiable
	Fill(rid RID, version int, content []byte) error // TODO: put to separate interface?
}

// List of standard record types
const (
	RecordTypeDocument RecordType = 'd'
	RecordTypeBytes    RecordType = 'b'
	RecordTypeFlat     RecordType = 'f'
)

func init() {
	declareRecordType(RecordTypeDocument, "document", func() ORecord { return NewDocumentRecord() })
	//declareRecordType(RecordTypeFlat,"flat",func() orient.ORecord { panic("flat record type is not implemented") }) // TODO: implement
	declareRecordType(RecordTypeBytes, "bytes", func() ORecord { return &BytesRecord{} })
}

// RecordType defines a registered record type
type RecordType byte

var recordFactories = make(map[RecordType]RecordFactory)

// RecordFactory is a function to create records of certain type
type RecordFactory func() ORecord

func declareRecordType(tp RecordType, name string, fnc RecordFactory) {
	if _, ok := recordFactories[tp]; ok {
		panic(fmt.Errorf("record type byte '%v' already in use", tp))
	}
	recordFactories[tp] = fnc
}

// GetRecordFactory returns RecordFactory for a given type
func GetRecordFactory(tp RecordType) RecordFactory {
	return recordFactories[tp]
}

// NewRecordOfType creates a new record of specified type
func NewRecordOfType(tp RecordType) ORecord {
	fnc := GetRecordFactory(tp)
	if fnc == nil {
		panic(fmt.Errorf("record type '%c' is not supported", tp))
	}
	return fnc()
}

// BytesRecord is a rawest representation of a record. It's schema less.
// Use this if you need to store byte[] without matter about the content.
// Useful also to store multimedia contents and binary files.
type BytesRecord struct {
	RID     RID
	Version int
	Data    []byte
}

// GetIdentity returns a record RID
func (r BytesRecord) GetIdentity() RID {
	return r.RID
}

// GetRecord returns a record data
func (r BytesRecord) GetRecord() interface{} {
	if r.Data == nil {
		return nil
	}
	return r.Data
}

// Fill sets identity, version and raw data of the record
func (r *BytesRecord) Fill(rid RID, version int, content []byte) error {
	r.RID = rid
	r.Version = version
	r.Data = content
	return nil
}
func (r BytesRecord) String() string {
	return fmt.Sprintf("{%s %d %d}:%s", r.RID, r.Version, len(r.Data), base64.StdEncoding.EncodeToString(r.Data))
}

var (
	_ DocumentSerializable = (*DocumentRecord)(nil)
	_ MapSerializable      = (*DocumentRecord)(nil)
)

// NewDocumentRecord creates a new DocumentRecord with default RecordSerializer
func NewDocumentRecord() *DocumentRecord {
	return &DocumentRecord{ser: GetDefaultRecordSerializer()}
}

// DocumentRecord is a subset of BytesRecord which stores serialized Document.
type DocumentRecord struct {
	data BytesRecord
	ser  RecordSerializer
}

// GetIdentity returns a record RID
func (r DocumentRecord) GetIdentity() RID {
	return r.data.GetIdentity()
}

// GetRecord decodes a record and returns Document. Will return nil if error occurs.
func (r DocumentRecord) GetRecord() interface{} {
	doc, err := r.ToDocument()
	if err != nil {
		return nil
	}
	return doc
}

// Fill sets identity, version and raw data of the record
func (r *DocumentRecord) Fill(rid RID, version int, content []byte) error {
	r.data.Fill(rid, version, content)
	return nil
}
func (r DocumentRecord) String() string {
	return "Document" + r.data.String()
}

// SetSerializer sets RecordSerializer for decoding a record
func (r *DocumentRecord) SetSerializer(ser RecordSerializer) {
	r.ser = ser
}

// ToDocument decodes a record to Document
func (r DocumentRecord) ToDocument() (doc *Document, err error) {
	defer catch(&err)
	if len(r.data.Data) == 0 {
		err = io.ErrUnexpectedEOF
		return
	}
	doc = NewEmptyDocument()
	doc.RID = r.data.RID
	doc.Version = r.data.Version

	var (
		o  interface{}
		ok bool
	)
	if o, err = r.ser.FromStream(r.data.Data); err != nil {
		return
	} else if doc, ok = o.(*Document); !ok {
		err = fmt.Errorf("expected document, got %T", o)
		return
	} else {
		doc.RID = r.data.RID
		doc.Version = r.data.Version
		return
	}
}

// ToMap decodes a record to a map
func (r DocumentRecord) ToMap() (map[string]interface{}, error) {
	doc, err := r.ToDocument()
	if err != nil {
		return nil, err
	}
	return doc.ToMap()
}
