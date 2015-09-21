package orient

import (
	"encoding/base64"
	"fmt"
)

type ORecord interface {
	OIdentifiable
	Fill(rid RID, version int, content []byte) error // TODO: put to separate interface?
	Content() ([]byte, error)
	Version() int
	SetVersion(v int)
	SetRID(rid RID)
	RecordType() RecordType
}

var (
	_ ORecord = (*BytesRecord)(nil)
	_ ORecord = (*Document)(nil)
)

// List of standard record types
const (
	RecordTypeDocument RecordType = 'd'
	RecordTypeBytes    RecordType = 'b'
	RecordTypeFlat     RecordType = 'f'
)

func init() {
	declareRecordType(RecordTypeDocument, "document", func() ORecord { return NewEmptyDocument() })
	//declareRecordType(RecordTypeFlat,"flat",func() orient.ORecord { panic("flat record type is not implemented") }) // TODO: implement
	declareRecordType(RecordTypeBytes, "bytes", func() ORecord { return NewBytesRecord() })
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

func NewBytesRecord() *BytesRecord { return &BytesRecord{} }

// BytesRecord is a rawest representation of a record. It's schema less.
// Use this if you need to store byte[] without matter about the content.
// Useful also to store multimedia contents and binary files.
type BytesRecord struct {
	RID  RID
	Vers int
	Data []byte
}

func (r BytesRecord) Content() (data []byte, err error) {
	return r.Data, nil
}

func (r BytesRecord) Version() int {
	return r.Vers
}
func (r *BytesRecord) SetVersion(v int) {
	r.Vers = v
}
func (r *BytesRecord) SetRID(rid RID) {
	r.RID = rid
}

func (r BytesRecord) RecordType() RecordType {
	return RecordTypeBytes
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
	r.Vers = version
	r.Data = content
	return nil
}

func (r BytesRecord) String() string {
	return fmt.Sprintf("{%s %d %d}:%s", r.RID, r.Vers, len(r.Data), base64.StdEncoding.EncodeToString(r.Data))
}
