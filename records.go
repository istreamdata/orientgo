package orient

import (
	"encoding/base64"
	"fmt"
	"github.com/istreamdata/orientgo/oschema"
	"io"
)

const (
	RecordTypeDocument RecordType = 'd'
	RecordTypeBytes    RecordType = 'b'
	RecordTypeFlat     RecordType = 'f'
)

type BytesRecord struct {
	RID     oschema.RID
	Version int
	Data    []byte
}

func (r BytesRecord) GetIdentity() oschema.RID {
	return r.RID
}
func (r BytesRecord) GetRecord() interface{} {
	if r.Data == nil {
		return nil
	}
	return r.Data
}
func (r *BytesRecord) Fill(rid oschema.RID, version int, content []byte) error {
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

func NewDocumentRecord() *DocumentRecord {
	return &DocumentRecord{ser: GetDefaultRecordSerializer()}
}

type DocumentRecord struct {
	data BytesRecord
	ser  RecordSerializer
	db   interface{}
}

func (r DocumentRecord) GetIdentity() oschema.RID {
	return r.data.GetIdentity()
}
func (r DocumentRecord) GetRecord() interface{} {
	doc, err := r.ToDocument()
	if err != nil {
		return nil
	}
	return doc
}
func (r *DocumentRecord) Fill(rid oschema.RID, version int, content []byte) error {
	r.data.Fill(rid, version, content)
	return nil
}
func (r DocumentRecord) String() string {
	return "Document" + r.data.String()
}
func (r *DocumentRecord) SetSerializer(ser RecordSerializer) {
	r.ser = ser
}
func (r *DocumentRecord) SetDB(db interface{}) {
	r.db = db
}
func (r DocumentRecord) ToDocument() (doc *oschema.ODocument, err error) {
	defer catch(&err)
	if len(r.data.Data) == 0 {
		err = io.ErrUnexpectedEOF
		return
	}
	doc = oschema.NewEmptyDocument()
	doc.RID = r.data.RID
	doc.Version = r.data.Version

	var (
		o  interface{}
		ok bool
	)
	if o, err = r.ser.FromStream(r.data.Data); err != nil {
		return
	} else if doc, ok = o.(*oschema.ODocument); !ok {
		err = fmt.Errorf("expected document, got %T", o)
		return
	} else {
		doc.RID = r.data.RID
		doc.Version = r.data.Version
		return
	}
}
func (r DocumentRecord) ToMap() (map[string]interface{}, error) {
	doc, err := r.ToDocument()
	if err != nil {
		return nil, err
	}
	return doc.ToMap()
}
