package obinary

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/oschema"
)

var (
	_ orient.Record = (*RecordData)(nil)
	_ orient.Record = (*SerializedRecord)(nil)
	_ orient.Record = (*NullRecord)(nil)
	_ orient.Record = (*RIDRecord)(nil)
)

type RawRecord []byte

func (r RawRecord) String() string {
	//return "RAW["+string(r)+"]"
	return "RAW"
}
func (r RawRecord) Deserialize(o interface{}) error {
	return fmt.Errorf("RawRecord deserialization is not supported for now") // TODO: need example from server to know how to handle this
}
func (r RawRecord) GetRID() oschema.ORID {
	return oschema.NewORID()
}

type SerializedRecord []byte

func (r SerializedRecord) String() string {
	//return "SERIALIZED["+string(r)+"]"
	return "SERIALIZED"
}
func (r SerializedRecord) Deserialize(o interface{}) error {
	return json.Unmarshal([]byte(r), o)
}
func (r SerializedRecord) GetRID() oschema.ORID {
	return oschema.NewORID()
}

type NullRecord struct{}

func (r NullRecord) String() string {
	return "NULL"
}
func (r NullRecord) Deserialize(o interface{}) error {
	return fmt.Errorf("null record to %T", o)
}
func (r NullRecord) GetRID() oschema.ORID {
	return oschema.NewORID()
}

type RIDRecord struct {
	RID oschema.ORID
	db  *Database
}

func (r RIDRecord) String() string {
	return r.RID.String()
}
func (r RIDRecord) Deserialize(o interface{}) error {
	if r.db == nil {
		return fmt.Errorf("cant deserialize RID %s without DB connection", r.RID)
	}
	recs, err := r.db.GetRecordByRID(r.RID, "*:0", true, false)
	if err != nil {
		return err
	}
	return recs.DeserializeAll(o)
}
func (r RIDRecord) GetRID() oschema.ORID {
	return r.RID
}

type RecordData struct {
	RID     oschema.ORID
	Version int32
	Data    []byte
	db      *Database
}

func (r RecordData) String() string {
	return fmt.Sprintf("{%s %d %d}:%s", r.RID, r.Version, len(r.Data), base64.StdEncoding.EncodeToString(r.Data))
}
func (r RecordData) Deserialize(o interface{}) error {
	switch obj := o.(type) {
	case *map[string]interface{}:
		mp, err := r.db.createMapFromBytes(r.RID, r.Data)
		if err != nil {
			return err
		}
		*obj = mp
		return nil
	case *oschema.ODocument:
		doc, err := r.db.createDocumentFromBytes(r.RID, r.Version, r.Data)
		if err != nil {
			return err
		}
		*obj = *doc
		return nil
	case **oschema.ODocument:
		doc, err := r.db.createDocumentFromBytes(r.RID, r.Version, r.Data)
		if err != nil {
			return err
		}
		*obj = doc
		return nil
	}
	mapDecoder, err := NewMapDecoder(o)
	if err != nil {
		return err
	}
	props, err := r.db.createMapFromBytes(r.RID, r.Data)
	if err != nil {
		return err
	}
	return mapDecoder.Decode(props)
}
func (r RecordData) GetRID() oschema.ORID {
	return r.RID
}
