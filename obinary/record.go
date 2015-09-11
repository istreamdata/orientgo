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
func (r RawRecord) GetIdentity() oschema.RID {
	return oschema.NewEmptyRID()
}

type SerializedRecord []byte

func (r SerializedRecord) String() string {
	//return "SERIALIZED["+string(r)+"]"
	return "SERIALIZED"
}
func (r SerializedRecord) Deserialize(o interface{}) error {
	return json.Unmarshal([]byte(r), o)
}
func (r SerializedRecord) GetIdentity() oschema.RID {
	return oschema.NewEmptyRID()
}

type RIDRecord struct {
	RID oschema.RID
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
func (r RIDRecord) GetIdentity() oschema.RID {
	return r.RID
}

type RecordData struct {
	RID     oschema.RID
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
func (r RecordData) GetIdentity() oschema.RID {
	return r.RID
}
