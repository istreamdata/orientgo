package obinary

import (
	"encoding/json"
	"fmt"
	"github.com/dyy18/orientgo"
	"github.com/dyy18/orientgo/oschema"
)

var (
	_ orient.Record = (*RecordData)(nil)
	_ orient.Record = (*SerializedRecord)(nil)
	_ orient.Record = (*NullRecord)(nil)
	_ orient.Record = (*RIDRecord)(nil)
)

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
	dbc *Client
}

func (r RIDRecord) String() string {
	return r.RID.String()
}
func (r RIDRecord) Deserialize(o interface{}) error {
	return fmt.Errorf("cant deserialize RID to %T for now", o)
}
func (r RIDRecord) GetRID() oschema.ORID {
	return r.RID
}

type RecordData struct {
	RID     oschema.ORID
	Version int32
	Data    []byte
	dbc     *Client
}

func (r RecordData) String() string {
	return fmt.Sprintf("{%s %d %d}", r.RID, r.Version, len(r.Data))
}
func (r RecordData) Deserialize(o interface{}) error {
	switch obj := o.(type) {
	case *map[string]interface{}:
		mp, err := r.dbc.createMapFromBytes(r.RID, r.Data)
		if err != nil {
			return err
		}
		*obj = mp
		return nil
	case *oschema.ODocument:
		doc, err := r.dbc.createDocumentFromBytes(r.RID, r.Version, r.Data)
		if err != nil {
			return err
		}
		*obj = *doc
		return nil
	case **oschema.ODocument:
		doc, err := r.dbc.createDocumentFromBytes(r.RID, r.Version, r.Data)
		if err != nil {
			return err
		}
		*obj = doc
		return nil
	}
	mapDecoder, err := r.dbc.NewMapDecoder(o)
	if err != nil {
		return err
	}
	props, err := r.dbc.createMapFromBytes(r.RID, r.Data)
	if err != nil {
		return err
	}
	return mapDecoder.Decode(props)
}
func (r RecordData) GetRID() oschema.ORID {
	return r.RID
}
