package obinary

import (
	"errors"

	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

// UpdateRecord should be used update an existing record in the OrientDB database.
// It does the REQUEST_RECORD_UPDATE OrientDB cmd (network binary protocol)
func (dbc *Client) UpdateRecord(doc *oschema.ODocument) (err error) {
	defer catch(&err)
	if doc.RID.ClusterID < 0 || doc.RID.ClusterPos < 0 {
		return errors.New("Document is not updateable - has negative RID values")
	}
	buf := dbc.writeCommandAndSessionId(requestRecordUPDATE)

	rw.WriteShort(buf, doc.RID.ClusterID)
	rw.WriteLong(buf, doc.RID.ClusterPos)

	// update-content flag
	rw.WriteBool(buf, true)

	// serialized-doc
	serde := dbc.RecordSerDes[int(dbc.serializationVersion)]

	// this writes the serialized record to dbc.buf
	err = serde.Serialize(doc, buf)
	if err != nil {
		return err
	}

	// record version
	rw.WriteInt(buf, doc.Version)

	// record-type: document
	rw.WriteByte(buf, byte('d')) // TODO: how support 'b' (raw bytes) & 'f' (flat data)?

	// mode: synchronous
	rw.WriteByte(buf, 0x0)

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return err
	}

	doc.Version = rw.ReadInt(dbc.conx)

	nCollChanges := rw.ReadInt(dbc.conx)

	if nCollChanges != 0 {
		// if > 0, then have to deal with RidBag mgmt:
		// [(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]
		panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
	}

	return nil
}
