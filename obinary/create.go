package obinary

import (
	"errors"

	"github.com/dyy18/orientgo/obinary/rw"
	"github.com/dyy18/orientgo/oschema"
)

// Use this to create a new record in the OrientDB database
// you are currently connected to.
// Does REQUEST_RECORD_CREATE OrientDB cmd (binary network protocol).
func (dbc *Client) CreateRecord(doc *oschema.ODocument) (err error) {
	defer catch(&err)

	buf := dbc.writeCommandAndSessionId(requestRecordCREATE)

	// cluster-id
	if doc.Classname == "" {
		return errors.New("classname must be present on Document to call CreateRecord")
	}
	clusterID := int16(-1) // indicates new class/cluster
	oclass, ok := dbc.currDb.Classes[doc.Classname]
	if ok {
		// TODO: need way to allow user to specify a non-default cluster
		clusterID = int16(oclass.DefaultClusterId)
	}

	rw.WriteShort(buf, clusterID)

	serde := dbc.RecordSerDes[int(dbc.serializationVersion)]

	// this writes the serialized record to dbc.buf
	err = serde.Serialize(doc, buf)
	if err != nil {
		return err
	}

	rw.WriteByte(buf, byte('d')) // document record-type

	rw.WriteByte(buf, byte(0)) // synchronous mode indicator

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return err
	}

	clusterID = rw.ReadShort(dbc.conx)
	clusterPos := rw.ReadLong(dbc.conx)

	doc.Version = rw.ReadInt(dbc.conx)

	nCollChanges := rw.ReadInt(dbc.conx)

	if nCollChanges != 0 {
		panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
	}

	doc.RID = oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}

	// In the Java client, they now a 'select from XXX' at this point -> would that be useful here?

	return nil
}
