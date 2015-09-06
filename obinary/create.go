package obinary

import (
	"errors"

	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
	"io"
)

// Use this to create a new record in the OrientDB database
// you are currently connected to.
// Does REQUEST_RECORD_CREATE OrientDB cmd (binary network protocol).
func (db *Database) CreateRecord(doc *oschema.ODocument) (err error) {
	defer catch(&err)
	if doc.Classname == "" {
		return errors.New("classname must be present on Document to call CreateRecord")
	}
	clusterID := int16(-1) // indicates new class/cluster
	oclass, ok := db.db.Classes[doc.Classname]
	if ok {
		clusterID = int16(oclass.DefaultClusterId) // TODO: need way to allow user to specify a non-default cluster
	}

	serde := db.serializer()

	err = db.sess.sendCmd(requestRecordCREATE, func(w io.Writer) {
		rw.WriteShort(w, clusterID)
		if err := serde.Serialize(doc, w); err != nil {
			panic(err)
		}
		rw.WriteByte(w, byte('d')) // document record-type
		rw.WriteByte(w, byte(0))   // synchronous mode indicator
	}, func(r io.Reader) {
		clusterID = rw.ReadShort(r)
		clusterPos := rw.ReadLong(r)
		doc.Version = rw.ReadInt(r)
		nCollChanges := rw.ReadInt(r)
		if nCollChanges != 0 {
			panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
		}
		doc.RID = oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}
	})
	// In the Java client, they now a 'select from XXX' at this point -> would that be useful here?
	return
}
