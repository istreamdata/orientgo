package obinary

import (
	"fmt"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
	"io"
)

// UpdateRecord should be used update an existing record in the OrientDB database.
// It does the REQUEST_RECORD_UPDATE OrientDB cmd (network binary protocol)
func (db *Database) UpdateRecord(doc *oschema.ODocument) (err error) {
	defer catch(&err)
	if doc == nil {
		return fmt.Errorf("document is nil")
	} else if doc.RID.ClusterID < 0 || doc.RID.ClusterPos < 0 {
		return fmt.Errorf("document is not updateable - has negative RID values")
	}
	ser := db.serializer()
	return db.sess.sendCmd(requestRecordUPDATE, func(w io.Writer) {
		rw.WriteShort(w, doc.RID.ClusterID)
		rw.WriteLong(w, doc.RID.ClusterPos)
		rw.WriteBool(w, true) // update-content flag
		if err := ser.Serialize(doc, w); err != nil {
			panic(err)
		}
		rw.WriteInt(w, doc.Version) // record version
		rw.WriteByte(w, byte('d'))  // record-type: document // TODO: how support 'b' (raw bytes) & 'f' (flat data)?
		rw.WriteByte(w, 0)          // mode: synchronous
	}, func(r io.Reader) {
		doc.Version = rw.ReadInt(r)
		nCollChanges := rw.ReadInt(r)
		if nCollChanges != 0 {
			// if > 0, then have to deal with RidBag mgmt:
			// [(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]
			panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
		}
	})
}
