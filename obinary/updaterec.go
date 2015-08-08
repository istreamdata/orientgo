package obinary

import (
	"errors"

	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/oschema"
)

//
// UpdateRecord should be used update an existing record in the OrientDB database.
// It does the REQUEST_RECORD_UPDATE OrientDB cmd (network binary protocol)
//
func UpdateRecord(dbc *DBClient, doc *oschema.ODocument) error {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_RECORD_UPDATE)
	if err != nil {
		return oerror.NewTrace(err)
	}

	if doc.RID.ClusterID < 0 || doc.RID.ClusterPos < 0 {
		return errors.New("Document is not updateable - has negative RID values")
	}

	err = rw.WriteShort(dbc.buf, doc.RID.ClusterID)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteLong(dbc.buf, doc.RID.ClusterPos)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// update-content flag
	err = rw.WriteBool(dbc.buf, true) // TODO: need to check doc.dirty flag
	if err != nil {
		return oerror.NewTrace(err)
	}

	// serialized-doc
	serde := dbc.RecordSerDes[int(dbc.serializationVersion)]

	// this writes the serialized record to dbc.buf
	serializedBytes, err := serde.Serialize(dbc, doc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteBytes(dbc.buf, serializedBytes)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// record version
	err = rw.WriteInt(dbc.buf, doc.Version)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// record-type: document
	err = rw.WriteByte(dbc.buf, byte('d')) // TODO: how support 'b' (raw bytes) & 'f' (flat data)?
	if err != nil {
		return oerror.NewTrace(err)
	}

	// mode: synchronous
	err = rw.WriteByte(dbc.buf, 0x0)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	doc.Version, err = rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	nCollChanges, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	if nCollChanges != 0 {
		// if > 0, then have to deal with RidBag mgmt:
		// [(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]
		panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
	}

	return nil
}
