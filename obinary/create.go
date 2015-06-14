package obinary

import (
	"bytes"
	"errors"

	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/oschema"
)

//
// Does REQUEST_RECORD_CREATE binary OrientDB cmd
//
func CreateRecord(dbc *DBClient, doc *oschema.ODocument) error {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_COMMAND)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// cluster-id
	if doc.Classname == "" {
		return errors.New("classname must be present on Document to call CreateRecord")
	}
	clusterID := int16(-1) // indicates new class/cluster
	oclass, ok := dbc.currDB.Classes[doc.Classname]
	if ok {
		// TODO: need way to allow user to specify a non-default cluster
		clusterID = int16(oclass.DefaultClusterId)
	}

	err = rw.WriteShort(dbc.buf, clusterID)
	if err != nil {
		return oerror.NewTrace(err)
	}

	serdebuf := new(bytes.Buffer)
	serde := dbc.RecordSerDes[int(dbc.serializationVersion)]
	err = serde.Serialize(doc, serdebuf)

	docbytes := serdebuf.Bytes()

	err = rw.WriteBytes(dbc.buf, docbytes)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteByte(dbc.buf, byte('d')) // document record-type
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteByte(dbc.buf, byte(0)) // synchronous mode indicator
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

	clusterID, err = rw.ReadShort(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	clusterPos, err := rw.ReadLong(dbc.conx)
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
		panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
	}

	doc.RID = oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}

	//
	// In the Java client, they now a 'select from XXX' at this point -> would that be useful here?
	//

	return nil
}
