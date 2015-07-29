package obinary

import (
	"github.com/quux00/ogonori/obuf"
	"github.com/quux00/ogonori/oschema"
)

//
// TODO: in the Java version there is a "fill" method on ODocument (ORecord)
//       to create a record from these entries => maybe move this there?
//
func createDocumentFromBytes(rid oschema.ORID, recVersion int32, serializedDoc []byte, dbc *DBClient) (*oschema.ODocument, error) {
	var doc *oschema.ODocument
	doc = oschema.NewDocument("") // don't know classname yet (in serialized record)
	doc.RID = rid
	doc.Version = recVersion

	// TODO: here need to make a query to look up the schema of the doc if we don't have it already cached

	// the first byte specifies record serialization version
	// use it to look up serializer and strip off that byte
	serde := dbc.RecordSerDes[int(serializedDoc[0])]
	recBuf := obuf.NewReadBuffer(serializedDoc[1:])
	err := serde.Deserialize(dbc, doc, recBuf)
	if err != nil {
		return nil, err
	}
	return doc, nil
}
