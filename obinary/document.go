package obinary

import (
	"bytes"
	"ogonori/oschema"
)

//
// TODO: in the Java version there is a "fill" method on ODocument (ORecord)
//       to create a record from these entries => maybe move this there?
//
func createDocument(rid string, recVersion int, serializedDoc []byte, dbc *DbClient) (*oschema.ODocument, error) {
	var doc *oschema.ODocument
	doc = oschema.NewDocument("") // don't know classname yet (in serialized record)
	doc.Rid = rid
	doc.Version = recVersion

	// TODO: here need to make a query to look up the schema of the doc if we don't have it already cached

	recBuf := bytes.NewBuffer(serializedDoc)
	err := dbc.RecordSerializer.Deserialize(doc, recBuf)
	if err != nil {
		return nil, err
	}
	return doc, nil
}
