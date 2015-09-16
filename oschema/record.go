package oschema

type ORecord interface {
	OIdentifiable
	Fill(rid RID, version int, content []byte) error // TODO: put to separate interface?
}
