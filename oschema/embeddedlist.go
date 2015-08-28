package oschema

// OEmbeddedList is a interface wrapper for go slices that
// should be used when serializing Go ODocuments to the
// OrientDB database.
type OEmbeddedList interface {
	Len() int
	Get(idx int) interface{}
	Add(val interface{})
	// Add(val interface{}, typ ODataType) // TODO: we could allow for mixed type lists -> useful?
	Type() OType
	Values() []interface{}
}

// ------

type OEmbeddedStringList struct { // FIXME: not yet used -> remove ??
	slice []string
	// TODO: should this just embed OEmbeddedSlice?
}

// ------

type OEmbeddedSlice struct {
	slice []interface{}
	typ   OType
}

func NewEmbeddedSlice(v []interface{}, typ OType) OEmbeddedList {
	return &OEmbeddedSlice{slice: v, typ: typ}
}

func (es *OEmbeddedSlice) Len() int {
	return len(es.slice)
}

func (es *OEmbeddedSlice) Get(idx int) interface{} {
	return es.slice[idx]
}

func (es *OEmbeddedSlice) Add(val interface{}) {
	es.slice = append(es.slice, val)
}

func (es *OEmbeddedSlice) Type() OType {
	return es.typ
}

func (es *OEmbeddedSlice) Values() []interface{} {
	return es.slice
}
