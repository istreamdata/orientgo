package oschema

import (
	"bytes"
	"fmt"
)

//
// OEmbeddedMap acts like a map[string]interface{} but:
// * it preserves insertion order
// * it can optionally retain the data type of the value
//
// Right now there is only an OEmbeddedArrayMap implementation
// optimized for small maps.  If larger maps are needed, an
// OEmbeddedTreeMap should be implemented.
//
// Note that there is no Delete functionality (will be reviewed for that later)
//
type OEmbeddedMap interface {
	Len() int
	Get(key string) (val interface{}, typ byte)
	Put(key string, val interface{}, typ byte)
	Value(key string) interface{}

	Keys() []string
	Values() []interface{}
	Types() []byte
	All() (keys []string, vals []interface{}, types []byte)
}

//
// OEmbeddedArrayMap is optimized for small data sets, since it requires
// linear searches over the key slice to do lookups. For maps with a
// small number of entries (< 10 ?) this is typically faster than true
// hash lookups or tree walks
//
// IMPORTANT NOTE: OEmbeddedArrayMap does not properly handle value changes
// (keys mapping to new values).  They will be appended to the end and the
// old values will not be removed.  This behavior will be reviewed later.
//
type OEmbeddedArrayMap struct {
	keys  []string
	vals  []interface{}
	types []byte // TODO: change to oschema.DataType
}

//
// Creates an empty EmbeddedMap with default capacity (currently=8)
//
func NewEmbeddedMap() OEmbeddedMap {
	return NewEmbeddedMapWithCapacity(8)
}

//
// Creates an empty EmbeddedMap with specified capacity
//
func NewEmbeddedMapWithCapacity(cap int) OEmbeddedMap {
	return &OEmbeddedArrayMap{
		keys:  make([]string, 0, cap),
		vals:  make([]interface{}, 0, cap),
		types: make([]byte, 0, cap),
	}
}

func (em *OEmbeddedArrayMap) Len() int {
	return len(em.keys)
}

func (em *OEmbeddedArrayMap) Put(key string, val interface{}, typ byte) {
	em.keys = append(em.keys, key)
	em.vals = append(em.vals, val)
	em.types = append(em.types, typ)
}

func (em *OEmbeddedArrayMap) Value(key string) interface{} {
	v, _ := em.Get(key)
	return v
}

func (em *OEmbeddedArrayMap) Get(key string) (interface{}, byte) {
	for i, s := range em.keys {
		if s == key {
			return em.vals[i], em.types[i]
		}
	}
	return nil, UNKNOWN
}

func (em *OEmbeddedArrayMap) Keys() []string {
	return em.keys
}

func (em *OEmbeddedArrayMap) Values() []interface{} {
	return em.vals
}

func (em *OEmbeddedArrayMap) Types() []byte {
	return em.types
}

func (em *OEmbeddedArrayMap) All() (keys []string, vals []interface{}, types []byte) {
	return em.Keys(), em.Values(), em.Types()
}

func (em OEmbeddedArrayMap) String() string {
	var buf bytes.Buffer
	buf.WriteString("[EmbeddedMap:\n")
	buf.WriteString(fmt.Sprintf("  Keys : %v\n", em.keys))
	buf.WriteString(fmt.Sprintf("  Types: %v\n", em.types))
	buf.WriteString(fmt.Sprintf("  Vals : %v\n", em.vals))
	buf.WriteString("\n]")
	return buf.String()
}
