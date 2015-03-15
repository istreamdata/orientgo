package odatastructure

import (
	"testing"

	"github.com/quux00/ogonori/oschema"
)

func createTestMap() OEmbeddedMap {
	em := NewEmbeddedMap()
	em.Put("foo", "bar", oschema.STRING)
	em.Put("wibble", "wobble", oschema.STRING)
	em.Put("one", int32(1), oschema.INTEGER)
	em.Put("two", int16(2), oschema.SHORT)
	return em
}

func TestEmbeddedMapLookup(t *testing.T) {
	em := createTestMap()

	v, typ := em.Get("wibble")
	equals(t, "wobble", v)
	equals(t, byte(oschema.STRING), typ)

	v, typ = em.Get("one")
	equals(t, int32(1), v)
	equals(t, byte(oschema.INTEGER), typ)

	v = em.Value("foo")
	equals(t, "bar", v)

	v, typ = em.Get("NOT THERE")
	equals(t, nil, v)
	equals(t, byte(oschema.UNKNOWN), typ)
}

func TestEmbeddedMapInsertOrderRetained(t *testing.T) {
	em := createTestMap()
	equals(t, 4, em.Len())

	keys, vals, types := em.All()
	equals(t, "foo", keys[0])
	equals(t, "wibble", keys[1])
	equals(t, "one", keys[2])
	equals(t, "two", keys[3])

	equals(t, "bar", vals[0])
	equals(t, "wobble", vals[1])
	equals(t, int32(1), vals[2])
	equals(t, int16(2), vals[3])

	equals(t, byte(oschema.STRING), types[0])
	equals(t, byte(oschema.STRING), types[1])
	equals(t, byte(oschema.INTEGER), types[2])
	equals(t, byte(oschema.SHORT), types[3])

	em.Put("ogonori?", true, oschema.BOOLEAN)
	em.Put("last", []byte("slice"), oschema.BINARY)
	equals(t, 6, em.Len())

	keys, vals, types = em.All()
	equals(t, "ogonori?", keys[4])
	equals(t, "last", keys[5])
	equals(t, true, vals[4])
	equals(t, []byte("slice"), vals[5])
	equals(t, byte(oschema.BOOLEAN), types[4])
	equals(t, byte(oschema.BINARY), types[5])
}
