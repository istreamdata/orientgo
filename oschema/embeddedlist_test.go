package oschema

import "testing"

func TestEmbeddedSlice(t *testing.T) {

	vals := []interface{}{"a", "bb", "ccc", "DDDD"}
	embList := NewEmbeddedSlice(vals, STRING)

	equals(t, 4, embList.Len())
	equals(t, STRING, embList.Type())
	equals(t, "bb", embList.Get(1))
	equals(t, "DDDD", embList.Get(3))
	equals(t, vals, embList.Values())

	embList.Add("EE")
	embList.Add("fff")
	equals(t, 6, embList.Len())
	equals(t, "a", embList.Get(0))
	equals(t, "bb", embList.Get(1))
	equals(t, "DDDD", embList.Get(3))
	equals(t, "EE", embList.Get(4))
	equals(t, "fff", embList.Get(5))

	equals(t, 6, len(embList.Values()))

}
