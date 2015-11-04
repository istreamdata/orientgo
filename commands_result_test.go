package orient

import (
	"reflect"
	"testing"
)

func documentFrom(o interface{}) *Document {
	doc := NewEmptyDocument()
	if err := doc.From(o); err != nil {
		panic(err)
	}
	return doc
}

func testResults(t testing.TB, input, dest, expect interface{}) {
	if err := newResults(input).All(dest); err != nil {
		t.Fatalf("check failed: %v", err)
	} else if !reflect.DeepEqual(reflect.ValueOf(dest).Elem().Interface(), expect) {
		t.Fatalf("wrong data: %T(%+v) != %T(%+v)", dest, dest, expect, expect)
	}
}

func TestResultsRecordsOneToMap(t *testing.T) {
	src := map[string]interface{}{"name": "record"}
	doc := documentFrom(src)
	var dst map[string]interface{}
	testResults(t, []OIdentifiable{doc}, &dst, src)
}

func TestResultsRecordToMap(t *testing.T) {
	doc := NewEmptyDocument()
	doc.SetFieldWithType("one", map[string]string{"name": "record"}, EMBEDDEDMAP)
	type Item struct {
		Name string
	}
	var dst map[string]*Item
	testResults(t, doc, &dst, map[string]*Item{
		"one": &Item{"record"},
	})
}

func TestResultsInnerStruct(t *testing.T) {
	type Inner struct {
		Name string
	}
	type Item struct {
		One   Inner
		Inner []Inner
	}
	one, two := Inner{Name: "one"}, Inner{Name: "two"}
	doc := NewDocument("V")
	doc.From(Item{One: one, Inner: []Inner{one, two}})
	var dst *Item
	testResults(t, doc, &dst, &Item{One: one, Inner: []Inner{one, two}})
}
