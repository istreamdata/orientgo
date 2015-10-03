package orient_test

import (
	"gopkg.in/istreamdata/orientgo.v2"
	"reflect"
	"testing"
)

func TestDocumentFromStruct(t *testing.T) {
	doc := orient.NewEmptyDocument()
	type item struct {
		Ind  int
		Name string
	}
	a := item{11, "named"}
	if err := doc.From(a); err != nil {
		t.Fatal(err)
	}
	var b item
	if err := doc.ToStruct(&b); err != nil {
		t.Fatal(err)
	} else if b != a {
		t.Fatal("data differs")
	}
}

func TestDocumentFromStructEmbedded(t *testing.T) {
	doc := orient.NewEmptyDocument()
	type Item struct {
		Ind  int
		Name string
	}
	type obj struct {
		Item Item
		Data string
	}
	a := obj{Item: Item{11, "named"}, Data: "dataz"}
	if err := doc.From(a); err != nil {
		t.Fatal(err)
	} else if doc.GetField("Item").Type != orient.EMBEDDED {
		t.Fatal("wrong field type")
	}
	var b obj
	if err := doc.ToStruct(&b); err != nil {
		t.Fatal(err)
	} else if b != a {
		t.Fatal("data differs")
	}
}

func TestDocumentFromStructEmbeddedAnon(t *testing.T) {
	doc := orient.NewEmptyDocument()
	type Item struct {
		Ind  int
		Name string
	}
	type obj struct {
		Item `mapstructure:",squash"`
		Data string
	}
	a := obj{Item: Item{11, "named"}, Data: "dataz"}
	if err := doc.From(a); err != nil {
		t.Fatal(err)
	} else if doc.GetField("Item") != nil {
		t.Fatal("default behavior should be squash")
	}
	var b obj
	if err := doc.ToStruct(&b); err != nil {
		t.Fatal(err)
	} else if b != a {
		t.Fatalf("data differs: %+v vs %+v", a, b)
	}
}

func TestDocumentFromMap(t *testing.T) {
	doc := orient.NewEmptyDocument()
	type item struct {
		Ind  int
		Name string
	}
	a := map[string]interface{}{"Ind": 11, "Name": "named"}
	if err := doc.From(a); err != nil {
		t.Fatal(err)
	}
	if b, err := doc.ToMap(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(a, b) {
		t.Fatal("data differs")
	}
}
