package orient_test

import (
	"bytes"
	"gopkg.in/istreamdata/orientgo.v2"
	"testing"
)

func TestRIDString(t *testing.T) {
	if s := orient.NewRID(5, 12).String(); s != "#5:12" {
		t.Fatal("wrong RID generated: ", s)
	}
}

func TestRIDParse(t *testing.T) {
	if rid, err := orient.ParseRID(" #5:12 "); err != nil {
		t.Fatal(err)
	} else if rid != (orient.RID{ClusterID: 5, ClusterPos: 12}) {
		t.Fatal("wrong RID parsed: ", rid)
	}
	if rid, err := orient.ParseRID(" 5:12 "); err != nil {
		t.Fatal(err)
	} else if rid != (orient.RID{ClusterID: 5, ClusterPos: 12}) {
		t.Fatal("wrong RID parsed: ", rid)
	}
}

func TestRIDNext(t *testing.T) {
	rid1 := orient.RID{ClusterID: 5, ClusterPos: 12}
	rid2 := orient.RID{ClusterID: 5, ClusterPos: 12}
	rid3 := rid2.NextRID()
	if rid3 == rid2 {
		t.Fatal("RID is the same after Next")
	} else if rid1 != rid2 {
		t.Fatal("source RID is changed after Next")
	} else if rid2.ClusterID != rid3.ClusterID {
		t.Fatal("RID ClusterId is changed after Next")
	} else if rid2.ClusterPos+1 != rid3.ClusterPos {
		t.Fatal("next RID ClusterPos is wrong")
	}
}

func testRIDSerialize(t *testing.T, s string) {
	buf := bytes.NewBuffer(nil)
	if err := orient.MustParseRID(s).ToStream(buf); err != nil {
		t.Fatal(err)
	} else if buf.Len() != orient.RIDSerializedSize {
		t.Fatalf("wrong serialized size: %d vs %d", buf.Len(), orient.RIDSerializedSize)
	}
	rid := orient.NewEmptyRID()
	if err := rid.FromStream(buf); err != nil {
		t.Fatal(err)
	} else if rid.String() != s {
		t.Fatalf("different RIDs: %v vs %v", s, rid)
	}
}

func TestRIDSerialize(t *testing.T) {
	testRIDSerialize(t, "#12:2556")
	testRIDSerialize(t, "#-1:-2")
}
