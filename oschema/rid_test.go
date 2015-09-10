package oschema_test

import (
	"github.com/istreamdata/orientgo/oschema"
	"testing"
)

func TestRIDString(t *testing.T) {
	if s := oschema.NewRID(5, 12).String(); s != "#5:12" {
		t.Fatal("wrong RID generated: ", s)
	}
}

func TestRIDParse(t *testing.T) {
	if rid, err := oschema.ParseRID(" #5:12 "); err != nil {
		t.Fatal(err)
	} else if rid != (oschema.RID{ClusterID: 5, ClusterPos: 12}) {
		t.Fatal("wrong RID parsed: ", rid)
	}
	if rid, err := oschema.ParseRID(" 5:12 "); err != nil {
		t.Fatal(err)
	} else if rid != (oschema.RID{ClusterID: 5, ClusterPos: 12}) {
		t.Fatal("wrong RID parsed: ", rid)
	}
}

func TestRIDNext(t *testing.T) {
	rid1 := oschema.RID{ClusterID: 5, ClusterPos: 12}
	rid2 := oschema.RID{ClusterID: 5, ClusterPos: 12}
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
