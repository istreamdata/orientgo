package orient

import (
	"bytes"
	"fmt"
	"github.com/dyy18/orientgo/oschema"
	"reflect"
)

var (
	ErrNoNodesReturned       = fmt.Errorf("No nodes were returned from the database while expecting one")
	ErrMultipleNodesReturned = fmt.Errorf("Multiple nodes were returned from the database while expecting one")
)

type Record interface {
	Deserialize(o interface{}) error
	GetRID() oschema.ORID
}

type Records []Record

func (recs Records) String() string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "Records[%d]", len(recs))
	for _, r := range recs {
		fmt.Fprintf(buf, "\n\t%T: %+v", r, r)
	}
	return buf.String()
}

func (recs Records) LoadSupplementary(docs ...*oschema.ODocument) error {
	arr := make([]*oschema.ODocument, 0, len(docs))
	for _, doc := range docs {
		arr = append(arr, doc)
	}
	for _, r := range recs {
		if !IsSupplementaryRecord(r) {
			continue
		}
		var sdoc *oschema.ODocument
		if err := r.Deserialize(&sdoc); err != nil {
			return err
		}
		arr = append(arr, sdoc)
	}
	mp := make(map[oschema.ORID]*oschema.ODocument, len(arr))
	for _, doc := range arr {
		mp[doc.RID] = doc
	}
	assignLink := func(lnk *oschema.OLink) {
		if lnk == nil || lnk.Record != nil {
			return
		}
		if sdoc, ok := mp[lnk.RID]; ok {
			lnk.Record = sdoc
		}
	}
	for _, doc := range arr {
		for _, f := range doc.Fields {
			if f.Value == nil {
				continue
			}
			switch f.Type {
			case oschema.LINK:
				assignLink(f.Value.(*oschema.OLink))
			case oschema.LINKLIST, oschema.LINKSET:
				list := f.Value.([]*oschema.OLink)
				for _, lnk := range list {
					assignLink(lnk)
				}
			case oschema.LINKMAP:
				lmap := f.Value.(map[string]*oschema.OLink)
				for _, lnk := range lmap {
					assignLink(lnk)
				}
			}
		}
	}
	return nil
}

func IsSupplementaryRecord(r Record) bool {
	switch r.(type) {
	case SupplementaryRecord, *SupplementaryRecord:
		return true
	}
	return false
}

func (recs Records) DeserializeAll(o interface{}) error {
	val := reflect.ValueOf(o).Elem()
	if val.Kind() == reflect.Slice || val.Kind() == reflect.Array {
		val.Set(reflect.MakeSlice(reflect.SliceOf(val.Type().Elem()), len(recs), len(recs)))
		j := 0
		var docs []*oschema.ODocument
		for _, r := range recs {
			if IsSupplementaryRecord(r) {
				continue
			}
			cur := val.Index(j).Addr().Interface()
			err := r.Deserialize(cur)
			if err != nil {
				return err
			}
			switch doc := cur.(type) {
			case *oschema.ODocument:
				docs = append(docs, doc)
			case **oschema.ODocument:
				docs = append(docs, *doc)
			}
			if err != nil {
				return err
			}
			j++
		}
		recs.LoadSupplementary(docs...)
		val.Set(val.Slice(0, j))
		return nil
	} else {
		rec, err := recs.One()
		if err != nil {
			return err
		}
		if err = rec.Deserialize(o); err != nil {
			return err
		}
		switch doc := o.(type) {
		case *oschema.ODocument:
			err = recs.LoadSupplementary(doc)
		case **oschema.ODocument:
			err = recs.LoadSupplementary(*doc)
		}
		return err
	}
}
func (recs Records) One() (Record, error) {
	if len(recs) == 0 {
		return nil, ErrNoNodesReturned
	} else if len(recs) > 1 {
		return nil, ErrMultipleNodesReturned
	} else {
		return recs[0], nil
	}
}
func (recs Records) AsDocuments() (out []*oschema.ODocument, err error) {
	err = recs.DeserializeAll(&out)
	return
}
func (recs Records) AsInt() (out int, err error) {
	err = recs.DeserializeAll(&out)
	return
}
func (recs Records) AsBool() (out bool, err error) {
	err = recs.DeserializeAll(&out)
	return
}
func (recs Records) WithSupplementary() bool {
	for _, r := range recs {
		if IsSupplementaryRecord(r) {
			return true
		}
	}
	return false
}

type SupplementaryRecord struct {
	Record Record
}

func (r SupplementaryRecord) String() string {
	return fmt.Sprintf("Suppl(%v)", r.Record)
}
func (r SupplementaryRecord) Deserialize(o interface{}) error {
	return r.Record.Deserialize(o)
}
func (r SupplementaryRecord) GetRID() oschema.ORID {
	return r.Record.GetRID()
}
