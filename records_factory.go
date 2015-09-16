package orient

import (
	"fmt"
	"github.com/istreamdata/orientgo/oschema"
)

type RecordType byte

var recordFactories = make(map[RecordType]RecordFactory)

type RecordFactory func() oschema.ORecord

func declareRecordType(tp RecordType, name string, fnc RecordFactory) {
	if _, ok := recordFactories[tp]; ok {
		panic(fmt.Errorf("record type byte '%v' already in use", tp))
	}
	recordFactories[tp] = fnc
}

func GetRecordFactory(tp RecordType) RecordFactory {
	return recordFactories[tp]
}

func NewRecordOfType(tp RecordType) oschema.ORecord {
	fnc := GetRecordFactory(tp)
	if fnc == nil {
		panic(fmt.Errorf("record type '%c' is not supported", tp))
	}
	return fnc()
}

func init() {
	declareRecordType(RecordTypeDocument, "document", func() oschema.ORecord { return NewDocumentRecord() })
	//declareRecordType(RecordTypeFlat,"flat",func() oschema.ORecord { panic("flat record type is not implemented") }) // TODO: implement
	declareRecordType(RecordTypeBytes, "bytes", func() oschema.ORecord { return &BytesRecord{} })
}
