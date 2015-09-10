package obinary

import (
	"fmt"
	"github.com/istreamdata/orientgo/oschema"
	"io"
)

type ErrTypeSerialization struct {
	Val        interface{}
	Serializer interface{}
}

func (e ErrTypeSerialization) Error() string {
	return fmt.Sprintf("Serializer (%T)%s don't support record of type %T", e.Serializer, e.Serializer, e.Val)
}

type CustomSerializable interface {
	Classer
	Serializable
}

type Classer interface {
	GetClassName() string
}

var (
	recordFormats       = make(map[string]RecordSerializer)
	recordFormatDefault = ""
)

type Serializable interface {
	ToStream(w io.Writer) error
}

type Deserializable interface {
	FromStream(r io.Reader) error
}

type RecordSerializer interface {
	FormatName() string
	ToStream(w io.Writer, rec interface{}) error
	FromStream(r io.Reader, rec Deserializable) error
}

func RegisterRecordFormat(ser RecordSerializer) {
	recordFormats[ser.FormatName()] = ser
}

func SetDefaultRecordFormat(name string) {
	recordFormatDefault = name
}

func GetRecordFormat(name string) RecordSerializer {
	return recordFormats[name]
}

func GetDefaultRecordFormat() RecordSerializer {
	return recordFormats[recordFormatDefault]
}

const (
	documentSerializableClassName = "__orientdb_serilized_class__ "
)

type DocumentSerializable interface {
	ToDocument() (*oschema.ODocument, error)
}

type DocumentDeserializable interface {
	FromDocument(*oschema.ODocument) error
}
