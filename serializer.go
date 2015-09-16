package orient

import (
	"bytes"
	"fmt"
	"github.com/istreamdata/orientgo/obinary/rw"
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
	recordFormats       = make(map[string]func() RecordSerializer)
	recordFormatDefault = ""
)

type Serializable interface {
	ToStream(w io.Writer) error
}

type Deserializable interface {
	FromStream(r io.Reader) error
}

type GlobalPropertyFunc func(id int) (oschema.OGlobalProperty, bool)

type RecordSerializer interface {
	// String, in case of RecordSerializer must return it's class name, as it will be sent to server
	String() string

	// TODO: ToStream and FromStream must operate with Record instead of interface{}

	ToStream(w io.Writer, rec interface{}) error
	FromStream(data []byte) (interface{}, error)

	SetGlobalPropertyFunc(fnc GlobalPropertyFunc)
}

func RegisterRecordFormat(name string, fnc func() RecordSerializer) {
	recordFormats[name] = fnc
}

func SetDefaultRecordFormat(name string) {
	recordFormatDefault = name
}

func GetRecordFormat(name string) RecordSerializer {
	return recordFormats[name]()
}

func GetDefaultRecordSerializer() RecordSerializer {
	return GetRecordFormat(recordFormatDefault)
}

type DocumentSerializable interface {
	ToDocument() (*oschema.ODocument, error)
}

type DocumentDeserializable interface {
	FromDocument(*oschema.ODocument) error
}

var _ MapSerializable = (*oschema.ODocument)(nil)

type MapSerializable interface {
	ToMap() (map[string]interface{}, error)
}

func SerializeAnyStreamable(o CustomSerializable) (data []byte, err error) {
	defer catch(&err)
	buf := bytes.NewBuffer(nil)
	rw.WriteString(buf, o.GetClassName())
	if err = o.ToStream(buf); err != nil {
		return
	}
	data = buf.Bytes()
	return
}
