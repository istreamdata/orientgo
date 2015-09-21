package orient

import (
	"bytes"
	"fmt"
	"github.com/istreamdata/orientgo/obinary/rw"
	"io"
)

// ErrTypeSerialization represent serialization/deserialization error
type ErrTypeSerialization struct {
	Val        interface{}
	Serializer interface{}
}

func (e ErrTypeSerialization) Error() string {
	return fmt.Sprintf("Serializer (%T)%s don't support record of type %T", e.Serializer, e.Serializer, e.Val)
}

// CustomSerializable is an interface for objects that can be sent on wire
type CustomSerializable interface {
	Classer
	Serializable
}

// Classer is an interface for object that have analogs in OrientDB Java code
type Classer interface {
	// GetClassName return a Java class name for an object
	GetClassName() string
}

var (
	recordFormats       = make(map[string]func() RecordSerializer)
	recordFormatDefault = ""
)

// Serializable is an interface for objects that can be serialized to stream
type Serializable interface {
	ToStream(w io.Writer) error
}

// Deserializable is an interface for objects that can be deserialized from stream
type Deserializable interface {
	FromStream(r io.Reader) error
}

// GlobalPropertyFunc is a function for getting global properties by id
type GlobalPropertyFunc func(id int) (OGlobalProperty, bool)

// RecordSerializer is an interface for serializing records to byte streams
type RecordSerializer interface {
	// String, in case of RecordSerializer must return it's class name, as it will be sent to server
	String() string

	// TODO: ToStream and FromStream must operate with Record instead of interface{}

	ToStream(w io.Writer, rec interface{}) error
	FromStream(data []byte) (interface{}, error)

	SetGlobalPropertyFunc(fnc GlobalPropertyFunc)
}

// RegisterRecordFormat registers RecordSerializer with a given class name
func RegisterRecordFormat(name string, fnc func() RecordSerializer) {
	recordFormats[name] = fnc
}

// SetDefaultRecordFormat sets default record serializer
func SetDefaultRecordFormat(name string) {
	recordFormatDefault = name
}

// GetRecordFormat returns record serializer by class name
func GetRecordFormat(name string) RecordSerializer {
	return recordFormats[name]()
}

// GetDefaultRecordSerializer returns default record serializer
func GetDefaultRecordSerializer() RecordSerializer {
	return GetRecordFormat(recordFormatDefault)
}

// DocumentSerializable is an interface for objects that can be converted to Document
type DocumentSerializable interface {
	ToDocument() (*Document, error)
}

// DocumentDeserializable is an interface for objects that can be filled from Document
type DocumentDeserializable interface {
	FromDocument(*Document) error
}

var _ MapSerializable = (*Document)(nil)

// MapSerializable is an interface for objects that can be converted to map[string]interface{}
type MapSerializable interface {
	ToMap() (map[string]interface{}, error)
}

// SerializeAnyStreamable serializes a given object
func SerializeAnyStreamable(o CustomSerializable) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	bw := rw.NewWriter(buf)
	bw.WriteString(o.GetClassName())
	if err := o.ToStream(bw); err != nil {
		return nil, err
	}
	if err := bw.Err(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
