package binserde

import (
	"fmt"
	"io"
	"reflect"

	"gopkg.in/istreamdata/orientgo.v2"
	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
)

// There is apparently a second "binary serialization" system
// in OrientDB that has inconsistent type constants with the
// other one.
//
// Until I understand it better, for now I'm calling it, the
// "typeserializer" though that is misleading since the binserde.go
// Serializer also reads/writes (de/serializes) types.

// from Java client code base where all these extend
// com.orientechnologies.common.serialization.types.OBinarySerializer
const (
	BooleanSerializer                       = 1
	ByteSerializer                          = 2
	CharSerializer                          = 3
	DateSerializer                          = 4
	DateTimeSerializer                      = 5
	DoubleSerializer                        = 6
	FloatSerializer                         = 7
	IntegerSerializer                       = 8
	LinkSerializer                          = 9
	LongSerializer                          = 10
	NullSerializer                          = 11
	ShortSerializer                         = 12
	StringSerializer                        = 13
	CompositeKeySerializer                  = 14
	SimpleKeySerializer                     = 15
	StreamSerializerRID                     = 16
	BinaryTypeSerializer                    = 17
	DecimalSerializer                       = 18
	StreamSerializerListRID                 = 19
	StreamSerializerOldRIDContainer         = 20
	StreamSerializerSBTreeIndexRIDContainer = 21
	PhysicalPositionSerializer              = 50
)

type OBinaryTypeSerializer interface {
	Deserialize(r io.Reader) (interface{}, error)
	Serialize(val interface{}) ([]byte, error)
}

var TypeSerializers [21]OBinaryTypeSerializer

type OLinkSerializer struct{}

func (ols OLinkSerializer) Deserialize(r io.Reader) (v interface{}, err error) {
	return ols.DeserializeLink(r)
}
func (ols OLinkSerializer) DeserializeLink(r io.Reader) (v orient.OIdentifiable, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deserialize error: %v", r)
		}
	}()
	var rid orient.RID
	if err = rid.FromStream(r); err != nil {
		return
	}
	return rid, nil
}

// Serialize serializes a *orient.OLink into the binary format
// required by the OrientDB server.  If the `val` passed in is
// not a *orient.OLink, the method will panic.
func (ols OLinkSerializer) Serialize(val interface{}) ([]byte, error) {
	lnk, ok := val.(orient.OIdentifiable)
	if !ok {
		return nil, fmt.Errorf("Invalid LINK should be orient.OLink, got %s", reflect.TypeOf(val))
	}
	rid := lnk.GetIdentity()

	bs := make([]byte, rw.SizeShort+rw.SizeLong)
	rw.Order.PutUint16(bs[:rw.SizeShort], uint16(rid.ClusterID))
	rw.Order.PutUint64(bs[rw.SizeShort:], uint64(rid.ClusterPos))
	return bs, nil
}

func init() {
	TypeSerializers[LinkSerializer] = OLinkSerializer{}
}
