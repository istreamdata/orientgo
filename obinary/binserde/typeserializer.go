package binserde

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
	"io"
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
func (ols OLinkSerializer) DeserializeLink(r io.Reader) (v *oschema.OLink, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("deserialize error: %v", r)
		}
	}()
	clusterID := rw.ReadShort(r)
	clusterPos := rw.ReadLong(r)
	rid := oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}
	return &oschema.OLink{RID: rid}, nil
}

// Serialize serializes a *oschema.OLink into the binary format
// required by the OrientDB server.  If the `val` passed in is
// not a *oschema.OLink, the method will panic.
func (ols OLinkSerializer) Serialize(val interface{}) ([]byte, error) {
	lnk, ok := val.(*oschema.OLink)
	if !ok {
		return nil, fmt.Errorf("Invalid LINK should be oschema.OLink, got %s", reflect.TypeOf(val))
	}

	bs := make([]byte, 2+8) // sz of short + long
	binary.BigEndian.PutUint16(bs[0:2], uint16(lnk.RID.ClusterID))
	binary.BigEndian.PutUint64(bs[2:10], uint64(lnk.RID.ClusterPos))
	return bs, nil
}

func init() {
	TypeSerializers[LinkSerializer] = OLinkSerializer{}
}
