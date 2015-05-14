package binserde

import (
	"bytes"

	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/oschema"
)

//
// There is apparently a second "binary serialization" system
// in OrientDB that has inconsistent type constants with the
// other one.
//
// Until I understand it better, for now I'm calling it, the
// "typeserializer" though that is misleading since the binserde.go
// Serializer also reads/writes (de/serializes) types.
//

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
	Deserialize(buf *bytes.Buffer) (interface{}, error)
	Serialize(val interface{}) ([]byte, error)
}

var TypeSerializers [21]OBinaryTypeSerializer

type OLinkSerializer struct{}

func (ols OLinkSerializer) Deserialize(buf *bytes.Buffer) (interface{}, error) {
	clusterID, err := rw.ReadShort(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	clusterPos, err := rw.ReadLong(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	rid := oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}
	return &oschema.OLink{RID: rid}, nil
}

//
// Serialize serilializes a *oschema.OLink into the binary format
// required by the OrientDB server.  If the `val` passed in is
// not a *oschema.OLink, the method will panic.
//
func (ols OLinkSerializer) Serialize(val interface{}) ([]byte, error) {
	lnk := val.(*oschema.OLink)
	// TODO: do the big-endian translation of shorts and longs to byte arrays
	//       here manually - rather than creating another bytes.Buffer and passing
	//       it to rw.WriteShort, which passes it to binary.Write

	bs := make([]byte, 2+8) // sz of short + long
	buf := bytes.NewBuffer(bs)
	err := rw.WriteShort(buf, lnk.RID.ClusterID)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	err = rw.WriteLong(buf, lnk.RID.ClusterPos)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	return buf.Bytes(), nil
}

func init() {
	TypeSerializers[LinkSerializer] = OLinkSerializer{}
}
