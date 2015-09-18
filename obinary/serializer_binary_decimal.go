package obinary

import (
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
	"io"
	"math/big"
)

// TODO: use big.Float for Go 1.5

func (f binaryRecordFormatV0) readDecimal(r bytesReadSeeker) interface{} {
	scale := int(rw.ReadInt(r))
	value := big.NewInt(0).SetBytes(rw.ReadBytes(r))
	return oschema.Decimal{
		Scale: scale,
		Value: value,
	}
}

func (f binaryRecordFormatV0) writeDecimal(w io.Writer, o interface{}) {
	var d oschema.Decimal
	switch v := o.(type) {
	case int64:
		d = oschema.Decimal{Value: big.NewInt(v)}
	case *big.Int:
		d = oschema.Decimal{Value: v}
	case oschema.Decimal:
		d = v
	default:
		panic(orient.ErrTypeSerialization{Val: o, Serializer: f})
	}
	rw.WriteInt(w, int32(d.Scale))    // scale value, 0 for ints
	rw.WriteBytes(w, d.Value.Bytes()) // unscaled value
}
