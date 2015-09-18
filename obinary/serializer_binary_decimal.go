package obinary

import (
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"io"
	"math/big"
)

// TODO: use big.Float for Go 1.5

func (f binaryRecordFormatV0) readDecimal(r bytesReadSeeker) interface{} {
	scale := int(rw.ReadInt(r))
	value := big.NewInt(0).SetBytes(rw.ReadBytes(r))
	return orient.Decimal{
		Scale: scale,
		Value: value,
	}
}

func (f binaryRecordFormatV0) writeDecimal(w io.Writer, o interface{}) {
	var d orient.Decimal
	switch v := o.(type) {
	case int64:
		d = orient.Decimal{Value: big.NewInt(v)}
	case *big.Int:
		d = orient.Decimal{Value: v}
	case orient.Decimal:
		d = v
	default:
		panic(orient.ErrTypeSerialization{Val: o, Serializer: f})
	}
	rw.WriteInt(w, int32(d.Scale))    // scale value, 0 for ints
	rw.WriteBytes(w, d.Value.Bytes()) // unscaled value
}
