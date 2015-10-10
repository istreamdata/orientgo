package orient

import (
	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
	"math/big"
)

func (f binaryRecordFormatV0) readDecimal(r *rw.ReadSeeker) interface{} {
	scale := int(r.ReadInt())
	value := big.NewInt(0).SetBytes(r.ReadBytes())
	return Decimal{
		Scale: scale,
		Value: value,
	}
}

func (f binaryRecordFormatV0) writeDecimal(w *rw.Writer, o interface{}) {
	var d Decimal
	switch v := o.(type) {
	case int64:
		d = Decimal{Value: big.NewInt(v)}
	case *big.Int:
		d = Decimal{Value: v}
	case Decimal:
		d = v
	default:
		panic(ErrTypeSerialization{Val: o, Serializer: f})
	}
	w.WriteInt(int32(d.Scale))    // scale value, 0 for ints
	w.WriteBytes(d.Value.Bytes()) // unscaled value
}
