package varint

import (
	"fmt"
	"io"

	"gopkg.in/istreamdata/orientgo.v1/oerror"
)

//
// EncodeAndWriteVarInt32 zigzag encodes the int32 passed in and then
// translates that number to a protobuf/OrientDB varint, writing
// the bytes of that varint to the io.Writer.
//
func EncodeAndWriteVarInt32(wtr io.Writer, n int32) error {
	zze := ZigzagEncodeUInt32(n)
	err := varintEncode(wtr, uint64(zze))
	if err != nil {
		return oerror.NewTrace(err)
	}
	return nil
}

//
// EncodeAndWriteVarInt64 zigzag encodes the int64 passed in and then
// translates that number to a protobuf/OrientDB varint, writing
// the bytes of that varint to the io.Writer.
//
func EncodeAndWriteVarInt64(wtr io.Writer, n int64) error {
	zze := ZigzagEncodeUInt64(n)
	err := varintEncode(wtr, zze)
	if err != nil {
		return oerror.NewTrace(err)
	}
	return nil
}

//
// REMOVE ME?
//
func VarintEncode(w io.Writer, v uint64) error {
	return varintEncode(w, v)
}

//
// varintEncode encodes into the little-endian format of
// Google's Protocol Buffers standard
//
func varintEncode(w io.Writer, v uint64) error {
	ba := [1]byte{}
	nexp := 0
	ntot := 0
	for (v & 0xFFFFFFFFFFFFFF80) != 0 {
		ba[0] = byte((v & 0x7F) | 0x80)
		n, _ := w.Write(ba[:])
		ntot += n
		nexp++
		v >>= 7
	}
	ba[0] = byte(v & 0x7F)
	n, err := w.Write(ba[:])
	ntot += n
	nexp++
	if err != nil {
		return oerror.NewTrace(err)
	}
	if ntot != nexp {
		return fmt.Errorf("Incorrect number of bytes written. Expected %d. Actual %d", nexp, ntot)
	}
	return nil
}
