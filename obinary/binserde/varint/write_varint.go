package varint

import (
	"errors"
	"fmt"
	"io"

	"github.com/quux00/ogonori/oerror"
)

const (
	// max varint sizes
	Max1Byte = uint32(^uint8(0) >> 1)   // 127
	Max2Byte = uint32(^uint16(0) >> 2)  // 16,383
	Max3Byte = uint32(^uint32(0) >> 11) // 2,097,151
	Max4Byte = uint32(^uint32(0) >> 4)  // 268,435,455
	Max5Byte = uint64(^uint64(0) >> 29) // 34,359,738,367
	Max6Byte = uint64(^uint64(0) >> 22) // 4,398,046,511,103
	Max7Byte = uint64(^uint64(0) >> 15) // 562,949,953,421,311
	Max8Byte = uint64(^uint64(0) >> 8)  // 72,057,594,037,927,935
)

//
// WriteVarInt converts uint32 or uint64 integer values into
// 1 to 4 bytes, writing those bytes to the io.Writer.
// The number of bytes is determined by the size of the uint passed in -
// see the constants defined in this package for the ranges
// The uint passed in will have already been zigzag encoded to allow all
// "small" numbers (as measured by absolute value) to use less than 4 bytes.
//
// TODO: remove?
func WriteVarInt(w io.Writer, data interface{}) error {
	switch data.(type) {
	case uint32:
		return WriteVarInt32(w, data.(uint32))
	case uint64:
		return WriteVarInt64(w, data.(uint64))
	default:
		return errors.New("Data passed in is not uint32 nor uint64")
	}
	return nil
}

//
// WriteVarInt32 writes an integer that is less than or equal to Max4Byte
// to the Writer provided. It will write at 1-4 bytes using the varint
// encoding format of OrientDB schemaless binary serialization spec.
//
// Typically you should call WriteVarInt instead.  If you need to call
// the direct method, if your uint is greater than Max4Byte
// then call WriteVarInt64 instead.
//
func WriteVarInt32(w io.Writer, n uint32) error {
	if n <= uint32(Max1Byte) {
		return varintEncode32(w, n, 1)

	} else if n <= Max2Byte {
		return varintEncode32(w, n, 2)

	} else if n <= Max3Byte {
		return varintEncode32(w, n, 3)

	} else if n <= Max4Byte {
		return varintEncode32(w, n, 4)

	} else {
		return WriteVarInt64(w, uint64(n))
	}
}

//
// WriteVarInt64 writes an integer that is larger than Max4Byte
// to the Writer provided. It will write at least 5 bytes using
// the varint encoding format of OrientDB schemaless binary serialization
// specification.
//
// Typically you should call WriteVarInt instead.  If you need to call
// the direct method, if your uint is less than or equal to Max4Byte
// then call WriteVarInt32 instead.
//
func WriteVarInt64(w io.Writer, n uint64) error {
	if n <= uint64(Max5Byte) {
		return varintEncode64(w, n, 5)

	} else if n <= uint64(Max6Byte) {
		return varintEncode64(w, n, 6)

	} else if n <= uint64(Max7Byte) {
		return varintEncode64(w, n, 7)

	} else if n <= uint64(Max8Byte) {
		return varintEncode64(w, n, 8)

	} else {
		return fmt.Errorf("The maximum integer than can currently be written to varint is %d (%#x)",
			Max8Byte, Max8Byte)
	}
}

func varintEncode32(w io.Writer, v uint32, nbytes int) error {
	bs := make([]byte, nbytes)

	for i := 0; i < nbytes; i++ {
		shift := uint32(i * 7)
		b := byte(v >> shift)

		if i == nbytes-1 {
			bs[i] = b & byte(0x7f)
		} else {
			bs[i] = b | byte(0x80)
		}
	}

	n, err := w.Write(bs)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if n != nbytes {
		return fmt.Errorf("Incorrect number of bytes written. Expected %d. Actual %d", nbytes, n)
	}
	return nil
}

func varintEncode64(w io.Writer, v uint64, nbytes int) error {
	bs := make([]byte, nbytes)

	for i := 0; i < nbytes; i++ {
		shift := uint32(i * 7)
		b := byte(v >> shift)

		if i == nbytes-1 {
			bs[i] = b & byte(0x7f)
		} else {
			bs[i] = b | byte(0x80)
		}
	}

	n, err := w.Write(bs)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if n != nbytes {
		return fmt.Errorf("Incorrect number of bytes written. Expected %d. Actual %d", nbytes, n)
	}
	return nil
}
