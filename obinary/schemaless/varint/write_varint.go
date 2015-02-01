package varint

import (
	"errors"
	"fmt"
	"io"
)

const (
	Max1Byte = uint32(^uint8(0) >> 1)   // 127
	Max2Byte = uint32(^uint16(0) >> 2)  // 16,383
	Max3Byte = uint32(^uint32(0) >> 11) // 2,097,151
	Max4Byte = uint32(^uint32(0) >> 4)  // 268,435,455
	Max5Byte = uint64(^uint64(0) >> 29) // 34,359,738,367
)

//
// WriteVarInt converts uint32 or uint64 integer values into
// 1 to 4 bytes, writing those bytes to the io.Writer.
// The number of bytes is determined by the size of the uint passed in:
//  ... PUT RANGES HERE..
// The uint passed in will have already been zigzag encoded to allow all
// "small" numbers (as measured by absolute value) to use less than 4 bytes.
//
func WriteVarInt(w io.Writer, data interface{}) error {
	switch data.(type) {
	case uint32:
		return WriteVarInt32(w, data.(uint32))
	case uint64:
		fmt.Println("uint64...")
	default:
		return errors.New("Data passed in is not uint32 nor uint64")
	}
	return nil
}

func WriteVarInt32(w io.Writer, n uint32) error {
	if n <= uint32(Max1Byte) {
		return varintEncode(w, n, 1)

	} else if n <= Max2Byte {
		return varintEncode(w, n, 2)

	} else if n <= Max3Byte {
		return varintEncode(w, n, 3)

	} else if n <= Max4Byte {
		return varintEncode(w, n, 4)

	} else {
		return WriteVarInt64(w, uint64(n))
	}
}

func varintEncode(w io.Writer, v uint32, nbytes int) error {
	bs := make([]byte, nbytes)
	idx := 0
	if nbytes == 4 {
		bs[idx] = byte(v>>21) | byte(0x80)
		idx++
	}
	if nbytes >= 3 {
		bs[idx] = byte(v>>14) | byte(0x80)
		idx++
	}
	if nbytes >= 2 {
		bs[idx] = byte(v>>7) | byte(0x80)
		idx++
	}
	bs[idx] = byte(v) & byte(0x7f)

	n, err := w.Write(bs)
	if err != nil {
		return err
	}
	if n != nbytes {
		return fmt.Errorf("Incorrect number of bytes written. Expected %d. Actual: %d", nbytes, n)
	}
	return nil
}

func WriteVarInt64(w io.Writer, n uint64) error {
	return nil // TODO: implement me
}
