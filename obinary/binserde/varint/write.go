package varint

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteVarint zigzag encodes the int64 passed in and then
// translates that number to a protobuf/OrientDB varint, writing
// the bytes of that varint to the io.Writer.
func WriteVarint(w io.Writer, v int64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	_, err := w.Write(buf[:n])
	if err != nil {
		panic(err)
	}
}

func WriteBytes(w io.Writer, bs []byte) {
	WriteVarint(w, int64(len(bs)))
	if n, err := w.Write(bs); err != nil {
		panic(err)
	} else if n != len(bs) {
		panic(fmt.Errorf("Error in varint.WriteBytes: size of bytes written was less than byte slice size: %v", n))
	}
}

func WriteString(wtr io.Writer, s string) {
	WriteBytes(wtr, []byte(s))
}
