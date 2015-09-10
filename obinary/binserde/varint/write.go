package varint

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteVarint zigzag encodes the int64 passed in and then
// translates that number to a protobuf/OrientDB varint, writing
// the bytes of that varint to the io.Writer.
func WriteVarint(w io.Writer, v int64) int {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	if nw, err := w.Write(buf[:n]); err != nil {
		panic(err)
	} else if n != nw {
		panic(io.ErrShortWrite)
	}
	return n
}

func WriteBytes(w io.Writer, bs []byte) int {
	vn := WriteVarint(w, int64(len(bs)))
	if n, err := w.Write(bs); err != nil {
		panic(err)
	} else if n != len(bs) {
		panic(fmt.Errorf("Error in varint.WriteBytes: size of bytes written was less than byte slice size: %v", n))
	}
	return vn + len(bs)
}

func WriteString(wtr io.Writer, s string) int {
	return WriteBytes(wtr, []byte(s))
}
