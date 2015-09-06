package rw

import (
	"encoding/binary"
	"fmt"
	"io"
)

func write(w io.Writer, o interface{}) {
	if err := binary.Write(w, endianness, o); err != nil {
		panic(err)
	}
}

func WriteNull(w io.Writer) {
	WriteInt(w, -1)
}

func WriteByte(w io.Writer, b byte) {
	write(w, b)
}

// WriteShort writes a int16 in big endian order to the wfer
func WriteShort(w io.Writer, n int16) {
	write(w, n)
}

// WriteInt writes a int32 in big endian order to the wfer
func WriteInt(w io.Writer, n int32) {
	write(w, n)
}

// WriteLong writes a int64 in big endian order to the wfer
func WriteLong(w io.Writer, n int64) {
	write(w, n)
}

func WriteStrings(w io.Writer, ss ...string) {
	for _, s := range ss {
		WriteString(w, s)
	}
}

func WriteString(w io.Writer, s string) {
	WriteBytes(w, []byte(s))
}

// WriteRawBytes just writes the bytes, not prefixed by the size of the []byte
func WriteRawBytes(w io.Writer, bs []byte) {
	if n, err := w.Write(bs); err != nil {
		panic(err)
	} else if n != len(bs) {
		panic(fmt.Errorf("incorrect number of bytes written: %d", n))
	}
}

// WriteBytes is meant to be used for writing a structure that the OrientDB will
// interpret as a byte array, usually a serialized datastructure.  This means the
// first thing written to the wfer is the size of the byte array.  If you want
// to write bytes without the the size prefix, use WriteRawBytes instead.
func WriteBytes(w io.Writer, bs []byte) {
	WriteInt(w, int32(len(bs)))
	WriteRawBytes(w, bs)
}

// WriteBool writes byte(1) for true and byte(0) for false to the wfer,
// as specified by the OrientDB spec.
func WriteBool(w io.Writer, b bool) {
	if b {
		WriteByte(w, byte(1))
	} else {
		WriteByte(w, byte(0))
	}
}

// WriteFloat writes a float32 in big endian order to the wfer
func WriteFloat(w io.Writer, f float32) {
	write(w, f)
}

// WriteDouble writes a float64 in big endian order to the wfer
func WriteDouble(w io.Writer, f float64) {
	write(w, f)
}

func Copy(w io.Writer, r io.Reader) {
	if _, err := io.Copy(w, r); err != nil {
		panic(err)
	}
}
