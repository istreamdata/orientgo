package rw

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	SizeByte   = 1
	SizeShort  = 2
	SizeInt    = 4
	SizeLong   = 8
	SizeFloat  = SizeInt
	SizeDouble = SizeLong
)

func NewWriter(w io.Writer) *Writer {
	if bw, ok := w.(*Writer); ok {
		return bw
	}
	return &Writer{W: w}
}

type Writer struct {
	err error
	W   io.Writer
}

func (w Writer) Err() error {
	return w.err
}
func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	return w.W.Write(p)
}
func (w *Writer) write(o interface{}) error {
	if w.err != nil {
		return w.err
	}
	if err := binary.Write(w.W, Order, o); err != nil {
		w.err = err
	}
	return w.err
}

// WriteRawBytes just writes the bytes, not prefixed by the size of the []byte
func (w *Writer) WriteRawBytes(bs []byte) error {
	if w.err != nil {
		return w.err
	}
	if n, err := w.W.Write(bs); err != nil {
		w.err = err
	} else if n != len(bs) {
		w.err = fmt.Errorf("incorrect number of bytes written: %d", n)
	}
	return w.err
}
func (w *Writer) WriteByte(b byte) error {
	return w.WriteRawBytes([]byte{b})
}

// WriteShort writes a int16 in big endian order to Writer
func (w *Writer) WriteShort(n int16) error {
	buf := make([]byte, SizeShort)
	Order.PutUint16(buf, uint16(n))
	return w.WriteRawBytes(buf)
}

// WriteInt writes a int32 in big endian order to Writer
func (w *Writer) WriteInt(n int32) error {
	buf := make([]byte, SizeInt)
	Order.PutUint32(buf, uint32(n))
	return w.WriteRawBytes(buf)
}

// WriteLong writes a int64 in big endian order to Writer
func (w *Writer) WriteLong(n int64) error {
	buf := make([]byte, SizeLong)
	Order.PutUint64(buf, uint64(n))
	return w.WriteRawBytes(buf)
}

func (w *Writer) WriteNull() error {
	return w.WriteInt(-1)
}

// WriteBytes is meant to be used for writing a structure that the OrientDB will
// interpret as a byte array, usually a serialized data structure.  This means the
// first thing written to Writer is the size of the byte array.  If you want
// to write bytes without the the size prefix, use WriteRawBytes instead.
func (w *Writer) WriteBytes(bs []byte) error {
	w.WriteInt(int32(len(bs)))
	w.WriteRawBytes(bs)
	return w.err
}

func (w *Writer) WriteString(s string) error {
	return w.WriteBytes([]byte(s))
}

func (w *Writer) WriteStrings(ss ...string) error {
	for _, s := range ss {
		w.WriteString(s)
	}
	return w.err
}

// WriteBool writes byte(1) for true and byte(0) for false to Writer,
// as specified by the OrientDB spec.
func (w *Writer) WriteBool(b bool) error {
	if b {
		w.WriteByte(byte(1))
	} else {
		w.WriteByte(byte(0))
	}
	return w.err
}

// WriteFloat writes a float32 in big endian order to Writer
func (w *Writer) WriteFloat(f float32) error {
	return w.write(f)
}

// WriteDouble writes a float64 in big endian order to Writer
func (w *Writer) WriteDouble(f float64) error {
	return w.write(f)
}

// WriteVarint zigzag encodes the int64 passed in and then
// translates that number to a protobuf/OrientDB varint, writing
// the bytes of that varint to the io.Writer.
func (w *Writer) WriteVarint(v int64) (int, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	return n, w.WriteRawBytes(buf[:n])
}

func (w *Writer) WriteBytesVarint(bs []byte) (int, error) {
	vn, _ := w.WriteVarint(int64(len(bs)))
	return vn + len(bs), w.WriteRawBytes(bs)
}

func (w *Writer) WriteStringVarint(s string) (int, error) {
	return w.WriteBytesVarint([]byte(s))
}
