package varint

import (
	"bytes"
	"testing"
)

func TestWriteVarInt1Byte(t *testing.T) {
	var (
		n   uint32
		err error
	)
	buf := new(bytes.Buffer) // going to write the int as a varint of bytes to this buffer

	n = 0
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 1, buf.Len())
	equals(t, byte(0x0), buf.Bytes()[0])

	// ----

	n = Max1Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 1, buf.Len())
	equals(t, byte(0x7f), buf.Bytes()[0])

	// ----

	n = 57 // => 0x0 0x0 0x0 0x39 (big-endian)
	buf.Reset()
	// func WriteVarInt32(w io.Writer, n uint32) error {
	err = WriteVarInt32(buf, n)
	ok(t, err)

	equals(t, 1, buf.Len())
	equals(t, byte(0x39), buf.Bytes()[0])
}

func TestWriteVarInt2Bytes(t *testing.T) {
	var (
		n                uint32
		err              error
		actual, expected []byte
	)
	buf := new(bytes.Buffer)

	n = 14351 // 0x0 0x0 0x38 0x0f (big-endian, non-encoded)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x8f, 0x70}
	equals(t, expected, actual)

	// ----

	n = Max1Byte + 1 // 128 => 0x0 0x0 0x1 0x0 (big-endian, non-encoded)
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x80, 0x01}
	equals(t, expected, actual)

	// ----

	n = Max2Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0x7f}
	equals(t, expected, actual)
}

func TestWriteVarInt3Bytes(t *testing.T) {
	var (
		n                uint32
		err              error
		actual, expected []byte
	)
	n = 1836943 // 0x0 0x1c 0x07 0x8f
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 3, buf.Len())

	actual = buf.Bytes()
	expected = []byte{0x8f, 0x8f, 0x70}
	equals(t, expected, actual)

	// ----

	n = 578907 // 0x0, 0x08, 0xd5, 0x5b
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 3, buf.Len())

	actual = buf.Bytes()
	expected = []byte{0xdb, 0xaa, 0x23}
	equals(t, expected, actual)

	// ----

	n = Max2Byte + 1 // 16,384 => 0x4000
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 3, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x80, 0x80, 0x01}
	equals(t, expected, actual)

	// ----

	n = Max3Byte // 0x1f ff ff
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 3, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}

func TestWriteVarInt4Bytes(t *testing.T) {
	var (
		n                uint32
		err              error
		actual, expected []byte
	)
	n = 235128719 // 0x0e 0x03 0xc7 0x8f
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x8f, 0x8f, 0x8f, 0x70}
	equals(t, expected, actual)

	// ----

	n = 148053653 // 0x08 0xd3 0x1e 0x95
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x95, 0xbd, 0xcc, 0x46}
	equals(t, expected, actual)

	// ----

	n = Max3Byte + 1
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x80, 0x80, 0x80, 0x01}
	equals(t, expected, actual)

	// ----

	n = Max4Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}

func TestWriteVarInt5Bytes(t *testing.T) {
	var (
		n                uint64
		err              error
		actual, expected []byte
	)
	n = 13268435566 // 0x03 16 dc 42 6e
	//   0x03     0x16     0xdc     0x42     0x6e
	// 00000011 00010110 11011100 01000010 01101110  orig
	// 11101110 10000100 11110001 10110110 00110001  varint encoded
	//   0xee     0x84     0xf1     0xb6     0x31
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 5, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xee, 0x84, 0xf1, 0xb6, 0x31}
	equals(t, expected, actual)

	n = uint64(Max4Byte) + 1
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 5, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x80, 0x80, 0x80, 0x80, 0x01}
	equals(t, expected, actual)

	n = Max5Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 5, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}
