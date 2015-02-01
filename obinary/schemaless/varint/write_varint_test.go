package varint

import (
	"bytes"
	"testing"
)

// TODO: the varints should all potentially be treated as uints?  Or since Java doesn't have unsigned types, is ints ok?

func TestWriteVarInt1Byte(t *testing.T) {
	var (
		n   uint32
		err error
	)
	n = 25
	buf := new(bytes.Buffer)

	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 1, buf.Len())
	equals(t, byte(0x19), buf.Bytes()[0])

	n = 0
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 1, buf.Len())
	equals(t, byte(0x0), buf.Bytes()[0])

	n = Max1Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 1, buf.Len())
	equals(t, byte(0x7f), buf.Bytes()[0])
}

func TestWriteVarInt2Bytes(t *testing.T) {
	var (
		n                uint32
		err              error
		actual, expected []byte
	)
	n = 5025 // 0x13a1
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xa7, 0x21}
	equals(t, expected, actual)

	n = Max1Byte + 1 // 128
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x81, 0x0}
	equals(t, expected, actual)

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
	n = 1045723 // 0x0f f4 db
	//   0x0f     0xf4     0xdb
	// 00001111 11110100 11011011  orig
	// 10111111 11101001 01011011  varint encoded
	//   0xbf     0xe9     0x5b
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 3, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xbf, 0xe9, 0x5b}
	equals(t, expected, actual)

	n = Max2Byte + 1 // 16,384 => 0x4000
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 3, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x81, 0x80, 0x0}
	equals(t, expected, actual)

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
	n = 93333333 // 0x05 90 27 55
	//   0x05     0x90     0x27     0x55
	// 00000101 10010000 00100111 01010101  orig
	// 10101100 11000000 11001110 01010101  varint encoded
	//   0xac     0xc0     0xce     0x55
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xac, 0xc0, 0xce, 0x55}
	equals(t, expected, actual)

	n = Max3Byte + 1
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x81, 0x80, 0x80, 0x0}
	equals(t, expected, actual)

	n = Max4Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 4, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}
