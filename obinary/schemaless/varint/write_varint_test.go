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

func TestWriteVarInt5Bytes(t *testing.T) {
	var (
		n                uint64
		err              error
		actual, expected []byte
	)
	n = 13268435566 // 0x03 16 dc 42 6e
	//   0x03     0x16     0xdc     0x42     0x6e
	// 00000011 00010110 11011100 01000010 01101110  orig
	// 10110001 10110110 11110001 10000100 01101110  varint encoded
	//   0xb1     0xb6     0xf1     0x84     0x6e
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 5, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xb1, 0xb6, 0xf1, 0x84, 0x6e}
	equals(t, expected, actual)

	n = uint64(Max4Byte) + 1
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 5, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x81, 0x80, 0x80, 0x80, 0x0}
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
