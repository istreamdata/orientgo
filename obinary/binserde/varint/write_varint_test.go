package varint

import (
	"bytes"
	"encoding/binary"
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
	//   0x38        0x0f
	// 0011 1000   0000 1111  unencoded
	// 1000 1111   0111 0000  varint-encoded
	//   0x8f        0x70
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x8f, 0x70}
	equals(t, expected, actual)

	// ----

	n = 1001 // 0x0 0x0 0x03 0xe9
	//   0x03         0xe9
	// 0000 0011   1110 1001  unencoded
	// 1110 1001   0000 0111  varint-encoded
	//   0xe9         0x07
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 2, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xe9, 0x07}
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

func TestWriteVarInt6Bytes(t *testing.T) {
	var (
		n                uint64
		err              error
		actual, expected []byte
	)
	n = 4000046222092 // 0x3 a3 55 55 8b 0c
	//   0x3       a3       55       55       8b      0c
	// 00000011 10100011 01010101 01010101 10001011 00001100  orig
	// 10001100 10010110 11010110 10101010 10110101 01110100  varint encoded
	//   0x8c     0x96     0xd6     0xaa     0xb5     0x74
	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 6, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x8c, 0x96, 0xd6, 0xaa, 0xb5, 0x74}
	equals(t, expected, actual)

	n = uint64(Max5Byte) + 1
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 6, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	equals(t, expected, actual)

	n = Max6Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 6, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}

func TestWriteVarInt7Bytes(t *testing.T) {
	var (
		n                uint64
		err              error
		actual, expected []byte
	)
	n = 162149153121311 //  0x93 79 4a ac 24 1f

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	//   0x93      79       4a       ac       24       1f
	// 10010011 01111001 01001010 10101100 00100100 00011111  - orig

	// [1001 1111]  [1100 1000]  [1011 0000]  [1101 0101]  [1001 0100]  [1110 1111]  [0010 0100]  varint
	//   010 0100     110 1111     001 0100     101 0101     011 0000     100 1000     001 1111   remove high bit

	//     0x93          79          4a            ac          24           1f
	// [1001 0011]  [0111 1001]  [0100 1010]  [1010 1100]  [0010 0100]  [0001 1111]  - orig (big endian)
	//    10 0100     110 1111     001 0100     101 0101     011 0000     100 1000     001 1111   orig unit64 rearranged
	// [0010 0100]  [1110 1111]  [1001 0100]  [1101 0101]  [1011 0000]  [1100 1000]  [1001 1111]  big endian variint -> high bits added
	// [1001 1111]  [1100 1000]  [1011 0000]  [1101 0101]  [1001 0100]  [1110 1111]  [0010 0100]  little endian varint
	//     0x9f        0xc8         0xb0         0xd5         0x94         0xef         0x24

	buf := new(bytes.Buffer)
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 7, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x9f, 0xc8, 0xb0, 0xd5, 0x94, 0xef, 0x24}
	equals(t, expected, actual)

	n = uint64(Max6Byte) + 1
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 7, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	equals(t, expected, actual)

	n = Max7Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 7, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}

func TestWriteVarInt8Bytes(t *testing.T) {
	var (
		n                uint64
		err              error
		actual, expected []byte
	)
	buf := new(bytes.Buffer)

	n = Max8Byte
	buf.Reset()
	err = WriteVarInt(buf, n)
	ok(t, err)
	equals(t, 8, buf.Len())
	actual = buf.Bytes()
	expected = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
	equals(t, expected, actual)
}

func TestWriteVarIntLargeThanMax8ByteShouldReturnError(t *testing.T) {
	var (
		n   uint64
		err error
	)
	buf := new(bytes.Buffer)

	n = Max8Byte + 1
	err = WriteVarInt(buf, n)
	assert(t, err != nil, "int larger than Max8Byte should return error when varint encode attempted")
}

func TestEncodeAndWriteVarInt32_SingleByteVal(t *testing.T) {
	intval := int32(7)
	buf1 := new(bytes.Buffer)
	err := EncodeAndWriteVarInt32(buf1, intval)
	ok(t, err)
	buf2 := new(bytes.Buffer)
	err = EncodeAndWriteVarInt32(buf2, intval)
	ok(t, err)

	equals(t, 1, buf1.Len())
	equals(t, 1, buf2.Len())

	// test it via ReadVarIntAndDecode32
	decoded, err := ReadVarIntAndDecode32(buf1)
	ok(t, err)
	equals(t, intval, decoded)

	// test the directly expected value
	// 7 => 14 zigzag encoded, which is byte(14) as a varint
	var actInt byte
	err = binary.Read(buf2, binary.LittleEndian, &actInt)
	ok(t, err)
	equals(t, int32(14), int32(actInt))
}

func TestEncodeAndWriteVarInt32_TwoByteVal(t *testing.T) {
	intval := int32(-500)
	buf1 := new(bytes.Buffer)
	err := EncodeAndWriteVarInt32(buf1, intval)
	ok(t, err)
	buf2 := new(bytes.Buffer)
	err = EncodeAndWriteVarInt32(buf2, intval)
	ok(t, err)

	equals(t, 2, buf1.Len())
	equals(t, 2, buf2.Len())

	// test it via ReadVarIntAndDecode32
	decoded, err := ReadVarIntAndDecode32(buf1)
	ok(t, err)
	equals(t, intval, decoded)

	// test the directly expected value
	// -500 => 999 zigzag encoded, which is 0x03e7
	//   0x03     0xe7
	// 00000011 11100111  regular int16 (big endian)
	// 11100111 00000111  varint encoded (little endian)
	//   0xe7     0x07
	expBytes := []byte{0xe7, 0x07}
	equals(t, expBytes, buf2.Bytes())
}

func TestEncodeAndWriteVarInt32_ThreeByteVal(t *testing.T) {
	intval := (int32(Max3Byte) / 2) - 1 // have divide by 2, since gets doubled when zigzag encoded
	buf := new(bytes.Buffer)
	err := EncodeAndWriteVarInt32(buf, intval)
	ok(t, err)

	equals(t, 3, buf.Len())

	// test it via ReadVarIntAndDecode32
	decoded, err := ReadVarIntAndDecode32(buf)
	ok(t, err)
	equals(t, intval, decoded)
}

func TestEncodeAndWriteVarInt32_FourByteVal(t *testing.T) {
	intval := (int32(Max4Byte) / 2) - 1 // have divide by 2, since gets doubled when zigzag encoded
	buf := new(bytes.Buffer)
	err := EncodeAndWriteVarInt32(buf, intval)
	ok(t, err)

	equals(t, 4, buf.Len())

	// test it via ReadVarIntAndDecode32
	decoded, err := ReadVarIntAndDecode32(buf)
	ok(t, err)
	equals(t, intval, decoded)
}

func TestEncodeAndWriteVarInt64_FiveByteVal(t *testing.T) {
	intval := (int64(Max5Byte) / 2) - 1 // have divide by 2, since gets doubled when zigzag encoded
	buf := new(bytes.Buffer)
	err := EncodeAndWriteVarInt64(buf, intval)
	ok(t, err)

	equals(t, 5, buf.Len())

	// test it via ReadVarIntAndDecode64
	decoded, err := ReadVarIntAndDecode64(buf)
	ok(t, err)
	equals(t, intval, decoded)
}
