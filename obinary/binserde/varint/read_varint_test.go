package varint

import (
	"bytes"
	"encoding/binary"

	"github.com/quux00/ogonori/obuf"

	"testing"
)

// see documentation in TestReadVarInt2BytesRandomInputA
// for details on how I set up these tests and
// how varint encoding/ordering goes

func TestIsFinalVarIntByte(t *testing.T) {
	bs := []byte{0xff, 0xe0, 0x81, 0x7f}
	assert(t, !IsFinalVarIntByte(bs[0]), "")
	assert(t, !IsFinalVarIntByte(bs[1]), "")
	assert(t, !IsFinalVarIntByte(bs[2]), "")
	assert(t, IsFinalVarIntByte(bs[3]), "")
}

/* ---[ ReadVarInt for int32 results ]--- */

func TestReadVarInt1ByteRandomInputA(t *testing.T) {
	bs := []byte{0x39}
	buf := obuf.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x39}
	var expectedUint uint32
	buf2 := bytes.NewBuffer(expectedBytes)
	// I'm specifying BigEndian here because that's how I
	// ordered the "expectedBytes" - it is unrelated to how the
	// actual varint is ordered (which is LittleEndian)
	err = binary.Read(buf2, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt1ByteAllZeros(t *testing.T) {
	bs := []byte{0x0}
	buf := obuf.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedUint uint32
	buf2 := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf2, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt1ByteAllOnes(t *testing.T) {
	bs := []byte{0x7f}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x7f}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt2BytesRandomInput(t *testing.T) {
	bs := []byte{0x8f, 0x70}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x00, 0x38, 0x0f}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	// I'm specifying BigEndian here because that's how I
	// ordered the "expectedBytes" - it is unrelated to how the
	// actual varint is ordered (which is LittleEndian)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8f        0x70
	//     [1000 1111]  [0111 0000]
	// idx:     0            1
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0000]  [1000 1111]
	// Now remove the high bit from each:
	//      111 0000  000 1111
	// then squash together - the highest bits get set to zero
	//     0011 1000  0000 1111
	// then add 0x0 bytes for the top bytes (using big-endian order)
	//      0x0         0x0         0x38         0x0f
	//  [0000 0000] [0000 0000]  [0011 1000]  [0000 1111]
	//
}

func TestReadVarInt3BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x70}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x1c, 0x07, 0x8f}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt3BytesRandomInputB(t *testing.T) {
	bs := []byte{0xdb, 0xaa, 0x23}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x08, 0xd5, 0x5b}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt3BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0x7f}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x1f, 0xff, 0xff}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt3BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x0}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt4BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x70}
	buf := obuf.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0e, 0x03, 0xc7, 0x8f}
	var expectedUint uint32
	buf2 := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf2, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8f        0x8f         0x8f         0x70
	//     [1000 1111]  [1000 1111]  [1000 1111]  [0111 0000]
	// idx:     0            1            2            3
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0000]  [1000 1111]  [1000 1111]  [1000 1111]
	// Now remove the high bit from each:
	//      111 0000  000 1111  000 1111  000 1111
	// then squash together - the highest bits get set to zero
	//     [0000 1110]  [0000 0011]  [1100 0111]  [1000 1111]
	//        0x0e         0x03         0xc7         0x8f
	//
}

func TestReadVarInt4BytesRandomInputB(t *testing.T) {
	bs := []byte{0x95, 0xbd, 0xcc, 0x46}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x08, 0xd3, 0x1e, 0x95}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x95        0xbd         0xcc        0x46
	//     [1001 0101]  [1011 1101]  [1100 1100]  [0100 0110]
	// idx:     0            1            2            3
	//
	// To interpret the varint, let's flip to big endian order:
	//     [0100 0110]  [1100 1100]  [1011 1101]  [1001 0101]
	// Now remove the high bit from each:
	//       100 0110     100 1100     011 1101     001 0101
	// then squash together - the highest bits get set to zero
	//     [0000 1000]  [1101 0011]  [0001 1110]  [1001 0101]
	//        0x08         0xd3         0x1e         0x95
	//
}

func TestReadVarInt4BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0xff, 0x7f}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0f, 0xff, 0xff, 0xff}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt4BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x0}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedUint uint32
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

/* ---[ ReadVarIntAndDecode32 ]--- */

func TestReadVarIntAndDecode32_1Byte_Positive(t *testing.T) {
	bs := []byte{0x1a} // = 26 (un-zigzag-decoded)
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)
	zigzagDecodedInt := ZigzagDecodeInt32(actualUint)

	buf = bytes.NewBuffer(bs)
	actualInt, err := ReadVarIntAndDecode32(buf)
	ok(t, err)

	equals(t, zigzagDecodedInt, actualInt)
	equals(t, uint32(26), actualUint)
	equals(t, int32(13), actualInt)
}

func TestReadVarIntAndDecode32_2Bytes_Positive(t *testing.T) {
	bs := []byte{0x8c, 0x01}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)
	zigzagDecodedInt := ZigzagDecodeInt32(actualUint)

	buf = bytes.NewBuffer(bs)
	actualInt, err := ReadVarIntAndDecode32(buf)
	ok(t, err)

	equals(t, zigzagDecodedInt, actualInt)
	equals(t, uint32(140), actualUint)
	equals(t, int32(70), actualInt)
}

func TestReadVarIntAndDecode32_2Bytes_Negative(t *testing.T) {
	bs := []byte{0x8d, 0x01}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)
	zigzagDecodedInt := ZigzagDecodeInt32(actualUint)

	buf = bytes.NewBuffer(bs)
	actualInt, err := ReadVarIntAndDecode32(buf)
	ok(t, err)

	equals(t, zigzagDecodedInt, actualInt)
	equals(t, uint32(141), actualUint)
	equals(t, int32(-71), actualInt)
}

func TestReadVarIntAndDecode32_3Bytes_Negative(t *testing.T) {
	bs := []byte{0x8d, 0x81, 0x01}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)
	zigzagDecodedInt := ZigzagDecodeInt32(actualUint)

	buf = bytes.NewBuffer(bs)
	actualInt, err := ReadVarIntAndDecode32(buf)
	ok(t, err)

	equals(t, zigzagDecodedInt, actualInt)
	equals(t, uint32(16525), actualUint)
	equals(t, int32(-8263), actualInt)
}

func TestReadVarIntAndDecode32_4Bytes_Zero(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x00}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint32(buf)
	ok(t, err)
	zigzagDecodedInt := ZigzagDecodeInt32(actualUint)

	buf = bytes.NewBuffer(bs)
	actualInt, err := ReadVarIntAndDecode32(buf)
	ok(t, err)

	equals(t, zigzagDecodedInt, actualInt)
	equals(t, uint32(0), actualUint)
	equals(t, int32(0), actualInt)
}

// /* ---[ ReadVarInt for int64 results ]--- */

// OrientDB varint encoding (5 bytes):
//    0        1        2         3       4
// 1aaaaaaa 1bbbbbbb 1ccccccc 1ddddddd 0eeeeeee  // starting input
// 000000aa aaaaabbb bbbbcccc cccddddd ddeeeeee  // consolidated output
// 10000000 10000000 10000000 10000000 00000000  // TestReadVarInt5BytesAllZeros input
// 11111111 11111111 11111111 11111111 01111111  // TestReadVarInt5BytesAllOnes input
// 00000111 11111111 11111111 11111111 11111111  // TestReadVarInt5BytesAllOnes output
//   0x07
// 10101010 11000001 11001100 10000011 01111110  // TestReadVarInt5BytesRandomA input
//   0xaa     0xc1     0xcc     0x83     0x7e
// 00000010 10101000 00110011 00000001 11111110  // TestReadVarInt5BytesRandomA output
//   0x02     0xa8     0x33     0x01     0xfe

func TestReadVarInt5BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x8f, 0x70}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x07, 0x01, 0xe3, 0xc7, 0x8f}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8f        0x8f        0x8f         0x8f          0x70
	//     [1000 1111] [1000 1111]  [1000 1111]  [1000 1111]  [0111 0000]
	// idx:     0            1            2            3           4
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0000]  [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111]
	// Now remove the high bit from each:
	//       111 0000     000 1111     000 1111     000 1111     000 1111
	// then squash together - the highest bits get set to zero
	//     [0000 0111]  [0000 0001]  [1110 0011]  [1100 0111]   [1000 1111]
	//        0x07         0x01         0xe3         0xc7          0x8f
	//

}

func TestReadVarInt6BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x8f, 0x8f, 0x70}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x03, 0x80, 0xf1, 0xe3, 0xc7, 0x8f}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8f        0x8f         0x8f        0x8f         0x8f          0x70
	//     [1000 1111] [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111]  [0111 0000]
	// idx:     0            1            2            3           4            5
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0000]  [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111]
	// Now remove the high bit from each:
	//       111 0000     000 1111     000 1111     000 1111     000 1111     000 1111
	// then squash together - the highest bits get set to zero
	//     [0000 0011]  [1000 0000]  [1111 0001]  [1110 0011]   [1100 0111]   [1000 1111]
	//        0x03         0x80         0xf1         0xe3          0xc7          0x8f
	//
}

func TestReadVarInt7BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x8f, 0x8f, 0x8f, 0x70}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x01, 0xc0, 0x78, 0xf1, 0xe3, 0xc7, 0x8f}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8f         0x8f        0x8f         0x8f        0x8f         0x8f          0x70
	//     [1000 1111]  [1000 1111] [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111]  [0111 0000]
	// idx:     0            1            2            3           4            5            6
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0000]  [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111] [1000 1111]  [1000 1111]
	// Now remove the high bit from each:
	//       111 0000     000 1111     000 1111     000 1111     000 1111    000 1111    000 1111
	// then squash together - the highest bits get set to zero
	//     [0000 0001]  [1100 0000]  [0111 1000]  [1111 0001]   [1110 0011]   [1100 0111]  [1000 1111]
	//        0x01         0xc0         0x78         0xf1          0xe3          0xc7          0x8f
	//
}

func TestReadVarInt7BytesRandomInputB(t *testing.T) {
	bs := []byte{0x8d, 0xcc, 0x90, 0x80, 0x95, 0x81, 0x74}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x01, 0xd0, 0x09, 0x50, 0x04, 0x26, 0x0d}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8d         0xcc        0x90         0x80        0x95         0x81         0x74
	//     [1000 1101]  [1100 1100] [1001 0000]  [1000 0000]  [1001 0101]  [1000 0001]  [0111 0100]
	// idx:     0            1            2            3           4            5            6
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0100]  [1000 0001]  [1001 0101]  [1000 0000]  [1001 0000] [1100 1100]  [1000 1101]
	// Now remove the high bit from each:
	//       111 0100     000 0001     001 0101     000 0000     001 0000    100 1100    000 1101
	// then squash together - the highest bits get set to zero
	//     [0000 0001]  [1101 0000]  [0000 1001]  [0101 0000]   [0000 0100]   [0010 0110]  [0000 1101]
	//        0x01         0xd0         0x09         0x50          0x04          0x26         0x0d
	//
}

func TestReadVarInt8BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x8f, 0x8f, 0x8f, 0x8f, 0x70}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0xe0, 0x3c, 0x78, 0xf1, 0xe3, 0xc7, 0x8f}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)

	// ----------------------------
	// explanation of this example:
	// ----------------------------
	// Varints are encoded in little endian order. The varint input
	// for this example is:
	//         0x8f         0x8f         0x8f        0x8f         0x8f        0x8f         0x8f          0x70
	//     [1000 1111]  [1000 1111]  [1000 1111] [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111]  [0111 0000]
	// idx:     0            1            2            3           4            5            6            7
	//
	// To "inflate" the varint, let's flip to big endian order:
	//     [0111 0000]  [1000 1111]  [1000 1111]  [1000 1111]  [1000 1111] [1000 1111]  [1000 1111]  [1000 1111]
	// Now remove the high bit from each:
	//       111 0000     000 1111     000 1111     000 1111     000 1111    000 1111     000 1111     000 1111
	// then squash together - the highest bits get set to zero
	//     [1110 0000]  [0011 1100]  [0111 1000]  [1111 0001]   [1110 0011]   [1100 0111]  [1000 1111]
	//        0xe0         0x3c         0x78         0xf1          0xe3          0xc7          0x8f
	//
}

func TestReadVarInt5BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0xff, 0xff, 0x7f}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x07, 0xff, 0xff, 0xff, 0xff}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarInt5BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x80, 0x0}
	buf := bytes.NewBuffer(bs)
	actualUint, err := ReadVarIntToUint64(buf)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	var expectedUint uint64
	buf = bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedUint)
	ok(t, err)

	equals(t, expectedUint, actualUint)
}

func TestReadVarIntAndZigzagDecode(t *testing.T) {
	bs := []byte{0x64} // uint value 100

	var (
		zzencoded uint32
		actualVal int32
		err       error
	)
	buf := bytes.NewBuffer(bs)
	zzencoded, err = ReadVarIntToUint32(buf)
	ok(t, err)
	actualVal = ZigzagDecodeInt32(zzencoded)

	equals(t, uint32(100), zzencoded)
	equals(t, int32(50), actualVal)
}

func TestRoundTripFromWritingZZEncodedAndReadingBack(t *testing.T) {
	var (
		b          byte
		orig       int32
		zzorig     uint32
		zzreadback uint32
		result     int32
	)
	orig = int32(-18923)

	// first zigzag encode the orig val
	zzorig = ZigzagEncodeUInt32(orig)

	// write it to varint format
	buf := new(bytes.Buffer)
	err := WriteVarInt(buf, zzorig)
	ok(t, err)

	// read it from varint to regular int32 format
	bs := make([]byte, 0, 4)
	for {
		b, err = buf.ReadByte()
		ok(t, err)
		bs = append(bs, b)
		if IsFinalVarIntByte(b) {
			break
		}
	}
	buf = bytes.NewBuffer(bs)
	zzreadback, err = ReadVarIntToUint32(buf)
	ok(t, err)
	equals(t, zzorig, zzreadback)

	// finally zigzag decode back to orig
	result = ZigzagDecodeInt32(zzreadback)
	equals(t, orig, result)
}

func TestExtactAndPadNBytesErrorCondition_NoHighBitSetWithLessThan4Entries(t *testing.T) {
	// not a valid varint
	bs := []byte{0x80, 0x99} // none has the high bit set to zero
	buf := bytes.NewBuffer(bs)

	_, err := extractAndPadNBytes(buf, 4)
	assert(t, err != nil, "none had high bit set, so error should be returned")
}

func TestExtactAndPadNBytesErrorCondition_NoHighBitSetWith4Entries(t *testing.T) {
	// not a valid varint
	bs := []byte{0x80, 0x99, 0x80, 0x99} // none has the high bit set to zero
	buf := bytes.NewBuffer(bs)

	_, err := extractAndPadNBytes(buf, 4)
	assert(t, err != nil, "none had high bit set, so error should be returned")
}
