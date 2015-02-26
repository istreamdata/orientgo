package varint

import (
	"bytes"
	"encoding/binary"

	"testing"
)

/* ---[ ReadVarInt for int32 results ]--- */

func TestReadVarInt1ByteRandomInputA(t *testing.T) {
	bs := []byte{0x39}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x39, 0x0, 0x0, 0x0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt1ByteAllOnes(t *testing.T) {
	bs := []byte{0x7f}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x7f}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt1ByteAllZeros(t *testing.T) {
	bs := []byte{0x0}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt2BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x70}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x00, 0x07, 0xf0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt3BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x70}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x03, 0xc7, 0xf0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt3BytesRandomInputB(t *testing.T) {
	bs := []byte{0xdb, 0xaa, 0x23}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x00, 0x16, 0xd5, 0x23}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt3BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0x7f}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x1f, 0xff, 0xff}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt3BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x0}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt4BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x70}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x01, 0xe3, 0xc7, 0xf0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt4BytesRandomInputB(t *testing.T) {
	bs := []byte{0x95, 0xbd, 0xcc, 0x46}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x02, 0xaf, 0x66, 0x46}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt4BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0xff, 0x7f}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0f, 0xff, 0xff, 0xff}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt4BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x0}
	actualInt, err := ReadVarIntToUint32(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

/* ---[ ReadVarInt for int64 results ]--- */

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
	bs := []byte{0xaa, 0xc1, 0xcc, 0x83, 0x7e}
	actualInt, err := ReadVarIntToUint64(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x02, 0xa8, 0x33, 0x01, 0xfe}
	var expectedInt uint64
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt5BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0xff, 0xff, 0x7f}
	actualInt, err := ReadVarIntToUint64(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x07, 0xff, 0xff, 0xff, 0xff}
	var expectedInt uint64
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarInt5BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x80, 0x0}
	actualInt, err := ReadVarIntToUint64(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	var expectedInt uint64
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestReadVarIntAndZigzagDecode(t *testing.T) {
	bs := []byte{0x64} // uint value 100

	var (
		zzencoded uint32
		actualVal int32
		err       error
	)
	zzencoded, err = ReadVarIntToUint32(bs)
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
	err = ReadVarInt(bs, &zzreadback)
	ok(t, err)
	equals(t, zzorig, zzreadback)

	// finally zigzag decode back to orig
	result = ZigzagDecodeInt32(zzreadback)
	equals(t, orig, result)
}

func TestReadVarInt(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x70}
	var actual uint32
	err := ReadVarInt(bs, &actual)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x03, 0xc7, 0xf0}
	var expectedInt uint32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.LittleEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actual)
}
