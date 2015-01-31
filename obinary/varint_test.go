package obinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"testing"
)

/* ---[ VarInt for int32 results ]--- */

func TestVarInt2BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x70}
	actualInt, err := VarInt2Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x00, 0x07, 0xf0}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt3BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x70}
	actualInt, err := VarInt3Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x03, 0xc7, 0xf0}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt3BytesRandomInputB(t *testing.T) {
	bs := []byte{0xdb, 0xaa, 0x23}
	actualInt, err := VarInt3Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x00, 0x16, 0xd5, 0x23}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt3BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0x7f}
	actualInt, err := VarInt3Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x1f, 0xff, 0xff}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt3BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x0}
	actualInt, err := VarInt3Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt4BytesRandomInputA(t *testing.T) {
	bs := []byte{0x8f, 0x8f, 0x8f, 0x70}
	actualInt, err := VarInt4Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x01, 0xe3, 0xc7, 0xf0}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt4BytesRandomInputB(t *testing.T) {
	bs := []byte{0x95, 0xbd, 0xcc, 0x46}
	actualInt, err := VarInt4Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x02, 0xaf, 0x66, 0x46}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt4BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0xff, 0x7f}
	actualInt, err := VarInt4Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0f, 0xff, 0xff, 0xff}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt4BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x0}
	actualInt, err := VarInt4Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0}
	var expectedInt int32
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

/* ---[ VarInt for int64 results ]--- */

// OrientDB varint encoding (5 bytes):
//    0        1        2         3       4
// 1aaaaaaa 1bbbbbbb 1ccccccc 1ddddddd 0eeeeeee  // starting input
// 000000aa aaaaabbb bbbbcccc cccddddd ddeeeeee  // consolidated output
// 10000000 10000000 10000000 10000000 00000000  // TestVarInt5BytesAllZeros input
// 11111111 11111111 11111111 11111111 01111111  // TestVarInt5BytesAllOnes input
// 00000111 11111111 11111111 11111111 11111111  // TestVarInt5BytesAllOnes output
//   0x07
// 10101010 11000001 11001100 10000011 01111110  // TestVarInt5BytesRandomA input
//   0xaa     0xc1     0xcc     0x83     0x7e
// 00000010 10101000 00110011 00000001 11111110  // TestVarInt5BytesRandomA output
//   0x02     0xa8     0x33     0x01     0xfe

func TestVarInt5BytesRandomInputA(t *testing.T) {
	bs := []byte{0xaa, 0xc1, 0xcc, 0x83, 0x7e}
	actualInt, err := VarInt5Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x02, 0xa8, 0x33, 0x01, 0xfe}
	var expectedInt int64
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt5BytesAllOnes(t *testing.T) {
	bs := []byte{0xff, 0xff, 0xff, 0xff, 0x7f}
	actualInt, err := VarInt5Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x07, 0xff, 0xff, 0xff, 0xff}
	var expectedInt int64
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

func TestVarInt5BytesAllZeros(t *testing.T) {
	bs := []byte{0x80, 0x80, 0x80, 0x80, 0x0}
	actualInt, err := VarInt5Bytes(bs)
	ok(t, err)

	expectedBytes := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	var expectedInt int64
	buf := bytes.NewBuffer(expectedBytes)
	err = binary.Read(buf, binary.BigEndian, &expectedInt)
	ok(t, err)

	equals(t, expectedInt, actualInt)
}

///////////////////////////////////////////////////
/* ---[ OLD - use the actual UTF-8 encoding ]--- */

func TestConsolidateToShort1(t *testing.T) {
	// b0: 110'10001
	// b1: 10'100001
	bs := []byte{byte(0xd1), byte(0xa1)}

	shortval, err := ConsolidateToShort(bs)
	if err != nil {
		log.Fatal(err)
	}
	equals(t, int16(1121), shortval)
	equals(t, int16(0x0461), shortval)
	// final expected: 00000100 01100001
}

func TestConsolidateToShort2(t *testing.T) {
	// b0: 110'11111
	// b1: 10'111111
	bs := []byte{byte(0xdf), byte(0xbf)}

	shortval, err := ConsolidateToShort(bs)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("max-two-bytes: %v\n", shortval/2)
	equals(t, int16(0x07ff), shortval)
	// final expected: 00000111 11111111
}

func TestConsolidateToShort3(t *testing.T) {
	// b0: 110'00000
	// b1: 10'000000
	bs := []byte{byte(0xc0), byte(0x80)}

	shortval, err := ConsolidateToShort(bs)
	if err != nil {
		log.Fatal(err)
	}
	equals(t, int16(0x0), shortval)
	// final expected: 00000000 00000000
}

func TestConsolidateToShort4(t *testing.T) {
	// b0: 110'00010
	// b1: 10'000100
	bs := []byte{byte(0xc2), byte(0x84)}

	shortval, err := ConsolidateToShort(bs)
	if err != nil {
		log.Fatal(err)
	}
	equals(t, int16(0x84), shortval)
	// final expected: 00000000 10000100
}
