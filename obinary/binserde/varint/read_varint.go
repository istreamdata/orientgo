//
// Package varint is used for the OrientDb schemaless serialization
// where variable size integers are used with zigzag encoding to
// convert negative integers to a positive unsigned int format so
// that smaller integers (whether negative or positive) can be transmitted
// in less than 4 bytes on the wire.  The variable length zigzag encoding
// used by OrientDB is the same as that used for Google's Protocol Buffers
// and is documented here:
// https://developers.google.com/protocol-buffers/docs/encoding?csw=1
//
package varint

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/quux00/ogonori/oerror"
)

//
// IsFinalVarIntByte checks the high bit of byte `b` to determine
// whether it is the last byte in an OrientDB varint encoding.
// If the high bit is zero, true is returned.
//
func IsFinalVarIntByte(b byte) bool {
	return (b >> 7) == 0x0
}

//
// ReadVarIntAndDecode32 reads a varint from buf to a uint32
// and then zigzag decodes it to an int32 value.
//
func ReadVarIntAndDecode32(buf io.Reader) (int32, error) {
	encodedLen, err := ReadVarIntToUint32(buf)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}
	return ZigzagDecodeInt32(encodedLen), nil
}

//
// ReadVarIntAndDecode64 reads a varint from buf to a uint64
// and then zigzag decodes it to an int64 value.
//
func ReadVarIntAndDecode64(buf io.Reader) (int64, error) {
	encodedLen, err := ReadVarIntToUint64(buf)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}
	return ZigzagDecodeInt64(encodedLen), nil
}

//
// ReadVarIntToUint32 reads a variable length integer from the input buffer.
// The inflated integer is written is returned as a uint32 value.
// This method only "inflates" the varint into a uint32; it does NOT
// zigzag decode it.
//
func ReadVarIntToUint32(buf io.Reader) (uint32, error) {
	var (
		bs  []byte
		a   uint32
		err error
	)

	bs, err = extractAndPadNBytes(buf, 4)
	if err != nil {
		return uint32(0), err
	}
	vintbuf := bytes.NewBuffer(bs)

	err = binary.Read(vintbuf, binary.LittleEndian, &a)
	if err != nil {
		return uint32(0), err
	}

	b := a >> 1
	c := a >> 2
	d := a >> 3

	ma := uint32(0x7f)
	mb := uint32(0x3f80)
	mc := uint32(0x1fc000)
	md := uint32(0x0fe00000)

	i := a & ma
	j := b & mb
	k := c & mc
	l := d & md

	return (i | j | k | l), nil
}

//
// ReadVarIntToUint64 reads a variable length integer from the input buffer.
// The inflated integer is written is returned as a uint64 value.
// This method only "inflates" the varint into a uint64; it does NOT
// zigzag decode it.
//
func ReadVarIntToUint64(buf io.Reader) (uint64, error) {
	var (
		bs  []byte
		a   uint64
		err error
	)

	bs, err = extractAndPadNBytes(buf, 8)
	if err != nil {
		return uint64(0), err
	}
	vintbuf := bytes.NewBuffer(bs)
	err = binary.Read(vintbuf, binary.LittleEndian, &a)
	if err != nil {
		return uint64(0), err
	}

	b := a >> 1
	c := a >> 2
	d := a >> 3
	e := a >> 4
	f := a >> 5
	g := a >> 6
	h := a >> 7

	ma := uint64(0x7f)
	mb := uint64(0x3f80)
	mc := uint64(0x1fc000)
	md := uint64(0x0fe00000)
	me := uint64(0x07f0000000)
	mf := uint64(0x03f800000000)
	mg := uint64(0x01fc0000000000)
	mh := uint64(0xfe000000000000)

	// showing the shift and masks:

	// 1hhhhhhh 1ggggggg 1fffffff 1eeeeeee 1ddddddd 1ccccccc 1bbbbbbb 0aaaaaaa  a
	// 00000000 00000000 00000000 00000000 00000000 00000000 00000000 01111111  ma

	// 01hhhhhh h1gggggg g1ffffff f1eeeeee e1dddddd d1cccccc c1bbbbbb b0aaaaaa  b  a >> 1
	// 00000000 00000000 00000000 00000000 00000000 00000000 00111111 10000000  mb
	//                                                          0x3f    0x80

	// 001hhhhh hh1ggggg gg1fffff ff1eeeee ee1ddddd dd1ccccc cc1bbbbb bb0aaaaa  c  a >> 2
	// 00000000 00000000 00000000 00000000 00000000 00011111 11000000 00000000  mc
	//                                                0x1f     0xf0     0x0

	// 0001hhhh hhh1gggg ggg1ffff fff1eeee eee1dddd ddd1cccc ccc1bbbb bbb0aaaa  d  a >> 3
	// 00000000 00000000 00000000 00000000 00001111 11100000 00000000 00000000  md
	//                                       0x0f     0xe0      0x0     0x0

	// 00001hhh hhhh1ggg gggg1fff ffff1eee eeee1ddd dddd1ccc cccc1bbb bbbb0aaa  e  a >> 4
	// 00000000 00000000 00000000 00000111 11110000 00000000 00000000 00000000  me
	//                              0x07     0xf0     0x0       0x0     0x0

	// 000001hh hhhhh1gg ggggg1ff fffff1ee eeeee1dd ddddd1cc ccccc1bb bbbbb0aa  f  a >> 5
	// 00000000 00000000 00000011 11111000 00000000 00000000 00000000 00000000  mf
	//                     0x03     0xf8      0x0      0x0      0x0      0x0

	// 0000001h hhhhhh1g gggggg1f ffffff1e eeeeee1d dddddd1c cccccc1b bbbbbb0a  g  a >> 6
	// 00000000 00000001 11111100 00000000 00000000 00000000 00000000 00000000  mg
	//            0x01     0xfc     0x0       0x0      0x0      0x0     0x0

	// 00000001 hhhhhhh1 ggggggg1 fffffff1 eeeeeee1 ddddddd1 ccccccc1 bbbbbbb0  h  a >> 7
	// 00000000 11111110 00000000 00000000 00000000 00000000 00000000 00000000  mh
	//            0xfe      0x0       0x0      0x0      0x0      0x0     0x0

	xa := a & ma
	xb := b & mb
	xc := c & mc
	xd := d & md
	xe := e & me
	xf := f & mf
	xg := g & mg
	xh := h & mh

	return (xa | xb | xc | xd | xe | xf | xg | xh), nil
}

//
// extractAndPadNBytes reads up to N bytes from buf, reading
// them into a []byte, retaining little endian order.
// If high bit is set in a byte before reading N bytes,
// the remaining bytes in the []byte are left as 0x0.
// Note: n should only be 4 or 8, but this not checked.
//
func extractAndPadNBytes(buf io.Reader, n int) ([]byte, error) {
	encbytes := make([]byte, n)
	bs := make([]byte, 1) // NEW

	for i := 0; i < n; i++ {
		// b, err := buf.ReadByte()
		_, err := buf.Read(bs)
		if err != nil {
			if err == io.EOF {
				return encbytes,
					fmt.Errorf("varint.extractAndPadNBytes could not find final varint byte in first %d bytes", i-1)
			}
			return encbytes, err
		}
		encbytes[i] = bs[0]
		if IsFinalVarIntByte(bs[0]) {
			return encbytes, nil
		}
	}

	// if get here then read n bytes from buf, but none had the high
	// bit set to zero - unexpected condition
	return encbytes,
		fmt.Errorf("varint.extractAndPadNBytes could not find final varint byte in first %d bytes", n)
}
