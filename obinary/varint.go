package obinary // need to put in varint package

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func ConvertToInt(bs []byte) (int32, error) {
	if len(bs) > 4 {
		return int32(0), errors.New("byte slice is too long to convert to int32")
	}

	return int32(0), nil
}

func VarInt2Bytes(bs []byte) (int32, error) {
	bs4 := []byte{0x0, 0x0, bs[0], bs[1]}
	return VarInt4Bytes(bs4)
}

func VarInt3Bytes(bs []byte) (int32, error) {
	bs4 := []byte{0x0, bs[0], bs[1], bs[2]}
	return VarInt4Bytes(bs4)
}

func VarInt4Bytes(bs []byte) (int32, error) {
	var a int32
	buf := bytes.NewBuffer(bs[0:4])
	err := binary.Read(buf, binary.BigEndian, &a)
	if err != nil {
		return int32(0), err
	}

	b := a >> 1
	c := a >> 2
	d := a >> 3

	ma := int32(0x7f)
	mb := int32(0x3f80)
	mc := int32(0x1fc000)
	md := int32(0x0fe00000)

	i := a & ma
	j := b & mb
	k := c & mc
	l := d & md

	return (i | j | k | l), nil
}

func VarInt8Bytes(bs []byte) (int64, error) {
	var a int64
	buf := bytes.NewBuffer(bs[0:8])
	err := binary.Read(buf, binary.BigEndian, &a)
	if err != nil {
		return int64(0), err
	}

	b := a >> 1
	c := a >> 2
	d := a >> 3
	e := a >> 4
	f := a >> 5
	g := a >> 6
	h := a >> 7

	// masks to get the value bits from each section (a-h)
	ma := int64(0x7f)
	mb := int64(0x3f80)
	mc := int64(0x1fc000)
	md := int64(0x0fe00000)
	me := int64(0x07f0000000)
	mf := int64(0x03f800000000)
	mg := int64(0x01fa0000000000)
	mh := int64(0xfe000000000000)

	i := a & ma
	j := b & mb
	k := c & mc
	l := d & md
	m := e & me
	n := f & mf
	o := g & mg
	p := h & mh

	return (i | j | k | l | m | n | o | p), nil
}

func VarInt5Bytes(bs []byte) (int64, error) {
	bs8 := []byte{0x0, 0x0, 0x0, bs[0], bs[1], bs[2], bs[3], bs[4]}
	return VarInt8Bytes(bs8)
}

//////////////// //////// ////////

func ConsolidateToShort(bs []byte) (int16, error) {
	if len(bs) == 2 {
		return consolidateTwoBytes(bs)
	} else {
		return consolidateThreeBytes(bs)
	}
}

func consolidateThreeBytes(bs []byte) (int16, error) {
	// b0mask := byte(0x1f)
	// bNmask := byte(0x3f)

	return int16(0), nil
}

//
// ConsolidateToShort reads the first two bytes of the byte slice
// param, which is assumed to be a varint with UTF-8 style markers.
// The markers are stripped out and the 2 bytes are converted into
// an int16.  No zigzag decoding is done.
//
// TODO: should we pass []byte or bytes.Buffer ?
func consolidateTwoBytes(bs []byte) (int16, error) {

	// step 1: convert to int16
	var shortval int16
	buf := bytes.NewBuffer(bs)
	// TODO: still assuming BigEndian
	err := binary.Read(buf, binary.BigEndian, &shortval)
	if err != nil {
		return int16(0), err
	}

	// step 2: shift right by 2 to fill in the "10" markers in the second byte
	//         and zero out the 6 rightmost bits
	right2 := shortval >> 2
	b0bits := right2 & int16(0x7c0)

	// step 3: zero out all the high bits (leaving the 6 original rightmost bits intact)
	b1bits := shortval & int16(0x3f)

	// step 4: bitwise-or the outputs of step3 and 4 to get final val
	return (b0bits | b1bits), nil
}
