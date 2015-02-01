//
// Package varint is used for the OrientDb schemaless serialization
// where variable size integers are used with zigzag encoding to
// convert negative integers to a positive unsigned int format so
// that smaller integers (whether negative or positive) can be transmitted
// in less than 4 bytes on the wire.
//
package varint

import (
	"bytes"
	"encoding/binary"
)

// TODO: finish this if you want this to look more like encoding/binary in the stdlib
// func ReadVarInt(r io.Reader, data interface{}) error {
// 	switch data.(type) {
// 	case *int32:
// 		*data = readVi32(r)
// 	case *int64:
// 		return errors.New("*int64 case NOT YET IMPLEMENTED ...")
// 	default:
// 		return errors.New("Must pass in pointer to int32 or int64.")
// 	}
// 	return nil
// }

func ReadVarInt1Byte(bs []byte) (uint32, error) {
	bs4 := []byte{0x0, 0x0, 0x0, bs[0]}
	var uintval uint32
	buf := bytes.NewBuffer(bs4)
	err := binary.Read(buf, binary.BigEndian, &uintval)
	if err != nil {
		return uint32(0), err
	}
	return uintval, nil
}

func ReadVarInt2Bytes(bs []byte) (uint32, error) {
	bs4 := []byte{0x0, 0x0, bs[0], bs[1]}
	return ReadVarInt4Bytes(bs4)
}

func ReadVarInt3Bytes(bs []byte) (uint32, error) {
	bs4 := []byte{0x0, bs[0], bs[1], bs[2]}
	return ReadVarInt4Bytes(bs4)
}

//
// ReadVarInt4Bytes DOCUMENT ME
//
func ReadVarInt4Bytes(bs []byte) (uint32, error) {
	var a uint32
	buf := bytes.NewBuffer(bs[0:4])
	err := binary.Read(buf, binary.BigEndian, &a)
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

func ReadVarInt5Bytes(bs []byte) (uint64, error) {
	bs8 := []byte{0x0, 0x0, 0x0, bs[0], bs[1], bs[2], bs[3], bs[4]}
	return ReadVarInt8Bytes(bs8)
}

func ReadVarInt8Bytes(bs []byte) (uint64, error) {
	var a uint64
	buf := bytes.NewBuffer(bs[0:8])
	err := binary.Read(buf, binary.BigEndian, &a)
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

	// masks to get the value bits from each section (a-h)
	ma := uint64(0x7f)
	mb := uint64(0x3f80)
	mc := uint64(0x1fc000)
	md := uint64(0x0fe00000)
	me := uint64(0x07f0000000)
	mf := uint64(0x03f800000000)
	mg := uint64(0x01fa0000000000)
	mh := uint64(0xfe000000000000)

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
