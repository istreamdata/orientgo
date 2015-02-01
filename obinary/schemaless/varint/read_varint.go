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
	"errors"
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
// ReadVarInt will read up to 8 bytes from the byte slice and convert
// the encoded varint into a uint and copy the value into the `data`
// field passed in.  Thus `data` should be of type *uint32 or *uint64.
// Any other types will cause an error to be returned. Note that this
// function does NOT do zigzag decoding - that must be called separately
// after this function.
//
func ReadVarInt(bs []byte, data interface{}) error {
	switch data.(type) {
	case *uint32:
		v, err := ReadVarIntToUint32(bs)
		if err != nil {
			return err
		}
		*data.(*uint32) = v
		return nil

	case *uint64:
		v, err := ReadVarIntToUint64(bs)
		if err != nil {
			return err
		}
		*data.(*uint64) = v
		return nil

	default:
		return errors.New("Must pass in pointer to uint32 or uint64.")
	}
	return nil
}

func ensure4Bytes(bs []byte) []byte {
	if len(bs) == 1 {
		return []byte{0x0, 0x0, 0x0, bs[0]}
	} else if len(bs) == 2 {
		return []byte{0x0, 0x0, bs[0], bs[1]}
	} else if len(bs) == 3 {
		return []byte{0x0, bs[0], bs[1], bs[2]}
	} else {
		return bs
	}
}

//
// ReadVarInt4Bytes DOCUMENT ME
//
func ReadVarIntToUint32(bs []byte) (uint32, error) {
	bs = ensure4Bytes(bs)
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

func ensure8Bytes(bs []byte) []byte {
	if len(bs) == 5 {
		return []byte{0x0, 0x0, 0x0, bs[0], bs[1], bs[2], bs[3], bs[4]}
	} else if len(bs) == 6 {
		return []byte{0x0, 0x0, bs[0], bs[1], bs[2], bs[3], bs[4], bs[5]}
	} else if len(bs) == 7 {
		return []byte{0x0, bs[0], bs[1], bs[2], bs[2], bs[3], bs[4], bs[5], bs[6]}
	} else {
		return bs
	}
}

func ReadVarIntToUint64(bs []byte) (uint64, error) {
	bs = ensure8Bytes(bs)
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
