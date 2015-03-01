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
	"errors"
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
func ReadVarIntAndDecode32(buf *bytes.Buffer) (int32, error) {
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
func ReadVarIntAndDecode64(buf *bytes.Buffer) (int64, error) {
	encodedLen, err := ReadVarIntToUint64(buf)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}
	return ZigzagDecodeInt64(encodedLen), nil
}

func ReadVarIntToUint64(buf *bytes.Buffer) (uint64, error) {
	panic("ReadVarIntToUint64 Not Yet Implemented") // TODO: impl me (is this ever needed?)
}

//
// ReadVarIntToUint32 reads a variable length integer from the input buffer.
// The inflated integer is written is returned as a uint32 value.
// This method only "inflates" the varint into a uint32; it does NOT
// zigzag decode it.
//
func ReadVarIntToUint32(buf *bytes.Buffer) (uint32, error) {
	var (
		bs  []byte
		a   uint32
		err error
	)

	bs, err = extract4Bytes(buf)
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
// extract4Bytes reads up to 4 bytes from buf, reading
// them into a []byte, retaining little endian order.
// If high bit is set in a byte before reading 4 bytes,
// the remaining bytes in the []byte are left as 0x0.
//
func extract4Bytes(buf *bytes.Buffer) ([]byte, error) {
	encbytes := make([]byte, 4)

	for i := 0; i < 4; i++ {
		b, err := buf.ReadByte()
		if err != nil {
			if err == io.EOF {
				return encbytes, nil
			} else {
				return encbytes, err
			}
		}
		encbytes[i] = b
		if IsFinalVarIntByte(b) {
			return encbytes, nil
		}
	}

	// if get here then read 4 bytes from buf, but none had the high
	// bit set to zero - unexpected condition
	return encbytes,
		errors.New("varint.extract4Bytes could not find final varint byte in first 4 bytes")
}
