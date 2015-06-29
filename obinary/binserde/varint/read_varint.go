//
// Package varint is used for the OrientDB schemaless serialization
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
func ReadVarIntAndDecode32(r io.Reader) (int32, error) {
	encodedLen, err := ReadVarIntToUint(r)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}
	return ZigzagDecodeInt32(uint32(encodedLen)), nil
}

//
// ReadVarIntAndDecode64 reads a varint from r to a uint64
// and then zigzag decodes it to an int64 value.
//
func ReadVarIntAndDecode64(r io.Reader) (int64, error) {
	encodedLen, err := ReadVarIntToUint(r)
	if err != nil {
		return 0, oerror.NewTrace(err)
	}
	return ZigzagDecodeInt64(encodedLen), nil
}

//
// ReadVarIntToUint reads a variable length integer from the input buffer.
// The inflated integer is written is returned as a uint64 value.
// This method only "inflates" the varint into a uint64; it does NOT
// zigzag decode it.
//
func ReadVarIntToUint(r io.Reader) (uint64, error) {
	var (
		varbs []byte
		ba    [1]byte
		u     uint64
		n     int
		err   error
	)

	varbs = make([]byte, 0, 10)

	/* ---[ read in all varint bytes ]--- */
	for {
		n, err = r.Read(ba[:])
		if err != nil {
			return 0, oerror.NewTrace(err)
		}
		if n != 1 {
			return 0, oerror.IncorrectNetworkRead{Expected: 1, Actual: n}
		}
		varbs = append(varbs, ba[0])
		if IsFinalVarIntByte(ba[0]) {
			varbs = append(varbs, byte(0x0))
			break
		}
	}

	/* ---[ decode ]--- */
	var buf bytes.Buffer
	if len(varbs) == 1 {
		buf.WriteByte(varbs[0])

	} else {
		var right, left uint
		for i := 0; i < len(varbs)-1; i++ {
			right = uint(i) % 8
			left = 7 - right
			if i == 7 {
				continue
			}
			vbcurr := varbs[i]
			vbnext := varbs[i+1]

			x := vbcurr & byte(0x7f)
			y := x >> right
			z := vbnext << left
			buf.WriteByte(y | z)
		}
	}

	padTo8Bytes(&buf)
	err = binary.Read(&buf, binary.LittleEndian, &u)
	if err != nil {
		return 0, err
	}
	return u, nil
}

//
// padTo8Bytes writes as many additional 0x0 bytes to buf as
// necessary to make it 8 bytes long. If buf.Len() >= 8 then
// this function is a noop.
//
func padTo8Bytes(buf *bytes.Buffer) {
	rounds := 8 - buf.Len()
	for i := 0; i < rounds; i++ {
		buf.WriteByte(0x0)
	}
}
