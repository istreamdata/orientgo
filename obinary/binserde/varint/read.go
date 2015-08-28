// Package varint is used for the OrientDB schemaless serialization
// where variable size integers are used with zigzag encoding to
// convert negative integers to a positive unsigned int format so
// that smaller integers (whether negative or positive) can be transmitted
// in less than 4 bytes on the wire.  The variable length zigzag encoding
// used by OrientDB is the same as that used for Google's Protocol Buffers
// and is documented here:
// https://developers.google.com/protocol-buffers/docs/encoding?csw=1
package varint

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ByteReader interface {
	io.Reader
	io.ByteReader
}

// ReadVarIntAndDecode64 reads a varint from r to a uint64
// and then zigzag decodes it to an int64 value.
func ReadVarint(r io.ByteReader) int64 {
	if v, err := binary.ReadVarint(r); err != nil {
		panic(err)
	} else {
		return v
	}
}

// ReadVarIntToUint reads a variable length integer from the input buffer.
// The inflated integer is written is returned as a uint64 value.
// This method only "inflates" the varint into a uint64; it does NOT
// zigzag decode it.
func ReadUvarint(r io.ByteReader) uint64 {
	if v, err := binary.ReadUvarint(r); err != nil {
		panic(err)
	} else {
		return v
	}
}

// varint.ReadBytes, like rw.ReadBytes, first reads a length from the
// input buffer and then that number of bytes into a []byte from the
// input buffer. The difference is that the integer indicating the length
// of the byte array to follow is a zigzag encoded varint.
func ReadBytes(buf ByteReader) []byte {
	// an encoded varint give the length of the remaining byte array
	lenbytes := ReadVarint(buf)

	if lenbytes == 0 {
		return nil
	} else if lenbytes < 0 {
		panic(fmt.Errorf("Error in varint.ReadBytes: size of bytes was less than zero: %v", lenbytes))
	}

	data := make([]byte, int(lenbytes))
	if _, err := io.ReadFull(buf, data); err != nil {
		panic(err)
	}
	return data
}

// varint.ReadString, like rw.ReadString, first reads a length from the
// input buffer and then that number of bytes (of ASCII chars) into a string
// from the input buffer. The difference is that the integer indicating the
// length of the byte array to follow is a zigzag encoded varint.
func ReadString(buf ByteReader) string {
	return string(ReadBytes(buf))
}
