package varint

import (
	"bytes"
	"fmt"

	"github.com/quux00/ogonori/oerror"
)

//
// varint.ReadBytes, like rw.ReadBytes, first reads a length from the
// input buffer and then that number of bytes into a []byte from the
// input buffer. The difference is that the integer indicating the length
// of the byte array to follow is a zigzag encoded varint.
//
func ReadBytes(buf *bytes.Buffer) ([]byte, error) {
	// an encoded varint give the length of the remaining byte array
	sz, err := ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, err
	}

	if sz == 0 {
		return nil, nil
	}

	if sz < 0 {
		return nil, fmt.Errorf("Error in varint.ReadBytes: size of bytes was less than zero: %v", sz)
	}

	size := int(sz)
	data := buf.Next(size)
	if len(data) != size {
		return nil, oerror.IncorrectNetworkRead{Expected: size, Actual: len(data)}
	}
	return data, nil
}

//
// varint.ReadString, like rw.ReadString, first reads a length from the
// input buffer and then that number of bytes (of ASCII chars) into a string
// from the input buffer. The difference is that the integer indicating the
// length of the byte array to follow is a zigzag encoded varint.
//
func ReadString(buf *bytes.Buffer) (string, error) {
	bs, err := ReadBytes(buf)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
