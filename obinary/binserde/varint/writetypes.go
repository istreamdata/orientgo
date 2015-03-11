package varint

import (
	"bytes"
	"fmt"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/oerror"
)

//
// varint.WriteBytes, like rw.WriteBytes, first reads a length from the
// input buffer and then that number of bytes into a []byte from the
// input buffer. The difference is that the integer indicating the length
// of the byte array to follow is a zigzag encoded varint.
//
func WriteBytes(buf *bytes.Buffer, bs []byte) (err error) {
	sz := int64(len(bs))
	if sz <= int64(constants.MaxInt) {
		err = EncodeAndWriteVarInt32(buf, int32(sz))
	} else {
		err = EncodeAndWriteVarInt64(buf, sz)
	}
	if err != nil {
		return oerror.NewTrace(err)
	}

	n, err := buf.Write(bs)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// an encoded varint give the length of the remaining byte array
	if n != int(sz) {
		return fmt.Errorf("Error in varint.WriteBytes: size of bytes written was less than byte slice size: %v", n)
	}
	return nil
}

//
// varint.WriteString, like rw.WriteString, first reads a length from the
// input buffer and then that number of bytes (of ASCII chars) into a string
// from the input buffer. The difference is that the integer indicating the
// length of the byte array to follow is a zigzag encoded varint.
//
func WriteString(buf *bytes.Buffer, s string) error {
	return WriteBytes(buf, []byte(s))
}
