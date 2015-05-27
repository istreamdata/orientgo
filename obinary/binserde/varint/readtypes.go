package varint

import (
	"fmt"
	"io"

	"github.com/quux00/ogonori/oerror"
)

//
// varint.ReadBytes, like rw.ReadBytes, first reads a length from the
// input buffer and then that number of bytes into a []byte from the
// input buffer. The difference is that the integer indicating the length
// of the byte array to follow is a zigzag encoded varint.
//
func ReadBytes(buf io.Reader) ([]byte, error) {
	// an encoded varint give the length of the remaining byte array
	// TODO: might be better to have a ReadVarIntAndDecode that chooses whether to do
	//       int32 or int64 based on the size of the varint and then returns interface{} ?
	lenbytes, err := ReadVarIntAndDecode64(buf)
	if err != nil {
		return nil, err
	}

	if lenbytes == 0 {
		return nil, nil
	}

	if lenbytes < 0 {
		return nil, fmt.Errorf("Error in varint.ReadBytes: size of bytes was less than zero: %v", lenbytes)
	}

	size := int(lenbytes)
	data := make([]byte, size)
	n, err := buf.Read(data)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if n != size {
		return nil, oerror.IncorrectNetworkRead{Expected: size, Actual: n}
	}
	return data, nil
}

//
// varint.ReadString, like rw.ReadString, first reads a length from the
// input buffer and then that number of bytes (of ASCII chars) into a string
// from the input buffer. The difference is that the integer indicating the
// length of the byte array to follow is a zigzag encoded varint.
//
func ReadString(buf io.Reader) (string, error) {
	bs, err := ReadBytes(buf)
	if err != nil {
		return "", oerror.NewTrace(err)
	}
	return string(bs), nil
}
