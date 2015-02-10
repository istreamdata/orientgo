package varint

import (
	"bytes"
	"fmt"
	"ogonori/obinary/rw"
)

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
		return nil, rw.IncorrectNetworkRead{Expected: size, Actual: len(data)}
	}
	return data, nil
}

func ReadString(buf *bytes.Buffer) (string, error) {
	bs, err := ReadBytes(buf)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
