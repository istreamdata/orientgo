package obinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const DEFAULT_RETVAL = 255

func ReadByte(rdr io.Reader) (byte, error) {
	readbuf := make([]byte, 1)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, err
	}
	if n != 1 {
		return DEFAULT_RETVAL, IncorrectNetworkRead{expected: 1, actual: n}
	}
	return readbuf[0], nil
}

//
// ReadString xxxx
// If the string size is 0 an empty string and nil error are returned
//
func ReadString(rdr io.Reader) (string, error) {
	bs, err := ReadBytes(rdr)
	if err != nil || bs == nil {
		return "", err
	}
	return string(bs), nil
}

func ReadBytes(rdr io.Reader) ([]byte, error) {
	// the first four bytes give the length of the remaining byte array
	sz, err := ReadInt(rdr)
	if err != nil {
		return nil, err
	}
	// sz of 0 indicates empty byte array
	// sz of -1 indicates null value
	// for now, I'm returning nil []byte for both
	if sz <= 0 {
		return nil, nil
	}

	fmt.Printf("++ ReadBytes sz: %v\n", sz) // DEBUG
	readbuf := make([]byte, sz)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return nil, err
	}
	if n != sz {
		return nil, IncorrectNetworkRead{expected: sz, actual: n}
	}
	return readbuf, nil
}

func ReadInt(rdr io.Reader) (int, error) {
	intSz := 4
	readbuf := make([]byte, intSz)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, err
	}
	if n != intSz {
		return DEFAULT_RETVAL, IncorrectNetworkRead{expected: intSz, actual: n}
	}

	var intval int32
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &intval)
	if err != nil {
		return DEFAULT_RETVAL, err
	}

	return int(intval), nil
	// return ToIntBigEndian(readbuf), nil
}

func ReadShort(rdr io.Reader) (int16, error) {
	shortSz := 2
	readbuf := make([]byte, shortSz)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, err
	}
	if n != shortSz {
		return DEFAULT_RETVAL, IncorrectNetworkRead{expected: shortSz, actual: n}
	}

	var shortval int16
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &shortval)
	if err != nil {
		return int16(DEFAULT_RETVAL), err
	}

	return shortval, nil
}
