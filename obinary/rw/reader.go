//
// rw is the read-write package for reading and writing types
// from the OrientDB binary network protocol.  Reading is done
// via io.Reader and writing is done to bytes.Buffer (since the
// extra functionality of byte.Buffer is desired).  All the
// OrientDB types are represented here for non-encoded forms.
// For varint and zigzag encoding/decoding handling use the
// obinary/varint package instead.
//
package rw

import (
	"bytes"
	"encoding/binary"
	"io"

	"gopkg.in/istreamdata/orientgo.v1/oerror"
)

const DEFAULT_RETVAL = 255

/* ---[ types ]--- */

/* -------------------------------- */
/* ---[ Lower Level Functions ]--- */
/* -------------------------------- */

func ReadByte(rdr io.Reader) (byte, error) {
	readbuf := make([]byte, 1)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, oerror.NewTrace(err)
	}
	if n != 1 {
		return DEFAULT_RETVAL, oerror.IncorrectNetworkRead{Expected: 1, Actual: n}
	}
	return readbuf[0], nil
}

//
// ReadString xxxx
// If the string size is 0 an empty string and nil error are returned
//
func ReadString(rdr io.Reader) (string, error) {
	bs, err := ReadBytes(rdr)
	if err != nil {
		return "", oerror.NewTrace(err)
	}
	if bs == nil {
		return "", nil
	}
	return string(bs), nil
}

//
// ReadBytes reads in an OrientDB byte array.  It reads the first 4 bytes
// from the Reader as an int to determine the length of the byte array
// to read in.
// If the specified size of the byte array is 0 (empty) or negative (null)
// nil is returned for the []byte.
//
func ReadBytes(rdr io.Reader) ([]byte, error) {
	// the first four bytes give the length of the remaining byte array
	sz, err := ReadInt(rdr)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	// sz of 0 indicates empty byte array
	// sz of -1 indicates null value
	// for now, I'm returning nil []byte for both
	if sz <= 0 {
		return nil, nil
	}

	readbuf := make([]byte, sz)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if n != int(sz) {
		return nil, oerror.IncorrectNetworkRead{Expected: int(sz), Actual: n}
	}
	return readbuf, nil
}

func ReadInt(rdr io.Reader) (int32, error) {
	intSz := 4
	readbuf := make([]byte, intSz)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, oerror.NewTrace(err)
	}
	if n != intSz {
		return DEFAULT_RETVAL, oerror.IncorrectNetworkRead{Expected: intSz, Actual: n}
	}

	var intval int32
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &intval)
	if err != nil {
		return DEFAULT_RETVAL, oerror.NewTrace(err)
	}

	return intval, nil
}

func ReadLong(rdr io.Reader) (int64, error) {
	longSz := 8
	readbuf := make([]byte, longSz)

	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, oerror.NewTrace(err)
	}
	if n != longSz {
		return DEFAULT_RETVAL, oerror.IncorrectNetworkRead{Expected: longSz, Actual: n}
	}

	var longval int64
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &longval)
	if err != nil {
		return DEFAULT_RETVAL, oerror.NewTrace(err)
	}

	return longval, nil
}

func ReadShort(rdr io.Reader) (int16, error) {
	shortSz := 2
	readbuf := make([]byte, shortSz)
	n, err := rdr.Read(readbuf)
	if err != nil {
		return DEFAULT_RETVAL, err
	}
	if n != shortSz {
		return DEFAULT_RETVAL, oerror.IncorrectNetworkRead{Expected: shortSz, Actual: n}
	}

	var shortval int16
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &shortval)
	if err != nil {
		return int16(DEFAULT_RETVAL), oerror.NewTrace(err)
	}

	return shortval, nil
}

func ReadFloat(rdr io.Reader) (float32, error) {
	floatSz := 4
	readbuf := make([]byte, floatSz)

	n, err := rdr.Read(readbuf)
	if err != nil {
		return 0.0, err
	}
	if n != floatSz {
		return 0.0, oerror.IncorrectNetworkRead{Expected: floatSz, Actual: n}
	}

	var floatval float32
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &floatval)
	if err != nil {
		return 0.0, err
	}

	return floatval, nil
}

func ReadDouble(rdr io.Reader) (float64, error) {
	doubleSz := 8
	readbuf := make([]byte, doubleSz)

	n, err := rdr.Read(readbuf)
	if err != nil {
		return 0.0, err
	}
	if n != doubleSz {
		return 0.0, oerror.IncorrectNetworkRead{Expected: doubleSz, Actual: n}
	}

	var doubleval float64
	buf := bytes.NewBuffer(readbuf)
	err = binary.Read(buf, binary.BigEndian, &doubleval)
	if err != nil {
		return 0.0, err
	}

	return doubleval, nil
}

//
// Reads one byte from the Reader. If the byte is zero, then false is returned,
// otherwise true.  If error is non-nil, then the bool value is undefined.
//
func ReadBool(rdr io.Reader) (bool, error) {
	b, err := ReadByte(rdr)
	if err != nil {
		return false, err
	}
	// non-zero is true
	return b != byte(0), nil
}

/* -------------------------------- */
/* ---[ Higher Level Functions ]--- */
/* -------------------------------- */

//
// ReadErrorResponse reads an "Exception" message from the OrientDB server.
// The OrientDB server can return multiple exceptions, all of which are
// incorporated into a single ogonori OServerException Error struct.
// If error (the second return arg) is not nil, then there was a
// problem reading the server exception on the wire.
//
func ReadErrorResponse(rdr io.Reader) (oerror.OServerException, error) {
	var (
		exClass, exMsg string
		err            error
	)
	classes := make([]string, 0, 1) // usually only one ?
	messages := make([]string, 0, 1)
	for {
		// before class/message combo there is a 1 (continue) or 0 (no more)
		marker, err := ReadByte(rdr)
		if err != nil {
			return oerror.OServerException{}, err
		}
		if marker == byte(0) {
			break
		}
		exClass, err = ReadString(rdr)
		if err != nil {
			return oerror.OServerException{}, err
		}
		classes = append(classes, exClass)
		exMsg, err = ReadString(rdr)
		if err != nil {
			return oerror.OServerException{}, err
		}
		messages = append(messages, exMsg)
	}

	// Next there *may* a serialized exception of bytes, but it is only
	// useful to Java clients, so read and ignore if present.
	// If there is no serialized exception, EOF will be returned
	_, err = ReadBytes(rdr)
	if err != nil && err != io.EOF {
		return oerror.OServerException{}, err
	}

	return oerror.OServerException{Classes: classes, Messages: messages}, nil
}
