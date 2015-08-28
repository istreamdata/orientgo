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
	"encoding/binary"
	"io"

	"github.com/dyy18/orientgo/oerror"
)

var endianness = binary.BigEndian

func read(r io.Reader, v interface{}) {
	if err := binary.Read(r, endianness, v); err != nil {
		panic(err)
	}
}

func ReadByte(r io.Reader) byte {
	readbuf := make([]byte, 1)
	n, err := r.Read(readbuf)
	if err != nil {
		panic(err)
	} else if n != 1 {
		panic(io.ErrUnexpectedEOF)
	}
	return readbuf[0]
}

// ReadString xxxx
// If the string size is 0 an empty string and nil error are returned
func ReadString(r io.Reader) string {
	bs := ReadBytes(r)
	if bs == nil {
		return ""
	}
	return string(bs)
}

func ReadRawBytes(r io.Reader, buf []byte) {
	if _, err := io.ReadFull(r, buf); err != nil {
		panic(err)
	}
}

// ReadBytes reads in an OrientDB byte array.  It reads the first 4 bytes
// from the Reader as an int to determine the length of the byte array
// to read in.
// If the specified size of the byte array is 0 (empty) or negative (null)
// nil is returned for the []byte.
func ReadBytes(r io.Reader) []byte {
	// the first four bytes give the length of the remaining byte array
	sz := ReadInt(r)
	// sz of 0 indicates empty byte array
	// sz of -1 indicates null value
	// for now, I'm returning nil []byte for both
	if sz <= 0 {
		return nil
	}

	readbuf := make([]byte, sz)
	ReadRawBytes(r, readbuf)
	return readbuf
}

func ReadInt(r io.Reader) (v int32) {
	read(r, &v)
	return
}

func ReadLong(r io.Reader) (v int64) {
	read(r, &v)
	return
}

func ReadShort(r io.Reader) (v int16) {
	read(r, &v)
	return
}

func ReadFloat(r io.Reader) (v float32) {
	read(r, &v)
	return
}

func ReadDouble(r io.Reader) (v float64) {
	read(r, &v)
	return
}

// Reads one byte from the Reader. If the byte is zero, then false is returned,
// otherwise true.  If error is non-nil, then the bool value is undefined.
func ReadBool(r io.Reader) bool {
	b := ReadByte(r)
	// non-zero is true
	return b != byte(0)
}

// ReadErrorResponse reads an "Exception" message from the OrientDB server.
// The OrientDB server can return multiple exceptions, all of which are
// incorporated into a single OServerException Error struct.
// If error (the second return arg) is not nil, then there was a
// problem reading the server exception on the wire.
func ReadErrorResponse(r io.Reader) (serverException error) {
	var (
		exClass, exMsg string
	)
	exc := make([]oerror.Exception, 0, 1) // usually only one ?
	for {
		// before class/message combo there is a 1 (continue) or 0 (no more)
		marker := ReadByte(r)
		if marker == byte(0) {
			break
		}
		exClass = ReadString(r)
		exMsg = ReadString(r)
		exc = append(exc, oerror.UnknownException{Class: exClass, Message: exMsg})
	}

	// Next there *may* a serialized exception of bytes, but it is only
	// useful to Java clients, so read and ignore if present.
	// If there is no serialized exception, EOF will be returned
	_ = ReadBytes(r) // TODO: catch EOFs?

	for _, e := range exc {
		switch e.ExcClass() {
		case "com.orientechnologies.orient.core.storage.ORecordDuplicatedException":
			return oerror.ODuplicatedRecordException{oerror.OServerException{Exceptions: exc}}
		}
	}
	return oerror.OServerException{Exceptions: exc}
}
