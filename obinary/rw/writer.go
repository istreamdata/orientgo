package rw

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

func WriteNull(w io.Writer) error {
	return WriteInt(w, -1)
}

func WriteByte(w io.Writer, b byte) error {
	var singleByteArray [1]byte
	singleByteArray[0] = b
	_, err := w.Write(singleByteArray[0:1])
	return err
}

//
// WriteShort writes a int16 in big endian order to the wfer
//
func WriteShort(w io.Writer, n int16) error {
	return binary.Write(w, binary.BigEndian, n)
}

//
// WriteInt writes a int32 in big endian order to the wfer
//
func WriteInt(w io.Writer, n int32) error {
	return binary.Write(w, binary.BigEndian, n)
}

//
// WriteLong writes a int64 in big endian order to the wfer
//
func WriteLong(w io.Writer, n int64) error {
	return binary.Write(w, binary.BigEndian, n)
}

func WriteStrings(w io.Writer, ss ...string) error {
	for _, s := range ss {
		err := WriteString(w, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteString(w io.Writer, s string) error {
	// len(string) returns the number of bytes, not runes, so it is correct here
	err := WriteInt(w, int32(len(s)))
	if err != nil {
		return err
	}

	n, err := w.Write([]byte(s))
	// n, err := w.WriteString(s)
	if n != len(s) {
		return errors.New("ERROR: Incorrect number of bytes written: " + strconv.Itoa(n))
	}
	return err
}

//
// WriteRawBytes just writes the bytes, not prefixed by the size of the []byte
//
func WriteRawBytes(w io.Writer, bs []byte) error {
	n, err := w.Write(bs)
	if n != len(bs) {
		return fmt.Errorf("ERROR: Incorrect number of bytes written: %d", n)
	}
	return err
}

//
// WriteBytes is meant to be used for writing a structure that the OrientDB will
// interpret as a byte array, usually a serialized datastructure.  This means the
// first thing written to the wfer is the size of the byte array.  If you want
// to write bytes without the the size prefix, use WriteRawBytes instead.
//
func WriteBytes(w io.Writer, bs []byte) error {
	err := WriteInt(w, int32(len(bs)))
	if err != nil {
		return err
	}
	n, err := w.Write(bs)
	if n != len(bs) {
		return errors.New("ERROR: Incorrect number of bytes written: " + strconv.Itoa(n))
	}
	return err
}

//
// WriteBool writes byte(1) for true and byte(0) for false to the wfer,
// as specified by the OrientDB spec.
//
func WriteBool(w io.Writer, b bool) error {
	if b {
		return WriteByte(w, byte(1))
	}
	return WriteByte(w, byte(0))
}

//
// WriteFloat writes a float32 in big endian order to the wfer
//
func WriteFloat(w io.Writer, f float32) error {
	return binary.Write(w, binary.BigEndian, f)
}

//
// WriteDouble writes a float64 in big endian order to the wfer
//
func WriteDouble(w io.Writer, f float64) error {
	return binary.Write(w, binary.BigEndian, f)
}
