package rw

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

func WriteNull(buf *bytes.Buffer) error {
	return WriteInt(buf, -1)
}

func WriteByte(buf *bytes.Buffer, b byte) error {
	return buf.WriteByte(b)
}

func WriteShort(buf *bytes.Buffer, n int16) error {
	return binary.Write(buf, binary.BigEndian, n)
}

// let's assume big endian for now
// Note: this method assumes the int can be safely cast to int32
func WriteInt(buf *bytes.Buffer, n int32) error {
	return binary.Write(buf, binary.BigEndian, n)
}

func WriteLong(buf *bytes.Buffer, n int64) error {
	return binary.Write(buf, binary.BigEndian, n)
}

func WriteStrings(buf *bytes.Buffer, ss ...string) error {
	for _, s := range ss {
		err := WriteString(buf, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteString(buf *bytes.Buffer, s string) error {
	// len(string) returns the number of bytes, not runes, so it is correct here
	err := WriteInt(buf, int32(len(s)))
	if err != nil {
		return err
	}
	n, err := buf.WriteString(s)
	if n != len(s) {
		return errors.New("ERROR: Incorrect number of bytes written: " + strconv.Itoa(n))
	}
	return err
}

//
// WriteRawBytes just writes the bytes, not prefixed by the size of the []byte
//
func WriteRawBytes(buf *bytes.Buffer, bs []byte) error {
	n, err := buf.Write(bs)
	if n != len(bs) {
		return fmt.Errorf("ERROR: Incorrect number of bytes written: %d", n)
	}
	return err
}

//
// WriteBytes is meant to be used for writing a structure that the OrientDB will
// interpret as a byte array, usually a serialized datastructure.  This means the
// first thing written to the buffer is the size of the byte array.  If you want
// to write bytes without the the size prefix, use WriteRawBytes instead.
//
func WriteBytes(buf *bytes.Buffer, bs []byte) error {
	err := WriteInt(buf, int32(len(bs)))
	if err != nil {
		return err
	}
	n, err := buf.Write(bs)
	if n != len(bs) {
		return errors.New("ERROR: Incorrect number of bytes written: " + strconv.Itoa(n))
	}
	return err
}

func WriteBool(buf *bytes.Buffer, b bool) error {
	if b {
		return WriteByte(buf, byte(1))
	}
	return WriteByte(buf, byte(0))
}
