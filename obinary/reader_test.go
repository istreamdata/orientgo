package obinary

import (
	"bytes"
	"encoding/binary"

	"testing"
)

const (
	MaxUint = ^uint32(0)
	MinUint = 0
	MaxInt  = int32(MaxUint >> 1)
	MinInt  = -MaxInt - 1
)

var err error

func TestReadByte(t *testing.T) {
	var val byte
	data := []byte{1, 2, 3}
	rdr := bytes.NewBuffer(data)

	val, err = ReadByte(rdr)
	ok(t, err)
	equals(t, byte(1), val)

	val, err = ReadByte(rdr)
	ok(t, err)
	equals(t, byte(2), val)

	val, err = ReadByte(rdr)
	ok(t, err)
	equals(t, byte(3), val)

	val, err = ReadByte(rdr)
	assert(t, err != nil, "error should not be nil")
	equals(t, byte(DEFAULT_RETVAL), val)

	val, err = ReadByte(new(bytes.Buffer))
	assert(t, err != nil, "error should not be nil")
	equals(t, byte(DEFAULT_RETVAL), val)
}

func TestReadBytes(t *testing.T) {
	var bs []byte

	// data[0:4] gets interpreted as a big-endian int (=4) which specifies the number of bytes to be read
	// bytes data are then data[1:5], since int32(data[0:4])==4)
	data := []byte{0, 0, 0, 4, 1, 2, 3, 4}
	rdr := bytes.NewBuffer(data)

	bs, err = ReadBytes(rdr)
	ok(t, err)
	equals(t, 4, len(bs))
	equals(t, byte(1), bs[0])
	equals(t, byte(2), bs[1])
	equals(t, byte(3), bs[2])
	equals(t, byte(4), bs[3])

	// ensure more than 4 entries are not read
	data = []byte{0, 0, 0, 4, 1, 2, 3, 4, 5, 6}
	rdr = bytes.NewBuffer(data)

	bs, err = ReadBytes(rdr)
	ok(t, err)
	equals(t, 4, len(bs))
	equals(t, byte(1), bs[0])
	equals(t, byte(2), bs[1])
	equals(t, byte(3), bs[2])
	equals(t, byte(4), bs[3])
}

func TestReadBytesWithTooFewEntries(t *testing.T) {
	var bs []byte

	// data[0:4] gets interpreted as a big-endian int (=4) which specifies the number of bytes to be read
	// bytes data are then data[1:5], since int32(data[0:4])==4)
	data := []byte{0, 0, 0, 12, 1, 2, 3, 4, 5}
	rdr := bytes.NewBuffer(data)

	bs, err = ReadBytes(rdr)
	assert(t, err != nil, "err should not be nil")
	equals(t, IncorrectNetworkRead{expected: 12, actual: 5}, err)
	assert(t, bs == nil, "bs should be nil")
}

func TestReadBytesWithNullBytesArray(t *testing.T) {
	var bs []byte

	// data[0:4] gets interpreted as a big-endian int (=0) which specifies an "empty"
	// byte array has been encoded
	data := []byte{0, 0, 0, 0, 1, 2, 3, 4, 5}
	rdr := bytes.NewBuffer(data)
	bs, err = ReadBytes(rdr)
	ok(t, err)
	assert(t, bs == nil, "bs should be nil")
}

func TestReadInt(t *testing.T) {
	var outval int
	data := []int32{0, 1, 100000, 200000, MaxInt, MinInt}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int32 into bytes
		err = binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadInt (fn under test)
		outval, err = ReadInt(buf)
		ok(t, err)
		equals(t, int(inval), outval)
	}
}

func TestReadIntWithBadInputs(t *testing.T) {
	// no input
	var outval int
	buf := new(bytes.Buffer)
	outval, err = ReadInt(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, int(DEFAULT_RETVAL), outval)

	// not enough input (int needs 4 bytes)
	data := []byte{0, 1, 2}
	buf = bytes.NewBuffer(data)
	outval, err = ReadInt(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, IncorrectNetworkRead{expected: 4, actual: 3}, err)
	equals(t, int(DEFAULT_RETVAL), outval)
}

func TestReadString(t *testing.T) {
	s := "one two 345"
	buf := new(bytes.Buffer)
	data := []byte{0, 0, 0, byte(len(s))} // integer sz of string
	buf.Write(data)
	buf.WriteString(s)

	outstr, err := ReadString(buf)
	ok(t, err)
	equals(t, s, outstr)
}

func TestReadStringWithNullString(t *testing.T) {
	// first with only integer in the Reader
	data := []byte{0, 0, 0, 0}
	buf := bytes.NewBuffer(data)
	outstr, err := ReadString(buf)
	ok(t, err)
	equals(t, "", outstr)

	// next with string in the buffer - still shouldn't be read
	s := "one two 345"
	buf.Reset()
	buf.Write(data)
	buf.WriteString(s)

	outstr, err = ReadString(buf)
	ok(t, err)
	equals(t, "", outstr)
}

func TestReadStringWithSizeLargerThanString(t *testing.T) {
	s := "one"
	buf := new(bytes.Buffer)
	data := []byte{0, 0, 0, byte(200)} // integer sz of string too large
	buf.Write(data)
	buf.WriteString(s)

	outstr, err := ReadString(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, IncorrectNetworkRead{expected: 200, actual: 3}, err)
	equals(t, "", outstr)
}
