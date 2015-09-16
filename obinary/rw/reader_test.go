package rw

import (
	"bytes"
	"encoding/binary"

	"gopkg.in/istreamdata/orientgo.v1/oerror"

	"testing"
	"io"
)

const (
	MaxUint16 = ^uint16(0)
	MinUint16 = 0
	MaxInt16  = int16(MaxUint16 >> 1)
	MinInt16  = -MaxInt16 - 1

	MaxUint = ^uint32(0)
	MinUint = 0
	MaxInt  = int32(MaxUint >> 1)
	MinInt  = -MaxInt - 1

	MaxUint64 = ^uint64(0)
	MinUint64 = 0
	MaxInt64  = int64(MaxUint64 >> 1)
	MinInt64  = -MaxInt64 - 1
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

func TestReadShort(t *testing.T) {
	var outval int16
	data := []int16{0, 1, -112, int16(MaxInt16) - 23, MaxInt16, MinInt16}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int16 into bytes
		err = binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadLong (fn under test)
		outval, err = ReadShort(buf)
		ok(t, err)
		equals(t, int16(inval), outval)
	}
}

func TestReadShortWithBadInputs(t *testing.T) {
	var outval int16

	// no input
	buf := new(bytes.Buffer)
	outval, err = ReadShort(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, int16(DEFAULT_RETVAL), outval)

	// not enough input (int16 needs 2 bytes)
	data := []byte{33}
	buf = bytes.NewBuffer(data)
	outval, err = ReadShort(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, oerror.IncorrectNetworkRead{Expected: 2, Actual: 1}, err)
	equals(t, int16(DEFAULT_RETVAL), outval)
}

func TestReadLong(t *testing.T) {
	var outval int64
	data := []int64{0, 1, -100000, int64(MaxInt) + 99999, MaxInt64, MinInt64}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int64 into bytes
		err = binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadLong (fn under test)
		outval, err = ReadLong(buf)
		ok(t, err)
		equals(t, int64(inval), outval)
	}
}

func TestReadLongWithBadInputs(t *testing.T) {
	// no input
	var outval int64
	buf := new(bytes.Buffer)
	outval, err = ReadLong(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, int64(DEFAULT_RETVAL), outval)

	// not enough input (int64 needs 8 bytes)
	data := []byte{0, 1, 2, 3}
	buf = bytes.NewBuffer(data)
	outval, err = ReadLong(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, oerror.IncorrectNetworkRead{Expected: 8, Actual: 4}, err)
	equals(t, int64(DEFAULT_RETVAL), outval)
}

func TestReadInt(t *testing.T) {
	var outval int32
	data := []int32{0, 1, -100000, 200000, MaxInt, MinInt}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int32 into bytes
		err = binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadInt (fn under test)
		outval, err = ReadInt(buf)
		ok(t, err)
		equals(t, inval, outval)
	}
}

func TestReadFloat(t *testing.T) {
	var outval float32
	data := []float32{0, -0.00003, 893421.883472, -88842.255}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()

		// turn float32 into bytes
		err = binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// bytes -> float32
		outval, err = ReadFloat(buf)
		ok(t, err)
		equals(t, inval, outval)
	}
}

func TestReadDouble(t *testing.T) {
	var outval float64
	data := []float64{0, -0.0000000000000003, 9000000088880000000893421.8838800472, -388842.255}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()

		// turn float32 into bytes
		err = binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// bytes -> float64
		outval, err = ReadDouble(buf)
		ok(t, err)
		equals(t, inval, outval)
	}
}

func TestReadIntWithBadInputs(t *testing.T) {
	// no input
	var outval int32
	buf := new(bytes.Buffer)
	outval, err = ReadInt(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, int32(DEFAULT_RETVAL), outval)

	// not enough input (int needs 4 bytes)
	data := []byte{0, 1, 2}
	buf = bytes.NewBuffer(data)
	outval, err = ReadInt(buf)
	assert(t, err != nil, "err should not be nil")
	equals(t, oerror.IncorrectNetworkRead{Expected: 4, Actual: 3}, err)
	equals(t, int32(DEFAULT_RETVAL), outval)
}

func TestReadBoolFalse(t *testing.T) {
	exp := false
	buf := new(bytes.Buffer)
	data := []byte{0} // 0=false in OrientDB
	buf.Write(data)

	actual, err := ReadBool(buf)
	ok(t, err)
	equals(t, exp, actual)
}

func TestReadBoolTrue(t *testing.T) {
	exp := true
	buf := new(bytes.Buffer)
	data := []byte{1} // 1=true in OrientDB
	buf.Write(data)

	actual, err := ReadBool(buf)
	ok(t, err)
	equals(t, exp, actual)
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
	equals(t, io.ErrUnexpectedEOF, oerror.ExtractCause(err))
	equals(t, "", outstr)
}

func TestReadErrorResponseWithSingleException(t *testing.T) {
	buf := new(bytes.Buffer)
	err = WriteByte(buf, byte(1)) // indicates continue of exception class/msg array
	ok(t, err)

	err := WriteStrings(buf, "org.foo.BlargException", "wibble wibble!!")
	ok(t, err)

	err = WriteByte(buf, byte(0)) // indicates end of exception class/msg array
	ok(t, err)

	err = WriteBytes(buf, []byte("this is a stacktrace simulator\nEOL"))
	ok(t, err)

	var serverExc oerror.OServerException
	serverExc, err = ReadErrorResponse(buf)
	ok(t, err)
	equals(t, 1, len(serverExc.Classes))

	equals(t, "org.foo.BlargException", serverExc.Classes[0])
	equals(t, "wibble wibble!!", serverExc.Messages[0])
}

func TestReadErrorResponseWithMultipleExceptions(t *testing.T) {
	buf := new(bytes.Buffer)
	err = WriteByte(buf, byte(1)) // indicates more exceptions to come
	ok(t, err)

	err := WriteStrings(buf, "org.foo.BlargException", "Too many blorgles!!")
	ok(t, err)

	err = WriteByte(buf, byte(1)) // indicates more exceptions to come
	ok(t, err)

	err = WriteStrings(buf, "org.foo.FeebleException", "Not enough juice")
	ok(t, err)

	err = WriteByte(buf, byte(1)) // indicates more exceptions to come
	ok(t, err)

	err = WriteStrings(buf, "org.foo.WobbleException", "Orbital decay")
	ok(t, err)

	err = WriteByte(buf, byte(0)) // indicates end of exceptions
	ok(t, err)

	err = WriteBytes(buf, []byte("this is a stacktrace simulator\nEOL"))
	ok(t, err)

	serverExc, err := ReadErrorResponse(buf)
	ok(t, err)

	equals(t, "org.foo.BlargException", serverExc.Classes[0])
	equals(t, "Not enough juice", serverExc.Messages[1])
	equals(t, "org.foo.WobbleException", serverExc.Classes[2])
	equals(t, "Orbital decay", serverExc.Messages[2])
}
