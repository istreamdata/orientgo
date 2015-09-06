package rw

import (
	"bytes"
	"encoding/binary"
	"testing"
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

func TestReadBytes(t *testing.T) {
	var bs []byte

	// data[0:4] gets interpreted as a big-endian int (=4) which specifies the number of bytes to be read
	// bytes data are then data[1:5], since int32(data[0:4])==4)
	data := []byte{0, 0, 0, 4, 1, 2, 3, 4}
	rdr := bytes.NewBuffer(data)

	bs = ReadBytes(rdr)
	equals(t, 4, len(bs))
	equals(t, byte(1), bs[0])
	equals(t, byte(2), bs[1])
	equals(t, byte(3), bs[2])
	equals(t, byte(4), bs[3])

	// ensure more than 4 entries are not read
	data = []byte{0, 0, 0, 4, 1, 2, 3, 4, 5, 6}
	rdr = bytes.NewBuffer(data)

	bs = ReadBytes(rdr)
	equals(t, 4, len(bs))
	equals(t, byte(1), bs[0])
	equals(t, byte(2), bs[1])
	equals(t, byte(3), bs[2])
	equals(t, byte(4), bs[3])
}

func TestReadBytesWithNullBytesArray(t *testing.T) {
	var bs []byte

	// data[0:4] gets interpreted as a big-endian int (=0) which specifies an "empty"
	// byte array has been encoded
	data := []byte{0, 0, 0, 0, 1, 2, 3, 4, 5}
	rdr := bytes.NewBuffer(data)
	bs = ReadBytes(rdr)
	assert(t, bs == nil, "bs should be nil")
}

func TestReadShort(t *testing.T) {
	var outval int16
	data := []int16{0, 1, -112, int16(MaxInt16) - 23, MaxInt16, MinInt16}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int16 into bytes
		err := binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadLong (fn under test)
		outval = ReadShort(buf)
		equals(t, int16(inval), outval)
	}
}

func TestReadLong(t *testing.T) {
	var outval int64
	data := []int64{0, 1, -100000, int64(MaxInt) + 99999, MaxInt64, MinInt64}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int64 into bytes
		err := binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadLong (fn under test)
		outval = ReadLong(buf)
		equals(t, int64(inval), outval)
	}
}

func TestReadInt(t *testing.T) {
	var outval int32
	data := []int32{0, 1, -100000, 200000, MaxInt, MinInt}

	buf := new(bytes.Buffer)
	for _, inval := range data {
		buf.Reset()
		// turn int32 into bytes
		err := binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// turn bytes back into int using obinary.ReadInt (fn under test)
		outval = ReadInt(buf)
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
		err := binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// bytes -> float32
		outval = ReadFloat(buf)
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
		err := binary.Write(buf, binary.BigEndian, inval)
		ok(t, err)

		// bytes -> float64
		outval = ReadDouble(buf)
		equals(t, inval, outval)
	}
}

func TestReadBoolFalse(t *testing.T) {
	exp := false
	buf := new(bytes.Buffer)
	data := []byte{0} // 0=false in OrientDB
	buf.Write(data)

	actual := ReadBool(buf)
	equals(t, exp, actual)
}

func TestReadBoolTrue(t *testing.T) {
	exp := true
	buf := new(bytes.Buffer)
	data := []byte{1} // 1=true in OrientDB
	buf.Write(data)

	actual := ReadBool(buf)
	equals(t, exp, actual)
}

func TestReadString(t *testing.T) {
	s := "one two 345"
	buf := new(bytes.Buffer)
	data := []byte{0, 0, 0, byte(len(s))} // integer sz of string
	buf.Write(data)
	buf.WriteString(s)

	outstr := ReadString(buf)
	equals(t, s, outstr)
}

func TestReadStringWithNullString(t *testing.T) {
	// first with only integer in the Reader
	data := []byte{0, 0, 0, 0}
	buf := bytes.NewBuffer(data)
	outstr := ReadString(buf)
	equals(t, "", outstr)

	// next with string in the buffer - still shouldn't be read
	s := "one two 345"
	buf.Reset()
	buf.Write(data)
	buf.WriteString(s)

	outstr = ReadString(buf)
	equals(t, "", outstr)
}
