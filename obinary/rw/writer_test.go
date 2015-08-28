package rw

import (
	"bytes"
	"encoding/binary"

	"testing"
)

func TestWriteBytes(t *testing.T) {
	buf := new(bytes.Buffer)
	byteMsg := []byte("I like Ike")
	WriteBytes(buf, byteMsg)

	equals(t, 4+len(byteMsg), buf.Len())
	bs := buf.Next(4)
	equals(t, len(byteMsg), bigEndianConvertToInt(bs))

	bs = buf.Next(len(byteMsg))
	equals(t, byteMsg, bs)
}

func TestWriteRawBytes(t *testing.T) {
	buf := new(bytes.Buffer)
	byteMsg := []byte("I like Ike")
	WriteRawBytes(buf, byteMsg)

	bs := buf.Next(len(byteMsg))
	equals(t, byteMsg, bs)

	// write empty bytes
	buf = new(bytes.Buffer)
	byteMsg = []byte{}
	WriteRawBytes(buf, byteMsg)

	equals(t, 0, buf.Len())
}

func TestWriteNull(t *testing.T) {
	buf := new(bytes.Buffer)
	WriteNull(buf)

	equals(t, 4, buf.Len()) // null in OrientDB is -1 (int32)

	var actInt int32
	binary.Read(buf, binary.BigEndian, &actInt)
	equals(t, int32(-1), actInt)
}

func TestWriteBool(t *testing.T) {
	buf := new(bytes.Buffer)
	WriteBool(buf, true)
	WriteBool(buf, false)
	WriteBool(buf, true)

	equals(t, 3, buf.Len())
	bs := buf.Bytes()
	equals(t, byte(1), bs[0])
	equals(t, byte(0), bs[1])
	equals(t, byte(1), bs[2])
}

func TestWriteFloat(t *testing.T) {
	f := float32(55.668209)
	buf := new(bytes.Buffer)
	WriteFloat(buf, f)

	equals(t, 4, buf.Len())

	f2 := ReadFloat(buf)
	equals(t, f, f2)
}

func TestWriteDouble(t *testing.T) {
	f := float64(199999999999999999955.6682090323333337298)
	buf := new(bytes.Buffer)
	WriteDouble(buf, f)

	equals(t, 8, buf.Len())

	f2 := ReadDouble(buf)
	equals(t, f, f2)
}

func TestWriteString(t *testing.T) {
	var buf bytes.Buffer
	WriteString(&buf, "hello")
	equals(t, 9, buf.Len())

	n, s := nextBinaryString(&buf)
	equals(t, 5, n)
	equals(t, "hello", s)
}

func TestWriteStrings(t *testing.T) {
	buf := new(bytes.Buffer)
	WriteStrings(buf, "a", "a longer string", "golang")
	equals(t, (4*3)+len("a")+len("a longer string")+len("golang"), buf.Len())

	// read back first string
	n, s := nextBinaryString(buf)
	equals(t, 1, n)
	equals(t, "a", s)

	// read back second string
	n, s = nextBinaryString(buf)
	equals(t, len("a longer string"), n)
	equals(t, "a longer string", s)

	// read back third string
	n, s = nextBinaryString(buf)
	equals(t, len("golang"), n)
	equals(t, "golang", s)
}

func TestWriteManyTypes(t *testing.T) {
	var (
		buf bytes.Buffer
		bs  []byte
	)
	WriteByte(&buf, 0x1)
	WriteString(&buf, "vått og tørt")
	WriteShort(&buf, int16(29876))
	WriteShort(&buf, int16(444))
	WriteInt(&buf, 9999999)
	WriteLong(&buf, MaxInt64)

	// read back
	bs = buf.Next(1) // byte
	equals(t, byte(0x1), bs[0])

	bs = buf.Next(4) // str length
	equals(t, 14, bigEndianConvertToInt(bs))

	bs = buf.Next(14) // str contents
	equals(t, "vått og tørt", string(bs))

	var act int16
	binary.Read(&buf, binary.BigEndian, &act) // use the binary.Read to convert rather than manual
	equals(t, int16(29876), act)

	binary.Read(&buf, binary.BigEndian, &act) // use the binary.Read to convert rather than manual
	equals(t, int16(444), act)

	var actInt int32
	binary.Read(&buf, binary.BigEndian, &actInt)
	equals(t, int32(9999999), actInt)

	var actLong int64
	binary.Read(&buf, binary.BigEndian, &actLong)
	equals(t, MaxInt64, actLong)

}

/* ---[ helper fns ]--- */

func nextBinaryString(buf *bytes.Buffer) (int, string) {
	intBytes := buf.Next(4)
	intVal := int(intBytes[3]) | int(intBytes[2])<<8 | int(intBytes[1])<<16 | int(intBytes[0])<<24

	strBytes := buf.Next(intVal)
	return intVal, string(strBytes)
}

func bigEndianConvertToInt(bs []byte) int {
	return int(binary.BigEndian.Uint32(bs))
}
