package rw

import (
	"bytes"
	"encoding/binary"

	"testing"
)

func TestWriteBytes(t *testing.T) {
	buf := new(bytes.Buffer)
	byteMsg := []byte("I like Ike")
	NewWriter(buf).WriteBytes(byteMsg)

	equals(t, 4+len(byteMsg), buf.Len())
	bs := buf.Next(4)
	equals(t, len(byteMsg), bigEndianConvertToInt(bs))

	bs = buf.Next(len(byteMsg))
	equals(t, byteMsg, bs)
}

func TestWriteRawBytes(t *testing.T) {
	buf := new(bytes.Buffer)
	byteMsg := []byte("I like Ike")
	NewWriter(buf).WriteRawBytes(byteMsg)

	bs := buf.Next(len(byteMsg))
	equals(t, byteMsg, bs)

	// write empty bytes
	buf = new(bytes.Buffer)
	byteMsg = []byte{}
	NewWriter(buf).WriteRawBytes(byteMsg)

	equals(t, 0, buf.Len())
}

func TestWriteNull(t *testing.T) {
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteNull()

	equals(t, 4, buf.Len()) // null in OrientDB is -1 (int32)

	var actInt int32
	binary.Read(buf, binary.BigEndian, &actInt)
	equals(t, int32(-1), actInt)
}

func TestWriteBool(t *testing.T) {
	buf := new(bytes.Buffer)
	bw := NewWriter(buf)
	bw.WriteBool(true)
	bw.WriteBool(false)
	bw.WriteBool(true)

	equals(t, 3, buf.Len())
	bs := buf.Bytes()
	equals(t, byte(1), bs[0])
	equals(t, byte(0), bs[1])
	equals(t, byte(1), bs[2])
}

func TestWriteFloat(t *testing.T) {
	f := float32(55.668209)
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteFloat(f)

	equals(t, 4, buf.Len())

	f2 := NewReader(buf).ReadFloat()
	equals(t, f, f2)
}

func TestWriteDouble(t *testing.T) {
	f := float64(199999999999999999955.6682090323333337298)
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteDouble(f)

	equals(t, 8, buf.Len())

	f2 := NewReader(buf).ReadDouble()
	equals(t, f, f2)
}

func TestWriteString(t *testing.T) {
	var buf bytes.Buffer
	NewWriter(&buf).WriteString("hello")
	equals(t, 9, buf.Len())

	n, s := nextBinaryString(&buf)
	equals(t, 5, n)
	equals(t, "hello", s)
}

func TestWriteStrings(t *testing.T) {
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteStrings("a", "a longer string", "golang")
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
		bw  = NewWriter(&buf)
	)
	bw.WriteByte(0x1)
	bw.WriteString("vått og tørt")
	bw.WriteShort(int16(29876))
	bw.WriteShort(int16(444))
	bw.WriteInt(9999999)
	bw.WriteLong(MaxInt64)
	equals(t, nil, bw.Err())

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

func TestWriteBytesVarint_GoodData_5Bytes(t *testing.T) {
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteBytesVarint([]byte("total"))

	equals(t, 6, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(10), n) // zigzag encoded value of 5 is 10
	ok(t, err)

	strbytes := buf.Next(5)
	equals(t, "total", string(strbytes))

	// // varint.ReadBytes expects a varint encoded int, followed by that many bytes
	// outbytes, err := ReadBytes(buf)
	// ok(t, err)
	// equals(t, 5, len(outbytes))
	// equals(t, "total", string(outbytes))
}

func TestWriteStringVarint_GoodData_5Bytes(t *testing.T) {
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteStringVarint("total")

	equals(t, 6, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(10), n) // zigzag encoded value of 5 is 10
	ok(t, err)

	strbytes := buf.Next(5)
	equals(t, "total", string(strbytes))
}

func TestWriteStringVarint_GoodData_EmptyString(t *testing.T) {
	buf := new(bytes.Buffer)
	NewWriter(buf).WriteStringVarint("")

	equals(t, 1, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(0), n)
	ok(t, err)
}
