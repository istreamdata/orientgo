package varint

import (
	"bytes"
	"testing"
)

func TestWriteBytes_GoodData_5Bytes(t *testing.T) {
	buf := new(bytes.Buffer)
	WriteBytes(buf, []byte("total"))

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

func TestWriteString_GoodData_5Bytes(t *testing.T) {
	buf := new(bytes.Buffer)
	WriteString(buf, "total")

	equals(t, 6, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(10), n) // zigzag encoded value of 5 is 10
	ok(t, err)

	strbytes := buf.Next(5)
	equals(t, "total", string(strbytes))
}

func TestWriteString_GoodData_EmptyString(t *testing.T) {
	buf := new(bytes.Buffer)
	WriteString(buf, "")

	equals(t, 1, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(0), n)
	ok(t, err)
}
