package varint

import (
	"bytes"
	"testing"

	"github.com/quux00/ogonori/constants"
)

func TestWriteBytes_GoodData_5Bytes(t *testing.T) {
	buf := new(bytes.Buffer)
	err := WriteBytes(buf, []byte("total"))
	ok(t, err)

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
	err := WriteString(buf, "total")
	ok(t, err)

	equals(t, 6, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(10), n) // zigzag encoded value of 5 is 10
	ok(t, err)

	strbytes := buf.Next(5)
	equals(t, "total", string(strbytes))
}

func TestWriteString_GoodData_EmptyString(t *testing.T) {
	buf := new(bytes.Buffer)
	err := WriteString(buf, "")
	ok(t, err)

	equals(t, 1, buf.Len())

	n, err := buf.ReadByte()
	equals(t, byte(0), n)
	ok(t, err)
}

// This one is slow and slows done rapid testing, so commented out for now
func xTestWriteBytes_VeryLargeArrayRequires64BitVarintEncode(t *testing.T) {
	lbsize := int64(constants.MaxInt) + 4
	largebytes := make([]byte, lbsize)
	// set some sentinel values to check later
	largebytes[0] = byte(255)
	largebytes[10] = byte(255)
	largebytes[100] = byte(255)

	buf := new(bytes.Buffer)
	err := WriteBytes(buf, largebytes)
	ok(t, err)

	equals(t, 5+len(largebytes), buf.Len()) // takes 5 bytes to varint encode this size

	// an error should be returned
	outbytes, err := ReadBytes(buf)
	ok(t, err)
	equals(t, lbsize, int64(len(outbytes)))
}
