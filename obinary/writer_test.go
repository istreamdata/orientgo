package obinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestWriteBytes(t *testing.T) {
	buf := new(bytes.Buffer)
	byteMsg := []byte("I like Ike")
	err := WriteBytes(buf, byteMsg)
	ok(t, err)

	equals(t, 4+len(byteMsg), buf.Len())
	bs := buf.Next(4)
	equals(t, len(byteMsg), bigEndianConvertToInt(bs))

	bs = buf.Next(len(byteMsg))
	equals(t, byteMsg, bs)
}

func TestWriteBool(t *testing.T) {
	buf := new(bytes.Buffer)
	err := WriteBool(buf, true)
	ok(t, err)
	err = WriteBool(buf, false)
	ok(t, err)
	err = WriteBool(buf, true)
	ok(t, err)

	equals(t, 3, buf.Len())
	bs := buf.Bytes()
	equals(t, byte(1), bs[0])
	equals(t, byte(0), bs[1])
	equals(t, byte(1), bs[2])
}

func TestWriteString(t *testing.T) {
	var buf bytes.Buffer
	err := WriteString(&buf, "hello")
	ok(t, err)
	equals(t, 9, buf.Len())

	n, s := nextBinaryString(&buf)
	equals(t, 5, n)
	equals(t, "hello", s)
}

func TestWriteStrings(t *testing.T) {
	buf := new(bytes.Buffer)
	err := WriteStrings(buf, "a", "a longer string", "golang")
	ok(t, err)
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
		err error
		bs  []byte
	)
	err = WriteByte(&buf, 0x1)
	ok(t, err)
	err = WriteString(&buf, "vått og tørt")
	ok(t, err)
	err = WriteShort(&buf, int16(29876))
	ok(t, err)
	err = WriteShort(&buf, int16(444))
	ok(t, err)
	err = WriteInt(&buf, 9999999)
	ok(t, err)

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
}

/* ---[ helper fns ]--- */

func nextBinaryString(buf *bytes.Buffer) (int, string) {
	intBytes := buf.Next(4)
	intVal := int(intBytes[3]) | int(intBytes[2])<<8 | int(intBytes[1])<<16 | int(intBytes[0])<<24

	strBytes := buf.Next(intVal)
	return intVal, string(strBytes)
}

// TODO: this may be wrong based on findings in production code -> need to research why
func bigEndianConvertToInt(bs []byte) int {
	return int(bs[3]) | int(bs[2])<<8 | int(bs[1])<<16 | int(bs[0])<<24
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n",
			filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
