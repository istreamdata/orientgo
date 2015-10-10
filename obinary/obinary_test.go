package obinary_test

import (
	"bytes"
	"fmt"
	"gopkg.in/istreamdata/orientgo.v2"
	"gopkg.in/istreamdata/orientgo.v2/obinary"
	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

func TestReadErrorResponseWithSingleException(t *testing.T) {
	buf := new(bytes.Buffer)
	bw := rw.NewWriter(buf)
	bw.WriteByte(byte(1)) // indicates continue of exception class/msg array
	bw.WriteStrings("org.foo.BlargException", "wibble wibble!!")
	bw.WriteByte(byte(0)) // indicates end of exception class/msg array
	bw.WriteBytes([]byte("this is a stacktrace simulator\nEOL"))

	var serverExc error
	serverExc = obinary.ReadErrorResponse(rw.NewReader(buf))

	e, ok := serverExc.(orient.OServerException)
	if !ok {
		t.Fatal("wrong exception type")
	}
	equals(t, 1, len(e.Exceptions))

	equals(t, "org.foo.BlargException", e.Exceptions[0].ExcClass())
	equals(t, "wibble wibble!!", e.Exceptions[0].ExcMessage())
}

func TestReadErrorResponseWithMultipleExceptions(t *testing.T) {
	buf := new(bytes.Buffer)
	bw := rw.NewWriter(buf)
	bw.WriteByte(byte(1)) // indicates more exceptions to come
	bw.WriteStrings("org.foo.BlargException", "Too many blorgles!!")
	bw.WriteByte(byte(1)) // indicates more exceptions to come
	bw.WriteStrings("org.foo.FeebleException", "Not enough juice")
	bw.WriteByte(byte(1)) // indicates more exceptions to come
	bw.WriteStrings("org.foo.WobbleException", "Orbital decay")
	bw.WriteByte(byte(0)) // indicates end of exceptions
	bw.WriteBytes([]byte("this is a stacktrace simulator\nEOL"))

	serverExc := obinary.ReadErrorResponse(rw.NewReader(buf))

	e, ok := serverExc.(orient.OServerException)
	if !ok {
		t.Fatal("wrong exception type")
	}

	equals(t, "org.foo.BlargException", e.Exceptions[0].ExcClass())
	equals(t, "Not enough juice", e.Exceptions[1].ExcMessage())
	equals(t, "org.foo.WobbleException", e.Exceptions[2].ExcClass())
	equals(t, "Orbital decay", e.Exceptions[2].ExcMessage())
}
