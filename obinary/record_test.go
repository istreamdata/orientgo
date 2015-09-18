package obinary_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary"
	"github.com/istreamdata/orientgo/obinary/rw"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestDeserializeRecordData(t *testing.T) {
	data, err := base64.StdEncoding.DecodeString(`AAASY2FyZXRha2VyAAAAJQcIbmFtZQAAAC0HBmFnZQAAADMBAA5NaWNoYWVsCkxpbnVzHg==`)
	if err != nil {
		t.Fatal(err)
	}

	rec := orient.NewDocumentRecord()
	rec.SetSerializer(&obinary.BinaryRecordFormat{})
	rec.Fill(orient.NewEmptyRID(), 0, data)

	if doc, err := rec.ToDocument(); err != nil {
		t.Fatal(err)
	} else if len(doc.Fields) != 3 {
		t.Fatal("wrong fields count in document")
	} else if doc.GetField("caretaker").Value.(string) != "Michael" ||
		doc.GetField("name").Value.(string) != "Linus" ||
		doc.GetField("age").Value.(int32) != 15 {
		t.Fatal("wrong values in document: ", doc)
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

func TestReadErrorResponseWithSingleException(t *testing.T) {
	buf := new(bytes.Buffer)
	rw.WriteByte(buf, byte(1)) // indicates continue of exception class/msg array
	rw.WriteStrings(buf, "org.foo.BlargException", "wibble wibble!!")
	rw.WriteByte(buf, byte(0)) // indicates end of exception class/msg array
	rw.WriteBytes(buf, []byte("this is a stacktrace simulator\nEOL"))

	var serverExc error
	serverExc = obinary.ReadErrorResponse(buf)

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
	rw.WriteByte(buf, byte(1)) // indicates more exceptions to come
	rw.WriteStrings(buf, "org.foo.BlargException", "Too many blorgles!!")
	rw.WriteByte(buf, byte(1)) // indicates more exceptions to come
	rw.WriteStrings(buf, "org.foo.FeebleException", "Not enough juice")
	rw.WriteByte(buf, byte(1)) // indicates more exceptions to come
	rw.WriteStrings(buf, "org.foo.WobbleException", "Orbital decay")
	rw.WriteByte(buf, byte(0)) // indicates end of exceptions
	rw.WriteBytes(buf, []byte("this is a stacktrace simulator\nEOL"))

	serverExc := obinary.ReadErrorResponse(buf)

	e, ok := serverExc.(orient.OServerException)
	if !ok {
		t.Fatal("wrong exception type")
	}

	equals(t, "org.foo.BlargException", e.Exceptions[0].ExcClass())
	equals(t, "Not enough juice", e.Exceptions[1].ExcMessage())
	equals(t, "org.foo.WobbleException", e.Exceptions[2].ExcClass())
	equals(t, "Orbital decay", e.Exceptions[2].ExcMessage())
}
