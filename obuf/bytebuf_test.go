package obuf

import (
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestRead(t *testing.T) {
	bs := []byte("hello there")
	buf := NewBuffer(bs)
	rdbs := make([]byte, 5)
	n, err := buf.Read(rdbs)
	ok(t, err)
	equals(t, 5, n)
	equals(t, string(rdbs), "hello")

	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, 5, n)
	equals(t, string(rdbs), " ther")

	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, 1, n)
	equals(t, string(rdbs[0:n]), "e")
}

func TestSkip(t *testing.T) {
	bs := []byte("hello there 123")
	buf := NewBuffer(bs)
	buf.Skip(2)
	rdbs := make([]byte, 4)
	n, err := buf.Read(rdbs)
	ok(t, err)
	equals(t, 4, n)
	equals(t, string(rdbs), "llo ")

	buf.Skip(5)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, 4, n)
	equals(t, string(rdbs), " 123")
}

func TestLenAndFullLen(t *testing.T) {
	bs := []byte("hello there 123")
	buf := NewBuffer(bs)
	equals(t, len(bs), buf.Len())
	equals(t, len(bs), buf.FullLen())

	rdbs := make([]byte, 1)
	n, err := buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "h")

	equals(t, len(bs)-1, buf.Len())
	equals(t, len(bs), buf.FullLen())

	buf.Skip(2)

	equals(t, len(bs)-3, buf.Len())
	equals(t, len(bs), buf.FullLen())

	rdbs = make([]byte, 2)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "lo")

	buf.Seek(1)

	equals(t, len(bs)-1, buf.Len())
	equals(t, len(bs), buf.FullLen())

	rdbs = make([]byte, 10)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "ello there")

	equals(t, len(bs)-11, buf.Len())
	equals(t, len(bs), buf.FullLen())

	buf.Seek(uint(buf.FullLen()))
	equals(t, 0, buf.Len())
	equals(t, len(bs), buf.FullLen())
}

func TestSeek(t *testing.T) {
	//            0123456789012345678
	bs := []byte("hello there 123 xxy")
	buf := NewBuffer(bs)
	buf.Seek(5)
	rdbs := make([]byte, 6)
	n, err := buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), " there")

	buf.Seek(8)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "ere 12")

	buf.Seek(1)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "ello t")

	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "here 1")

	buf.Seek(uint(buf.FullLen() - 1))
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "y")

	buf.Seek(uint(buf.FullLen()))
	_, err = buf.Read(rdbs)
	assert(t, err == io.EOF, "should have EOF")
}

func TestSeekBeyondRangeShouldPanic(t *testing.T) {
	panicked := false
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
		assert(t, panicked, "should have panicked")
	}()

	//            012345678901234
	bs := []byte("hello there 123")
	buf := NewBuffer(bs)
	buf.Seek(22)

	assert(t, false, "should not get here")
}

func TestSkipBeyondRangeShouldNotPanicJustReturnEOFOnRead(t *testing.T) {
	//            012345678901234
	bs := []byte("hello there 123")
	buf := NewBuffer(bs)
	buf.Skip(22)

	rdbs := make([]byte, 6)
	n, err := buf.Read(rdbs)
	assert(t, err == io.EOF, "should have EOF")
	equals(t, 0, n)
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
