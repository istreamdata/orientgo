package obuf

import (
	"io"

	"testing"
)

func TestRead(t *testing.T) {
	bs := []byte("hello there")
	buf := NewReadBuffer(bs)
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
	buf := NewReadBuffer(bs)
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

func TestUnreadByte(t *testing.T) {
	bs := []byte("hello there 123")
	buf := NewReadBuffer(bs)
	buf.Skip(2)
	rdbs := make([]byte, 4)
	n, err := buf.Read(rdbs)
	ok(t, err)
	equals(t, 4, n)
	equals(t, string(rdbs), "llo ")

	err = buf.UnreadByte()
	ok(t, err)

	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, 4, n)
	equals(t, string(rdbs), " the")

	err = buf.UnreadByte()
	ok(t, err)

	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, 4, n)
	equals(t, string(rdbs), "ere ")

	err = buf.UnreadByte()
	ok(t, err)
	err = buf.UnreadByte()
	assert(t, err != nil, "Multiple unreads should not be permitted")
}

func TestLenAndFullLen(t *testing.T) {
	bs := []byte("hello there 123")
	buf := NewReadBuffer(bs)
	equals(t, len(bs), buf.Len())
	equals(t, len(bs), buf.Capacity())

	rdbs := make([]byte, 1)
	n, err := buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "h")

	equals(t, len(bs)-1, buf.Len())
	equals(t, len(bs), buf.Capacity())

	buf.Skip(2)

	equals(t, len(bs)-3, buf.Len())
	equals(t, len(bs), buf.Capacity())

	rdbs = make([]byte, 2)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "lo")

	buf.Seek(1)

	equals(t, len(bs)-1, buf.Len())
	equals(t, len(bs), buf.Capacity())

	rdbs = make([]byte, 10)
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "ello there")

	equals(t, len(bs)-11, buf.Len())
	equals(t, len(bs), buf.Capacity())

	buf.Seek(uint(buf.Capacity()))
	equals(t, 0, buf.Len())
	equals(t, len(bs), buf.Capacity())
}

func TestSeek(t *testing.T) {
	//            0123456789012345678
	bs := []byte("hello there 123 xxy")
	buf := NewReadBuffer(bs)
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

	buf.Seek(uint(buf.Capacity() - 1))
	n, err = buf.Read(rdbs)
	ok(t, err)
	equals(t, string(rdbs[0:n]), "y")

	buf.Seek(uint(buf.Capacity()))
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
	buf := NewReadBuffer(bs)
	buf.Seek(22)

	assert(t, false, "should not get here")
}

func TestSkipBeyondRangeShouldNotPanicJustReturnEOFOnRead(t *testing.T) {
	//            012345678901234
	bs := []byte("hello there 123")
	buf := NewReadBuffer(bs)
	buf.Skip(22)

	rdbs := make([]byte, 6)
	n, err := buf.Read(rdbs)
	assert(t, err == io.EOF, "should have EOF")
	equals(t, 0, n)
}
