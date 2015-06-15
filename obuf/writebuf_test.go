package obuf

import (
	"encoding/binary"
	"testing"
)

const LongString = "1234567890 abcdefghijklmnopqrstuvwxyz -- goodnight irene ....!"

func TestWrites(t *testing.T) {
	input := []byte("hello there")

	wbuf := NewWriteBuffer(100)
	equals(t, 0, wbuf.Len())
	equals(t, 100, wbuf.Capacity())
	n, err := wbuf.Write(input)
	ok(t, err)
	equals(t, len(input), n)
	equals(t, len(input), wbuf.Len())
	equals(t, 100, wbuf.Capacity())

	equals(t, "hello there", string(wbuf.Bytes()))

	wbuf.WriteByte(0x02)
	wbuf.WriteByte(0x0a)
	wbuf.WriteByte(0xff)
	equals(t, len(input)+3, wbuf.Len())
	equals(t, 100, wbuf.Capacity())

	bs := wbuf.Bytes()
	equals(t, "hello there", string(bs[0:len(input)]))

	bs = bs[len(input):]
	equals(t, byte(0x02), bs[0])
	equals(t, byte(0x0a), bs[1])
	equals(t, byte(0xff), bs[2])

	n, err = wbuf.Write([]byte{44, 43, 42})
	ok(t, err)
	equals(t, 3, n)
	bs = wbuf.Bytes()
	equals(t, "hello there", string(bs[0:len(input)]))

	equals(t, byte(43), bs[len(bs)-2])
}

func TestReset(t *testing.T) {
	wbuf := NewWriteBuffer(25)

	wbuf.WriteByte(0x66)
	equals(t, 1, wbuf.Len())
	wbuf.Reset()
	equals(t, 0, wbuf.Len())
	equals(t, 25, wbuf.Capacity())
	bs := wbuf.Bytes()
	equals(t, bs, []byte{})

	wbuf.WriteByte(0xaa)
	input := []byte("hello there")
	n, err := wbuf.Write(input)
	ok(t, err)
	equals(t, len(input), n)
	equals(t, 1+len(input), wbuf.Len())
	wbuf.Reset()
	equals(t, 0, wbuf.Len())
	equals(t, bs, []byte{})
}

func TestGrowingBuffer(t *testing.T) {
	wbuf := NewWriteBuffer(5)

	wbuf.WriteByte(0xa6)
	wbuf.WriteByte(0xb6)
	wbuf.WriteByte(0xc6)
	wbuf.WriteByte(0xd6)
	wbuf.WriteByte(0xe6)
	equals(t, 5, wbuf.Len())
	equals(t, 5, wbuf.Capacity())

	wbuf.WriteByte(0xf6)
	equals(t, 6, wbuf.Len())
	assert(t, wbuf.Capacity() > 5, "Capacity should grow")

	bs := wbuf.Bytes()
	equals(t, byte(0xa6), bs[0])
	equals(t, byte(0xc6), bs[2])
	equals(t, byte(0xe6), bs[4])

	cap := wbuf.Capacity()
	wbuf.WriteByte(0xf7)
	equals(t, cap, wbuf.Capacity())

	strbytes := []byte(LongString)
	n, err := wbuf.Write(strbytes)
	ok(t, err)
	equals(t, n, len(strbytes))
	assert(t, wbuf.Capacity() > cap, "Capacity should grow")
	bs = wbuf.Bytes()
	equals(t, byte(0xa6), bs[0])
	equals(t, byte(0xc6), bs[2])
	equals(t, byte(0xf6), bs[5])
	equals(t, byte(0xf7), bs[6])
	equals(t, byte('1'), bs[7])
	equals(t, byte('3'), bs[9])
	equals(t, byte('!'), bs[len(bs)-1])
}

func TestSkipWrite(t *testing.T) {
	wbuf := NewWriteBuffer(1)
	wbuf.WriteByte(0xa6)
	equals(t, 1, wbuf.Len())

	wbuf.Skip(20)
	assert(t, wbuf.Capacity() > 21, "should have expanded cap")
	wbuf.WriteByte(0xe6)
	equals(t, 22, wbuf.Len())

	wbuf.Skip(100)
	equals(t, 22, wbuf.Len())
	posAfterSkip100 := wbuf.Len() + 100
	n, err := wbuf.Write([]byte(LongString))
	ok(t, err)
	equals(t, len(LongString), n)
	expLen := 22 + 100 + len(LongString)
	equals(t, expLen, wbuf.Len())

	bs := wbuf.Bytes()
	equals(t, expLen, len(bs))
	equals(t, byte(0xa6), bs[0])
	equals(t, byte(0x00), bs[1])
	equals(t, byte(0x00), bs[20])
	equals(t, byte(0xe6), bs[21])
	equals(t, byte(0x00), bs[posAfterSkip100-1])
	equals(t, LongString, string(bs[posAfterSkip100:]))
}

func TestSkipAndSeekWrite(t *testing.T) {
	wbuf := NewWriteBuffer(100)

	//   0   1   2   3  4  5  6  7  8   9   10   11   12
	// [a6, b6, c6, d6, 0, 0, 0, 0, 0, e6, 'E', 'O', 'L']

	wbuf.WriteByte(0xa6)
	wbuf.WriteByte(0xb6)
	wbuf.WriteByte(0xc6)
	wbuf.WriteByte(0xd6)

	wbuf.Skip(5)
	wbuf.WriteByte(0xe6)
	equals(t, 10, wbuf.Len())
	n, err := wbuf.Write([]byte("EOL"))
	ok(t, err)
	equals(t, 3, n)
	equals(t, 13, wbuf.Len())

	bs := wbuf.Bytes()
	equals(t, byte(0xd6), bs[3])
	equals(t, byte(0x0), bs[4])
	equals(t, byte(0x0), bs[8])
	equals(t, byte(0xe6), bs[9])
	equals(t, "EOL", string(bs[10:]))

	wbuf.Seek(4)
	equals(t, 13, wbuf.Len())
	err = binary.Write(wbuf, binary.BigEndian, int32(88509389))
	ok(t, err)
	equals(t, 13, wbuf.Len())

	wbuf.WriteByte(0x07)
	equals(t, 13, wbuf.Len())

	//   0   1   2   3   4   5   6   7    8    9   10   11   12
	// [a6, b6, c6, d6, !0, !0, !0, !0, 0x07, e6, 'E', 'O', 'L']
	//                  <-BigEnd int->

	bs = wbuf.Bytes()
	equals(t, byte(0xd6), bs[3])
	equals(t, byte(0xe6), bs[9])
	equals(t, "EOL", string(bs[10:]))
	// newly written bytes
	equals(t, byte(0x07), bs[8])

	decodedInt := binary.BigEndian.Uint32(bs[4:8])
	equals(t, 88509389, int(decodedInt))

	equals(t, 100, wbuf.Capacity())

	wbuf.Skip(2) // to pos 11
	n, err = wbuf.Write([]byte("+"))
	ok(t, err)
	equals(t, 1, n)
	equals(t, byte('+'), bs[11])
}

func TestSeekWrite(t *testing.T) {
	wbuf := NewWriteBuffer(100)
	commonSeekTest(wbuf, t)
	equals(t, 100, wbuf.Capacity())
}

func TestSeekBeyondRangeShouldExpandGracefully(t *testing.T) {
	wbuf := NewWriteBuffer(6)
	commonSeekTest(wbuf, t)
	assert(t, wbuf.Capacity() > 6, "should have grown")
}

func TestSeekJumpAroundViaSeeks(t *testing.T) {
	wbuf := NewWriteBuffer(6)

	wbuf.WriteByte(0xa6)
	wbuf.Seek(500)
	wbuf.WriteByte(0xb6)
	wbuf.Seek(1244)
	n, err := wbuf.Write([]byte("hi mom"))
	ok(t, err)
	equals(t, len("hi mom"), n)
	wbuf.Seek(33)
	wbuf.WriteByte(0xc6)
	wbuf.Seek(1)
	wbuf.WriteByte(0xd6)
	wbuf.Seek(101)
	wbuf.WriteByte(0xe6)
	wbuf.Write([]byte("hi dad"))
	wbuf.Seek(10000)
	n, err = wbuf.Write([]byte(LongString))
	ok(t, err)
	equals(t, len(LongString), n)
	equals(t, 10000+len(LongString), wbuf.Len())
	assert(t, wbuf.Capacity() >= wbuf.Len(), "Capacity should have grown")

	bs := wbuf.Bytes()
	equals(t, byte(0xa6), bs[0])

	// overwrite the first pos
	wbuf.Seek(0)
	wbuf.WriteByte(0x11)
	bs = wbuf.Bytes()
	equals(t, byte(0x11), bs[0])

	// test the previous writes
	equals(t, byte(0xd6), bs[1])
	equals(t, byte(0x00), bs[499])
	equals(t, byte(0xb6), bs[500])
	equals(t, byte(0x00), bs[501])
	equals(t, "hi dad", string(bs[102:108]))
	equals(t, LongString, string(bs[10000:]))
}

func commonSeekTest(wbuf *WriteBuf, t *testing.T) {
	//   0   1   2   3  4  5  6  7  8   9   10   11   12
	// [a6, b6, c6, d6, 0, 0, 0, 0, 0, e6, 'E', 'O', 'L']

	wbuf.WriteByte(0xa6)
	wbuf.WriteByte(0xb6)
	wbuf.WriteByte(0xc6)
	wbuf.WriteByte(0xd6)

	wbuf.Seek(9)
	wbuf.WriteByte(0xe6)
	equals(t, 10, wbuf.Len())
	n, err := wbuf.Write([]byte("EOL"))
	ok(t, err)
	equals(t, 3, n)
	equals(t, 13, wbuf.Len())

	bs := wbuf.Bytes()
	equals(t, byte(0xd6), bs[3])
	equals(t, byte(0x0), bs[4])
	equals(t, byte(0x0), bs[8])
	equals(t, byte(0xe6), bs[9])
	equals(t, "EOL", string(bs[10:]))

	wbuf.Seek(4)
	equals(t, 13, wbuf.Len())
	err = binary.Write(wbuf, binary.BigEndian, int32(88509389))
	ok(t, err)
	equals(t, 13, wbuf.Len())

	wbuf.WriteByte(0x07)
	equals(t, 13, wbuf.Len())

	//   0   1   2   3   4   5   6   7    8    9   10   11   12
	// [a6, b6, c6, d6, !0, !0, !0, !0, 0x07, e6, 'E', 'O', 'L']
	//                  <-BigEnd int->

	bs = wbuf.Bytes()
	equals(t, byte(0xd6), bs[3])
	equals(t, byte(0xe6), bs[9])
	equals(t, "EOL", string(bs[10:]))
	// newly written bytes
	equals(t, byte(0x07), bs[8])

	decodedInt := binary.BigEndian.Uint32(bs[4:8])
	equals(t, 88509389, int(decodedInt))
}
