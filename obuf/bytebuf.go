//
// seekable byte buffer package
//
package obuf

import "bytes"

//
// ByteBuf implements the Reader interface. It wraps
// a bytes.Buffer but allows relative Skips (forward)
// and absolute Seeks (forward and backwards).
//
type ByteBuf struct {
	bs  []byte        // the full byte array
	buf *bytes.Buffer // buffer walks over the bs slice
}

//
// Constructor for creating a new ByteBuf.
// bs is the underlying byte array to read from.
//
func NewBuffer(bs []byte) *ByteBuf {
	return &ByteBuf{
		bs:  bs,
		buf: bytes.NewBuffer(bs),
	}
}

//
// Skip forward the specified number of bytes.
// n is interpreted as relative to the unread portion of the slice.
// You cannot skip backwards. To do that use the Seek method.
//
// If n is beyond the end of the underlying byte array, this
// method will NOT panic. Instead, the next read will just
// return EOF.
//
func (b *ByteBuf) Skip(n uint) {
	b.buf.Next(int(n))
}

//
// Seek to an absolute position in the underlying byte array
// regardless of what part of the buffer has been read so far.
//
// If n is beyond the end of the underlying byte array, this
// method will panic.
//
func (b *ByteBuf) Seek(n uint) {
	nn := int(n)
	if nn > len(b.bs) {
		panic("Position beyond the end of the underlying byte slice")
	}
	b.buf = bytes.NewBuffer(b.bs[nn:])
}

//
// Len returns the number of bytes of the unread portion of the slice
//
func (b *ByteBuf) Len() int {
	return b.buf.Len()
}

//
// FullLen returns the number of bytes in the original byte slice
// regardless of current read position.
// TODO: is this method needed?
//
func (b *ByteBuf) FullLen() int {
	return len(b.bs)
}

//
// Read reads the next len(p) bytes from the buffer or until the buffer is
// drained. The return value n is the number of bytes read. If the buffer
// has no data to return, err is io.EOF (unless len(p) is zero); otherwise
// it is nil.
//
func (b *ByteBuf) Read(p []byte) (n int, err error) {
	return b.buf.Read(p)
}

//
// UnreadByte unreads the last byte returned by the most recent read
// operation. If write has happened since the last read, UnreadByte
// returns an error.
//
func (b *ByteBuf) UnreadByte() error {
	return b.buf.UnreadByte()
}
