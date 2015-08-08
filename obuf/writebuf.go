//
// seekable byte buffer package
//
package obuf

import "errors"

var ErrWrite error = errors.New("Unable to write all bytes")

//
// WriteBuf implements the Writer interface.
// It is a Writer that allows relative Skips (forward)
// and absolute Seeks (forward and backwards).
//
type WriteBuf struct {
	bs  []byte // the full byte array
	off int    // offset for writing
	end int    // last position written to
}

//
// Constructor for creating a new WriteBuf.
// capacity sets the initial internal byte slice capacity.
//
func NewWriteBuffer(capacity int) *WriteBuf {
	return &WriteBuf{bs: make([]byte, capacity)}
}

//
// Resets internal pointers to forget any data already written
// to the buffer.  A new underlying byte array is NOT created
// so any new writes may modify the slice you received if you
// previously called the Bytes() method.
//
func (b *WriteBuf) Reset() {
	b.off = 0
	b.end = 0
}

//
// Skip moves ahead a relative n bytes from the current offset,
// which is determined as the end of the last write or the
// last Seek position, if that was the most recent operation.
// If n + currOffset is larger than the capacity of the underlying
// byte array, the buffer size capacity will be increased.
//
func (b *WriteBuf) Skip(n uint) {
	seekpos := n + uint(b.off)
	b.Seek(seekpos)
}

//
// Seek to an absolute position in the underlying byte array
// regardless of what part of the buffer has been read so far.
//
// If n is beyond the end of the underlying byte array, the
// buffer size capacity will be increased.
//
func (b *WriteBuf) Seek(n uint) {
	x := int(n)
	if x > len(b.bs) {
		gsz := min(x*2, len(b.bs)*2)
		b.grow(x + gsz)
	}
	b.off = x
}

func (b *WriteBuf) grow(min int) {
	newsz := max(len(b.bs), min)
	newbs := make([]byte, newsz)
	copy(newbs, b.bs)
	b.bs = newbs
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// Len returns the number of bytes written to the buffer
//
func (b *WriteBuf) Len() int {
	return b.end
}

//
// Capacity returns the number of bytes in the original byte slice
// regardless of current write position.
//
func (b *WriteBuf) Capacity() int {
	return len(b.bs)
}

//
// Write writes len(p) bytes from p to the underlying data stream. It
// returns the number of bytes written from p (0 <= n <= len(p)) and any
// error encountered that caused the write to stop early. Write must return
// a non-nil error if it returns n < len(p). Write must not modify the
// slice data, even temporarily.
//
func (b *WriteBuf) Write(p []byte) (n int, err error) {
	// usually end >= off, but after a Seek, off > end
	cursor := max(b.end, b.off)
	if len(p)+cursor > b.Capacity() {
		newsz := max(2*len(b.bs), len(p)+cursor+16) // TODO: this could be more intelligent
		b.grow(newsz)
	}
	n = copy(b.bs[b.off:], p)
	if n != len(p) {
		return n, ErrWrite
	}

	b.off += n
	if b.off > b.end {
		b.end = b.off
	}

	return n, nil
}

//
// WriteByte writes a single byte to the underlying byte slice.
// If the byte slice is not large enough a new one will be allocated.
// nil error is always returned, since no error is possible in this operation,
// so error can be safely ignored from this method.
//
func (wb *WriteBuf) WriteByte(b byte) error {
	// usually end >= off, but after a Seek, off > end
	cursor := max(wb.end, wb.off)
	if wb.Capacity()-cursor < 1 {
		wb.grow(2 * len(wb.bs))
	}
	wb.bs[wb.off] = b
	wb.off++
	if wb.off > wb.end {
		wb.end = wb.off
	}
	return nil
}

//
// Bytes returns a reference to the underlying byte array
// truncated to the last written byte. Any subsequent changes to
// the byte slice returned will silently change the slice held
// by this buffer (unless a reallocation and copy is done to
// increase the size).
//
func (b *WriteBuf) Bytes() []byte {
	return b.bs[:b.end]
}
