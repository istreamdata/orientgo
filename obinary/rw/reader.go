// rw is the read-write package for reading and writing types
// from the OrientDB binary network protocol.  Reading is done
// via io.Reader and writing is done to bytes.Buffer (since the
// extra functionality of byte.Buffer is desired).  All the
// OrientDB types are represented here for non-encoded forms.
// For varint and zigzag encoding/decoding handling use the
// obinary/varint package instead.
package rw

import (
	"encoding/binary"
	"fmt"
	"io"
)

var Order = binary.BigEndian

func NewReader(r io.Reader) *Reader {
	switch br := r.(type) {
	case *Reader:
		return br
	case *ReadSeeker:
		return br.Reader
	}
	br := &Reader{R: r}
	br.br = byteReader{br}
	return br
}

type Reader struct {
	err error
	br  byteReader
	R   io.Reader
}

func (r *Reader) Err() error {
	return r.err
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.R.Read(p)
}

func (r *Reader) read(v interface{}) error {
	if r.err != nil {
		return r.err
	}
	if err := binary.Read(r.R, Order, v); err != nil {
		r.err = err
	}
	return r.err
}

func (r *Reader) ReadRawBytes(buf []byte) error {
	if r.err != nil {
		return r.err
	}
	if _, err := io.ReadFull(r.R, buf); err != nil {
		r.err = err
	}
	return r.err
}

// ReadBytes reads in an OrientDB byte array.  It reads the first 4 bytes
// from the Reader as an int to determine the length of the byte array
// to read in.
// If the specified size of the byte array is 0 (empty) or negative (null)
// nil is returned for the []byte.
func (r *Reader) ReadBytes() []byte {
	// the first four bytes give the length of the remaining byte array
	sz := r.ReadInt()
	// sz of 0 indicates empty byte array
	// sz of -1 indicates null value
	// for now, I'm returning nil []byte for both
	if sz <= 0 {
		return nil
	}

	b := make([]byte, sz)
	r.ReadRawBytes(b)
	return b
}

// ReadString xxxx
// If the string size is 0 an empty string and nil error are returned
func (r *Reader) ReadString() string {
	return string(r.ReadBytes())
}

func (r *Reader) ReadByte() byte {
	readbuf := make([]byte, 1)
	r.Read(readbuf)
	return readbuf[0]
}

func (r *Reader) ReadInt() (v int32) {
	r.read(&v)
	return
}

func (r *Reader) ReadLong() (v int64) {
	r.read(&v)
	return
}

func (r *Reader) ReadShort() (v int16) {
	r.read(&v)
	return
}

func (r *Reader) ReadFloat() (v float32) {
	r.read(&v)
	return
}

func (r *Reader) ReadDouble() (v float64) {
	r.read(&v)
	return
}

// Reads one byte from the Reader. If the byte is zero, then false is returned,
// otherwise true.  If error is non-nil, then the bool value is undefined.
func (r *Reader) ReadBool() bool {
	// non-zero is true
	return r.ReadByte() != byte(0)
}

type ByteReader interface {
	io.Reader
	io.ByteReader
}

type byteReader struct {
	r *Reader
}

func (r byteReader) ReadByte() (byte, error) {
	return r.r.ReadByte(), r.r.Err()
}

// ReadVarIntAndDecode64 reads a varint from r to a uint64
// and then zigzag decodes it to an int64 value.
func (r *Reader) ReadVarint() int64 {
	v, err := binary.ReadVarint(r.br)
	if err != nil {
		r.err = err
	}
	return v
}

// ReadVarIntToUint reads a variable length integer from the input buffer.
// The inflated integer is written is returned as a uint64 value.
// This method only "inflates" the varint into a uint64; it does NOT
// zigzag decode it.
func (r *Reader) ReadUvarint() uint64 {
	v, err := binary.ReadUvarint(r.br)
	if err != nil {
		r.err = err
	}
	return v
}

// varint.ReadBytes, like rw.ReadBytes, first reads a length from the
// input buffer and then that number of bytes into a []byte from the
// input buffer. The difference is that the integer indicating the length
// of the byte array to follow is a zigzag encoded varint.
func (r *Reader) ReadBytesVarint() []byte {
	// an encoded varint give the length of the remaining byte array
	lenbytes := r.ReadVarint()
	if lenbytes == 0 {
		return nil
	} else if lenbytes < 0 {
		panic(fmt.Errorf("Error in varint.ReadBytes: size of bytes was less than zero: %v", lenbytes))
	}

	data := make([]byte, int(lenbytes))
	r.ReadRawBytes(data)
	return data
}

// varint.ReadString, like rw.ReadString, first reads a length from the
// input buffer and then that number of bytes (of ASCII chars) into a string
// from the input buffer. The difference is that the integer indicating the
// length of the byte array to follow is a zigzag encoded varint.
func (r *Reader) ReadStringVarint() string {
	return string(r.ReadBytesVarint())
}

func NewReadSeeker(r io.ReadSeeker) *ReadSeeker {
	if br, ok := r.(*ReadSeeker); ok {
		return br
	}
	return &ReadSeeker{Reader: NewReader(r), S: r}
}

type ReadSeeker struct {
	*Reader
	S io.Seeker
}

func (r *ReadSeeker) Seek(off int64, whence int) (int64, error) {
	if r.Reader.err != nil {
		cur, _ := r.S.Seek(0, 1)
		return cur, r.Reader.err
	}
	cur, err := r.S.Seek(off, whence)
	if err != nil {
		r.Reader.err = err
	}
	return cur, err
}
