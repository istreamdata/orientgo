package obinary

import "testing"

func TestZigZagEncodeUInt32(t *testing.T) {
	n := int32(18)
	z := zigzagEncodeUInt32(n)
	equals(t, uint32(36), z)

	n = int32(-18)
	z = zigzagEncodeUInt32(n)
	equals(t, uint32(35), z)

	n = int32(0)
	z = zigzagEncodeUInt32(n)
	equals(t, uint32(0), z)

	n = int32(-1)
	z = zigzagEncodeUInt32(n)
	equals(t, uint32(1), z)

	n = int32(MaxInt)
	z = zigzagEncodeUInt32(n)
	equals(t, uint32(MaxUint-1), z)

	n = int32(MinInt)
	z = zigzagEncodeUInt32(n)
	equals(t, uint32(MaxUint), z)
}

func TestZigZagDecodeInt32(t *testing.T) {
	z := uint32(36)
	n := zigzagDecodeInt32(z)
	equals(t, int32(18), n)

	z = uint32(35)
	n = zigzagDecodeInt32(z)
	equals(t, int32(-18), n)

	z = uint32(0)
	n = zigzagDecodeInt32(z)
	equals(t, int32(0), n)

	z = uint32(1)
	n = zigzagDecodeInt32(z)
	equals(t, int32(-1), n)

	z = uint32(MaxUint - 1)
	n = zigzagDecodeInt32(z)
	equals(t, int32(MaxInt), n)

	z = uint32(MaxUint)
	n = zigzagDecodeInt32(z)
	equals(t, int32(MinInt), n)
}

func TestZigZagEncodeUInt64(t *testing.T) {
	n := int64(18)
	z := zigzagEncodeUInt64(n)
	equals(t, uint64(36), z)

	n = int64(-18)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(35), z)

	n = int64(0)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(0), z)

	n = int64(-1)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(1), z)

	n = int64(MaxInt)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(MaxUint-1), z)

	n = int64(MinInt)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(MaxUint), z)

	n = int64(MaxInt64)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(MaxUint64-1), z)

	n = int64(MinInt64)
	z = zigzagEncodeUInt64(n)
	equals(t, uint64(MaxUint64), z)
}

func TestZigZagDecodeInt64(t *testing.T) {
	z := uint64(36)
	n := zigzagDecodeInt64(z)
	equals(t, int64(18), n)

	z = uint64(35)
	n = zigzagDecodeInt64(z)
	equals(t, int64(-18), n)

	z = uint64(0)
	n = zigzagDecodeInt64(z)
	equals(t, int64(0), n)

	z = uint64(1)
	n = zigzagDecodeInt64(z)
	equals(t, int64(-1), n)

	z = uint64(MaxUint - 1)
	n = zigzagDecodeInt64(z)
	equals(t, int64(MaxInt), n)

	z = uint64(MaxUint)
	n = zigzagDecodeInt64(z)
	equals(t, int64(MinInt), n)

	z = uint64(MaxUint64 - 1)
	n = zigzagDecodeInt64(z)
	equals(t, int64(MaxInt64), n)

	z = uint64(MaxUint64)
	n = zigzagDecodeInt64(z)
	equals(t, int64(MinInt64), n)
}
