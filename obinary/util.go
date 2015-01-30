package obinary

//
// ToIntBigEndian converts the first 4 bytes of a byte slice into an int
// using BigEndian ordering in the byte slice
// The `bs` byte slice must have at least 4 bytes or this function will panic
//
func ToIntBigEndian(bs []byte) int {
	return int(bs[3]) | int(bs[2])<<8 | int(bs[1])<<16 | int(bs[0])<<24
}

func validStorageType(storageType string) bool {
	return storageType == PersistentStorageType || storageType == VolatileStorageType
}

func validDbType(dbtype string) bool {
	return dbtype == PersistentStorageType || dbtype == VolatileStorageType
}

func zigzagEncodeUInt32(n int32) uint32 {
	return uint32((n >> 31) ^ (n << 1))
}

func zigzagDecodeInt32(n uint32) int32 {
	return int32((-(n & 1)) ^ (n >> 1))
}

func zigzagEncodeUInt64(n int64) uint64 {
	return uint64((n >> 63) ^ (n << 1))
}

func zigzagDecodeInt64(n uint64) int64 {
	return int64((-(n & 1)) ^ (n >> 1))
}
