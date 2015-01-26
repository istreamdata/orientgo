package obinary

//
// ToIntBigEndian converts the first 4 bytes of a byte slice into an int
// using BigEndian ordering in the byte slice
// The `bs` byte slice must have at least 4 bytes or this function will panic
//
func ToIntBigEndian(bs []byte) int {
	return int(bs[3]) | int(bs[2])<<8 | int(bs[1])<<16 | int(bs[0])<<24
}
