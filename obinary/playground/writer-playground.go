package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func littleEndian(n int64) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	fmt.Printf("% x\n", buf.Bytes())
}

func bigEndian(n int64) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, n)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	fmt.Printf("% x\n", buf.Bytes())
}

func myAttempt(n int64) {
	var b [4]byte
	// the & Oxff is not necessary
	b[0] = byte((n >> 24) & 0xff)
	b[1] = byte((n >> 16) & 0xff)
	b[2] = byte((n >> 8) & 0xff)
	b[3] = byte(n & 0xff)
	fmt.Printf("% x\n", b)
}

func stringy() {
	phrase := "vått og tørt"
	fmt.Printf("string: \"%s\"\n", phrase)
	fmt.Printf("len as string: %d\n", len(phrase))
	fmt.Printf("len as bytes : %d\n", len([]byte(phrase)))
	fmt.Println("index rune char bytes")
	for index, char := range phrase {
		fmt.Printf("%-2d   %U  '%c'  % X\n",
			index, char, char,
			[]byte(string(char)))
	}
}

func main() {
	var n int64 = 893472220313
	fmt.Println("=== little ===")
	littleEndian(n)
	fmt.Println("=== big ===")
	bigEndian(n)
	fmt.Println("=== mine ===")
	myAttempt(n)
	fmt.Println("=== stringy ===")
	stringy()
}
