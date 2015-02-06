package binser

import (
	"bytes"
	"fmt"
	"ogonori/obinary"
	"ogonori/obinary/binser/varint"
)

/* ---[ data types ]--- */

type Header struct {
	fieldName string
	dataPtr   int
	dataType  byte
}

/* ---[ Functions ]--- */

func ParseSerializationVersion(buf *bytes.Buffer) (byte, error) {
	return buf.ReadByte()
}

func ParseClassname(buf *bytes.Buffer) (string, error) {
	var (
		cnameLen   int32
		cnameBytes []byte
		err        error
	)

	cnameLen, err = VarIntToInt32(buf)
	if err != nil {
		return "", err
	}
	if cnameLen < 0 {
		return "", fmt.Errorf("Varint for classname len in binary serialization was negative: ", cnameLen)
	}
	cnameBytes = buf.Next(int(cnameLen))
	if len(cnameBytes) != int(cnameLen) {
		return "",
			fmt.Errorf("Could not read expected number of bytes for className. Expected %d; Read: %d",
				cnameLen, len(cnameBytes))
	}

	return string(cnameBytes), nil
}

//
//
//
func ParseHeader(buf *bytes.Buffer) (Header, error) {
	nameLen, err := VarIntToInt32(buf)
	if err != nil {
		return Header{}, err
	}
	if nameLen < 0 {
		return Header{},
			fmt.Errorf("Varint for field name len in binary serialization was negative: ", nameLen)
	}

	fieldNameBytes := buf.Next(int(nameLen))
	if len(fieldNameBytes) != int(nameLen) {
		return Header{},
			fmt.Errorf("Could not read expected number of bytes for fieldname in header. Expected %d; Read: %d",
				nameLen, len(fieldNameBytes))
	}

	ptr, err := obinary.ReadInt(buf)
	if err != nil {
		return Header{}, err
	}

	dataType, err := obinary.ReadByte(buf)
	if err != nil {
		return Header{}, err
	}

	hdr := Header{field: string(fieldNameBytes),
		dataPtr:  ptr,
		dataType: dataType,
	}

	return hdr, nil
}

//
// VarIntToInt32 reads a varint from buf to a uint32
// and then zigzag decodes it to an int32 value.
//
func VarIntToInt32(buf *bytes.Buffer) (int32, error) {
	var encodedLen uint32
	err := varint.ReadVarIntBuf(buf, &encodedLen)
	if err != nil {
		return 0, err
	}
	return varint.ZigzagDecodeInt32(encodedLen), nil
}
