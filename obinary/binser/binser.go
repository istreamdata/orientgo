package binser

import (
	"bytes"
	"errors"
	"fmt"
	"ogonori/obinary/binser/varint"
	"ogonori/obinary/rw"
	"ogonori/oschema"
	"runtime"
)

//
// TODO: this needs to move up to obinary package and be called ORecordSerializer IF
// the csv serializer will also support the same methods below ... need to research so leaving for now
//
type ORecordSerializer interface {
	//
	// Deserialize reads bytes from the bytes.Buffer and puts the data into the
	// ODocument object.  The ODocument must already be created; nil cannot be
	// passed in for the `doc` field.
	//
	Deserialize(doc *oschema.ODocument, buf *bytes.Buffer) error

	//
	// Deserialize reads bytes from the bytes.Buffer and updates the ODocument object
	// passed in, but only for the fields specified in the `fields` slice.
	// The ODocument must already be created; nil cannot be passed in for the `doc` field.
	//
	DeserializePartial(doc *oschema.ODocument, buf *bytes.Buffer, fields []string) error

	//
	// Serialize reads the ODocument and serializes to bytes into the bytes.Buffer.
	//
	Serialize(doc *oschema.ODocument, buf *bytes.Buffer) error

	//
	// SerializeClass gets the class from the ODocument and serializes it to bytes
	// into the bytes.Buffer.
	//
	SerializeClass(doc *oschema.ODocument, buf *bytes.Buffer) error
}

// NOTE: once there is a V1, the V0 code should be moved to its own file

//
// ORecordSerializerBinaryV0 implements the ORecordSerializerBinary
// interface for version 0
//
type ORecordSerializerV0 struct {
	// TODO: need any internal data?
}

func (ser ORecordSerializerV0) Deserialize(doc *oschema.ODocument, buf *bytes.Buffer) error {
	if doc == nil {
		return errors.New("ODocument reference passed into ORecordSerializerBinaryV0.Deserialize was null")
	}

	version, err := readSerializationVersion(buf)
	if err != nil {
		return err
	}
	if version != byte(0) {
		return fmt.Errorf("ORecordSerializerBinaryV0 can only de/serialize version 0. Serialization version from server was %d",
			version)
	}

	classname, err := readClassname(buf)
	if err != nil {
		return err
	}
	fmt.Printf("DEBUG 1: classname%v\n", classname)

	if doc.Classname == "" {
		doc.Classname = classname

	} else if doc.Classname != classname {
		return fmt.Errorf("Classname clash. Classname in ODocument is %s; classname in serialized record is %s",
			doc.Classname, classname)
	}

	header, err := readHeader(buf)
	if err != nil {
		return err
	}
	fmt.Printf("DEBUG 2: header%v\n", header)

	for i, fid := range header.fieldIds {
		ofield := doc.GetFieldById(fid)
		if ofield == nil {
			// TODO: have to look up the field metadata from the server
			ofield = &oschema.OField{ // FIXME: BOGUS!!
				Id:   fid,
				Name: fmt.Sprintf("foo%d", i),
				Typ:  oschema.STRING,
			}
		}
		err = readDataValue(buf, ofield)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ser ORecordSerializerV0) DeserializePartial(doc *oschema.ODocument,
	buf *bytes.Buffer, fields []string) error {

	// TODO: impl me
	return nil
}

func (ser ORecordSerializerV0) Serialize(doc *oschema.ODocument, buf *bytes.Buffer) error {
	return nil
}

func (ser ORecordSerializerV0) SerializeClass(doc *oschema.ODocument, buf *bytes.Buffer) error {
	return nil
}

type header struct {
	fieldIds []int32
	dataPtrs []int
}

/* ---[ helper fns ]--- */

func readSerializationVersion(buf *bytes.Buffer) (byte, error) {
	return buf.ReadByte()
}

func readClassname(buf *bytes.Buffer) (string, error) {
	var (
		cnameLen   int32
		cnameBytes []byte
		err        error
	)

	cnameLen, err = varint.ReadVarIntAndDecode32(buf)
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

func readHeader(buf *bytes.Buffer) (header, error) {
	var (
		b       byte
		fieldId int32
		ptr     int
		err     error
	)
	hdr := header{
		fieldIds: make([]int32, 0, 8),
		dataPtrs: make([]int, 0, 8),
	}

	for {
		b, err = buf.ReadByte()
		if err != nil {
			_, _, line, _ := runtime.Caller(0) // TODO: check if this is correct
			return header{}, fmt.Errorf("Error in binser.readHeader (line %d): %v", line-2, err)
		}
		// 0 marks the end of the header
		if b == byte(0) {
			break
		}

		if err = buf.UnreadByte(); err != nil {
			_, _, line, _ := runtime.Caller(0)
			return header{}, fmt.Errorf("Error in binser.readHeader (line %d): %v", line-2, err)
		}

		fieldId, err = decodeFieldIdInHeader(buf)
		if err != nil {
			return header{}, err
		}
		if fieldId < 0 {
			return header{},
				fmt.Errorf("Varint for field name len in binary serialization was negative: ", fieldId)
		}
		fmt.Printf(">>> fieldId: %v\n", fieldId) // DEBUG

		ptr, err = rw.ReadInt(buf)
		if err != nil {
			_, _, line, _ := runtime.Caller(0)
			return header{}, fmt.Errorf("Error in binser.readHeader (line %d): %v", line-2, err)
		}
		fmt.Printf(">>> ptr: %v\n", ptr) // DEBUG

		hdr.fieldIds = append(hdr.fieldIds, fieldId)
		hdr.dataPtrs = append(hdr.dataPtrs, ptr)
	}

	return hdr, nil
}

func readDataValue(buf *bytes.Buffer, field *oschema.OField) error {
	var (
		val interface{}
		err error
	)
	// TODO: add more cases
	switch field.Typ {
	case oschema.STRING:
		val, err = varint.ReadString(buf)
	case oschema.INTEGER:
		val, err = rw.ReadInt(buf)
	case oschema.SHORT:
		val, err = rw.ReadShort(buf)
	case oschema.BOOLEAN:
		val, err = rw.ReadBool(buf)
	case oschema.BINARY:
		val, err = varint.ReadBytes(buf)
	default:
		err = errors.New("UnsupportedType: binser.readDataValue doesn't support all types yet ...")
	}

	if err == nil {
		field.Value = val
	}
	return err
}

func encodeFieldIdForHeader(id int32) []byte {
	// TODO: impl me
	// formulate for encoding is:
	// zigzagEncode( (fieldId+1) * -1 )
	// and then turn in varint []byte
	return nil
}

func decodeFieldIdInHeader(buf *bytes.Buffer) (int32, error) {
	var uencoded uint32
	err := varint.ReadVarIntBuf(buf, &uencoded)
	if err != nil {
		return 0, err
	}
	iencoded := varint.ZigzagDecodeInt32(uencoded)
	fieldId := (iencoded * -1) + 1
	return fieldId, nil
}
