//
// binserde stands for binary Serializer/Deserializer.
// It holds the interface and implementations for SerDes for the
// OrientDB Network Binary Protocol.
//
package binserde

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"

	"github.com/quux00/ogonori/obinary/binserde/varint"
	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/oschema"
)

//
// ORecordSerializer is the interface for the binary Serializer/Deserializer.
// More than one implementation will be needed if/when OrientDB creates additional
// versions of the binary serialization format.
// TODO: may want to use this interface for the csv serializer also - if so need to move this interface up a level
//
type ORecordSerializer interface {
	//
	// Deserialize reads bytes from the bytes.Buffer and puts the data into the
	// ODocument object.  The ODocument must already be created; nil cannot be
	// passed in for the `doc` field.  The serialization version (the first byte
	// of the serialized record) should be stripped off (already read) from the
	// bytes.Buffer being passed in
	//
	Deserialize(doc *oschema.ODocument, buf *bytes.Buffer) error // TODO: should this take an io.Reader instead of *bytes.Buffer ???

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
	// the global properties (in record #0:1) are unique to each database (I think)
	// so each client database obj needs to have its own ORecordSerializerV0
	GlobalProperties map[int]oschema.OGlobalProperty // key: property-id (aka field-id)
}

//
// The serialization version (the first byte of the serialized record) should
// be stripped off (already read) from the bytes.Buffer being passed in
//
func (serde *ORecordSerializerV0) Deserialize(doc *oschema.ODocument, buf *bytes.Buffer) error {
	if doc == nil {
		return errors.New("ODocument reference passed into ORecordSerializerBinaryV0.Deserialize was null")
	}

	classname, err := readClassname(buf)
	if err != nil {
		return oerror.NewTrace(err)
	}

	doc.Classname = classname

	header, err := readHeader(buf)
	if err != nil {
		return oerror.NewTrace(err)
	}

	ofields := make([]*oschema.OField, 0, len(header.dataPtrs))

	if len(header.propertyNames) > 0 {
		// propertyNames naes are set when a query returns properties, not a full record/document
		// classname is an empty string in this case
		for i, pname := range header.propertyNames {
			ofield := &oschema.OField{
				Name: pname,
				Typ:  header.types[i],
			}
			ofields = append(ofields, ofield)
		}
	}

	if len(ofields) == 0 {
		// was a Document query which returns propertyIds, not property names
		for _, fid := range header.propertyIds {
			property, ok := serde.GlobalProperties[int(fid)]
			var ofield *oschema.OField
			if ok {
				ofield = &oschema.OField{
					Id:   fid,
					Name: property.Name,
					Typ:  property.Type,
				}
			} else {
				errmsg := fmt.Sprintf("TODO: Need refresh of GlobalProperties since property with id %d was not found", fid)
				panic(errmsg)
				// TODO: need to do a refresh of the GlobalProperties from the database and try again
				// if that fails then there is a bug in OrientDB, so throw an error
				//  NOTE: see the method refreshGlobalProperties() in dbCommands
			}
			ofields = append(ofields, ofield)
		}
	}

	// once the fields are created, we can now fill in the values
	for i, fld := range ofields {
		// if data ptr is 0 (NULL), then it has no entry/value in the serialized record
		if header.dataPtrs[i] != 0 {
			val, err := serde.readDataValue(buf, fld.Typ)
			if err != nil {
				return err
			}
			fld.Value = val
		}

		doc.Fields[fld.Name] = fld
	}

	return nil
}

//
// TODO: need to study what exactly this method is supposed to do and not do
//       -> check the Java driver version
//
func (serde *ORecordSerializerV0) DeserializePartial(doc *oschema.ODocument,
	buf *bytes.Buffer, fields []string) error {

	// TODO: impl me
	return nil
}

func (serde *ORecordSerializerV0) Serialize(doc *oschema.ODocument, buf *bytes.Buffer) error {
	return nil
}

func (serde *ORecordSerializerV0) SerializeClass(doc *oschema.ODocument, buf *bytes.Buffer) error {
	return nil
}

//
// header in the schemaless serialization format.
// Generally only one of propertyIds or propertyNames
// will be filled in, not both.
//
type header struct {
	propertyIds   []int32
	propertyNames []string
	dataPtrs      []int32
	types         []byte
}

/* ---[ helper fns ]--- */

func readClassname(buf *bytes.Buffer) (string, error) {
	var (
		cnameLen   int32
		cnameBytes []byte
		err        error
	)

	cnameLen, err = varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return "", oerror.NewTrace(err)
	}
	if cnameLen < 0 {
		return "", oerror.NewTrace(
			fmt.Errorf("Varint for classname len in binary serialization was negative: %d", cnameLen))
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
	hdr := header{
		propertyIds:   make([]int32, 0, 4),
		propertyNames: make([]string, 0, 4),
		dataPtrs:      make([]int32, 0, 8),
		types:         make([]byte, 0, 8),
	}

	for {
		decoded, err := varint.ReadVarIntAndDecode32(buf)
		if err != nil {
			_, _, line, _ := runtime.Caller(0)
			return header{}, fmt.Errorf("Error in binserde.readHeader (line %d): %v", line-2, err)
		}

		if decoded == 0 { // 0 marks end of header
			break

		} else if decoded > 0 {
			// have a property, not a document, so the number is a zigzag encoded length for string (property name)

			// read property name
			size := int(decoded)
			data := buf.Next(size)
			if len(data) != size {
				return header{}, oerror.IncorrectNetworkRead{Expected: size, Actual: len(data)}
			}
			hdr.propertyNames = append(hdr.propertyNames, string(data))

			// read data pointer
			ptr, err := rw.ReadInt(buf)
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}

			// read data type
			dataType, err := buf.ReadByte()
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}
			hdr.types = append(hdr.types, dataType)
			hdr.dataPtrs = append(hdr.dataPtrs, ptr)

		} else {
			// have a document, not a property, so the number is an encoded property id,
			// convert to (positive) property-id
			propertyId := decodeFieldIdInHeader(decoded)

			ptr, err := rw.ReadInt(buf)
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}

			hdr.propertyIds = append(hdr.propertyIds, propertyId)
			hdr.dataPtrs = append(hdr.dataPtrs, ptr)
		}
	}

	return hdr, nil
}

//
// readDataValue reads the next data section from `buf` according
// to the type of the property (property.Typ) and updates the OField object
// to have the value.
//
func (serde *ORecordSerializerV0) readDataValue(buf *bytes.Buffer, datatype byte) (interface{}, error) {
	var (
		val interface{}
		err error
	)
	// TODO: many cases unimplemented
	switch datatype {
	case oschema.BOOLEAN:
		val, err = rw.ReadBool(buf)
		fmt.Printf("DEBUG BOOL: +readDataVal val: %v\n", val) // DEBUG
	case oschema.INTEGER:
		val, err = varint.ReadVarIntAndDecode32(buf)
		fmt.Printf("DEBUG INT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.SHORT:
		val, err = rw.ReadShort(buf)
		fmt.Printf("DEBUG SHORT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.LONG:
		val, err = varint.ReadVarIntAndDecode64(buf)
		fmt.Printf("DEBUG LONG: +readDataVal val: %v\n", val) // DEBUG
	case oschema.FLOAT:
		val, err = rw.ReadFloat(buf)
		fmt.Printf("DEBUG FLOAT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.DOUBLE:
		val, err = rw.ReadDouble(buf)
		fmt.Printf("DEBUG DOUBLE: +readDataVal val: %v\n", val) // DEBUG
	case oschema.DATETIME:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue DATETIME NOT YET IMPLEMENTED")
	case oschema.DATE:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue DATE NOT YET IMPLEMENTED")
	case oschema.STRING:
		val, err = varint.ReadString(buf)
		fmt.Printf("DEBUG STR: +readDataVal val: %v\n", val) // DEBUG
	case oschema.BINARY:
		val, err = varint.ReadBytes(buf)
		fmt.Printf("DEBUG BINARY: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDRECORD:
		doc := oschema.NewDocument("")
		err = serde.Deserialize(doc, buf)
		val = interface{}(doc)
		// fmt.Printf("DEBUG EMBEDDEDREC: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDLIST:
		val, err = serde.readEmbeddedCollection(buf)
		// fmt.Printf("DEBUG EMBD-LIST: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDSET:
		val, err = serde.readEmbeddedCollection(buf) // TODO: may need to create a set type as well
		// fmt.Printf("DEBUG EMBD-SET: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDMAP:
		val, err = serde.readEmbeddedMap(buf)
		// fmt.Printf("DEBUG EMBD-MAP: +readDataVal val: %v\n", val) // DEBUG
	case oschema.LINK:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue LINK NOT YET IMPLEMENTED")
	case oschema.LINKLIST:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue LINKLIST NOT YET IMPLEMENTED")
	case oschema.LINKSET:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue LINKSET NOT YET IMPLEMENTED")
	case oschema.LINKMAP:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue LINKMAP NOT YET IMPLEMENTED")
	case oschema.BYTE:
		val, err = rw.ReadByte(buf)
		fmt.Printf("DEBUG BYTE: +readDataVal val: %v\n", val) // DEBUG
	case oschema.CUSTOM:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue CUSTOM NOT YET IMPLEMENTED")
	case oschema.DECIMAL:
		// TODO: impl me -> Java client uses BigDecimal for this
		panic("ORecordSerializerV0#readDataValue DECIMAL NOT YET IMPLEMENTED")
	case oschema.LINKBAG:
		panic("ORecordSerializerV0#readDataValue LINKBAG NOT YET IMPLEMENTED")
	default:
		// ANY and TRANSIENT are do nothing ops
	}

	return val, err
}

//
// readEmbeddedMap handles the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
func (serde *ORecordSerializerV0) readEmbeddedMap(buf *bytes.Buffer) (map[string]interface{}, error) {
	numRecs, err := varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	nrecs := int(numRecs)

	// final map to be returned
	m := make(map[string]interface{})

	// data structures for reading the map header section, which gives key names and
	// value types (and value ptrs, but I don't need those for the way I parse the data)
	keynames := make([]string, nrecs)
	valtypes := make([]byte, nrecs)

	// read map headers
	for i := 0; i < nrecs; i++ {
		keytype, err := rw.ReadByte(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if keytype != oschema.STRING {
			panic(fmt.Sprintf("ReadEmbeddedMap got a key datatype %v - but it should be 7 (string)", keytype))
		}
		keynames[i], err = varint.ReadString(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		_, err = rw.ReadInt(buf) // pointer - throwing away
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		valtypes[i], err = rw.ReadByte(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
	}

	// read map values
	for i := 0; i < nrecs; i++ {
		val, err := serde.readDataValue(buf, valtypes[i])
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		m[keynames[i]] = val
	}

	return m, nil
}

//
// readEmbeddedCollection handles both EMBEDDEDLIST and EMBEDDEDSET types.
// Java client API:
//     Collection<?> readEmbeddedCollection(BytesContainer bytes, Collection<Object> found, ODocument document) {
//     `found`` gets added to during the recursive iterations
//
func (serde *ORecordSerializerV0) readEmbeddedCollection(buf *bytes.Buffer) ([]interface{}, error) {
	nrecs, err := varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	datatype, err := rw.ReadByte(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if datatype != oschema.ANY {
		// NOTE: currently the Java client doesn't handle this case either, so safe for now
		panic(fmt.Sprintf("ReadEmbeddedList got a datatype %v - currently that datatype is not supported", datatype))
	}

	ary := make([]interface{}, int(nrecs))

	// loop over all recs
	for i := range ary {
		// if type is ANY (unknown), then the next byte specifies the type of record to follow
		itemtype, err := rw.ReadByte(buf)
		if itemtype == oschema.ANY {
			ary[i] = nil // this is what the Java client does
			continue
		}

		val, err := serde.readDataValue(buf, itemtype)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		ary[i] = val
	}

	return ary, nil
}

func encodeFieldIdForHeader(id int32) []byte {
	// TODO: impl me
	// formula for encoding is:
	// zigzagEncode( (propertyId+1) * -1 )
	// and then turn in varint []byte
	return nil
}

func decodeFieldIdInHeader(decoded int32) int32 {
	propertyId := (decoded * -1) - 1
	return propertyId
}
