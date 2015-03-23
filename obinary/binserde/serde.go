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
	"github.com/quux00/ogonori/odatastructure"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
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
func (serde *ORecordSerializerV0) DeserializePartial(doc *oschema.ODocument, buf *bytes.Buffer, fields []string) error {
	// TODO: impl me
	return nil
}

func (serde *ORecordSerializerV0) Serialize(doc *oschema.ODocument, buf *bytes.Buffer) (err error) {
	// need to create a new buffer for the serialized record for ptr value calculations,
	// since the incoming buffer (`buf`) already has a lot of stuff written to it (session-id, etc)
	// that are NOT part of the serialized record
	// NOTE: this method assumes the byte(0) (serialization version) has ALREADY been written to `buf`
	serdebuf := new(bytes.Buffer) // holds only the serialized value

	// write the serialization version in so that the later buffer size math works
	// we will remove it at the end
	err = rw.WriteByte(serdebuf, 0)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = varint.WriteString(serdebuf, doc.Classname)
	if err != nil {
		return oerror.NewTrace(err)
	}
	ogl.Debugf("serdebuf A: %v\n", serdebuf.Bytes()) // DEBUG

	ogl.Debugf("doc A: %v\n", doc) // DEBUG
	err = serde.writeSerializedRecord(serdebuf, doc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// append the serialized record onto the primary buffer
	bs := serdebuf.Bytes()
	ogl.Debugf("serdebuf B: %v\n", bs) // DEBUG
	n, err := buf.Write(bs[1:])        // remove the version byte at the beginning
	if err != nil {
		return oerror.NewTrace(err)
	}
	if n != len(bs)-1 {
		_, file, line, _ := runtime.Caller(0)
		return fmt.Errorf("ERROR: file: %s: line %d: Incorrect number of bytes written to bytes buffer. Expected %d; Actual %d",
			file, line, len(bs)-1, n)
	}

	return nil
}

func (serde *ORecordSerializerV0) writeSerializedRecord(buf *bytes.Buffer, doc *oschema.ODocument) (err error) {
	nfields := len(doc.Fields)
	ptrPos := make([]int, 0, nfields) // position in buf where data ptr int needs to be written
	ptrVal := make([]int, 0, nfields) // data ptr value to write into buf
	subPtrPos := make([]int, 0, 8)
	subPtrVal := make([]int, 0, 8)

	dataBuf := new(bytes.Buffer)

	if doc.Classname == "" {
		// serializing a property or SQL params map -> use propertyName
		for fldName, fld := range doc.Fields {
			// propertyName
			err = varint.WriteString(buf, fldName)
			if err != nil {
				return oerror.NewTrace(err)
			}
			ptrPos = append(ptrPos, buf.Len())
			// placeholder data pointer
			err = rw.WriteInt(buf, 0)
			if err != nil {
				return oerror.NewTrace(err)
			}
			// data value type
			ogl.Debugf("@@@ Writing data type: %v\n", fld.Typ)
			err = rw.WriteByte(buf, fld.Typ)
			if err != nil {
				return oerror.NewTrace(err)
			}

			ptrVal = append(ptrVal, dataBuf.Len())
			// write the data value to a separate `data` buffer
			dbufpos, dbufvals, err := serde.writeDataValue(dataBuf, fld.Value, fld.Typ)
			if err != nil {
				return oerror.NewTrace(err)
			}
			// DEBUG
			ogl.Debugf("wsrA: ptrPos  : %v\n", ptrPos)
			ogl.Debugf("wsrA: ptrVal  : %v\n", ptrVal)
			ogl.Debugf("wsrA: dbufpos : %v\n", dbufpos)
			ogl.Debugf("wsrA: dbufvals: %v\n", dbufvals)
			// END DEBUG

			if dbufpos != nil {
				subPtrPos = append(subPtrPos, dbufpos...)
				subPtrVal = append(subPtrVal, dbufvals...)
			}
		}

	} else {
		// serializing a full document (not just a property or SQL params map) -> use propertyId
		// TODO: fill in
		panic("ELSE block of ORecordSerializerV0#writeSerializedRecord is NOT YET IMPLEMENTED !!")
	}

	// write End of Header (EOH) marker
	err = rw.WriteByte(buf, 0)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// fill in placeholder data ptr positions
	endHdrPos := buf.Len()
	for i := range ptrVal {
		ptrVal[i] += endHdrPos
	}

	// adjust the databuf ptr positions and values which are relative to the start of databuf
	for i := range subPtrPos {
		ptrPos = append(ptrPos, subPtrPos[i]+endHdrPos)
		ptrVal = append(ptrVal, subPtrVal[i]+endHdrPos)
	}

	// make a complete serialized record into one buffer
	_, err = buf.Write(dataBuf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	bs := buf.Bytes()
	for i, pos := range ptrPos {
		// this buffer works off a slice from the `buf` buffer, so writing to it should modify the underlying `buf` buffer
		tmpBuf := bytes.NewBuffer(bs[pos : pos+4])
		tmpBuf.Reset() // reset ptr to start of slice so can overwrite the placeholder value
		err = rw.WriteInt(tmpBuf, int32(ptrVal[i]))
		if err != nil {
			return oerror.NewTrace(err)
		}
	}

	return nil
}

// func (serde *ORecordSerializerV0) writeHeader(doc *oschema.ODocument, buf *bytes.Buffer) (ptrPos []int, err error) {
// 	ptrPos := make([]int, 0, len(m)) // position in buf where data ptr int needs to be written

// 	if doc.Classname == "" {
// 		// serializing a property or SQL params map -> use propertyName
// 		for fldName, fld := range doc.Fields {
// 			// propertyName
// 			err = varint.WriteString(buf, fldName)
// 			if err != nil {
// 				return ptrPos, oerror.NewTrace(err)
// 			}
// 			ptrPos = append(ptrPos, buf.Len())
// 			// placeholder data pointer
// 			err = rw.WriteInt(buf, 0)
// 			if err != nil {
// 				return ptrPos, oerror.NewTrace(err)
// 			}
// 			// data value type
// 			err = rw.WriteByte(buf, fld.Typ)
// 			if err != nil {
// 				return ptrPos, oerror.NewTrace(err)
// 			}
// 		}

// 	} else {
// 		// serializing a full document (not just a property or SQL params map) -> use propertyId
// 		// TODO: fill in
// 	}

// 	// write End of Header (EOH) marker
// 	err = rw.WriteByte(buf, 0)
// 	if err != nil {
// 		return ptrPos, oerror.NewTrace(err)
// 	}
// 	return ptrPos, nil
// }

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
// writeDataValue is part of the Serialize functionality
// TODO: change name to writeSingleValue ?
//
func (serde *ORecordSerializerV0) writeDataValue(buf *bytes.Buffer, value interface{}, datatype byte) (ptrPos, ptrVal []int, err error) {
	switch datatype {
	case oschema.STRING:
		err = varint.WriteString(buf, value.(string))
		ogl.Debugf("DEBUG STR: -writeDataVal val: %v\n", value.(string)) // DEBUG
	case oschema.BOOLEAN:
		err = rw.WriteBool(buf, value.(bool))
		ogl.Debugf("DEBUG BOOL: -writeDataVal val: %v\n", value.(bool)) // DEBUG
	case oschema.INTEGER:
		err = varint.EncodeAndWriteVarInt32(buf, value.(int32))         // TODO: are serialized integers ALWAYS varint encoded?
		ogl.Debugf("DEBUG INT: -writeDataVal val: %v\n", value.(int32)) // DEBUG
	case oschema.SHORT:
		err = rw.WriteShort(buf, value.(int16))
		ogl.Debugf("DEBUG SHORT: -writeDataVal val: %v\n", value.(int16)) // DEBUG
	case oschema.LONG:
		err = varint.EncodeAndWriteVarInt64(buf, value.(int64))          // TODO: are serialized longs ALWAYS varint encoded?
		ogl.Debugf("DEBUG LONG: -writeDataVal val: %v\n", value.(int64)) // DEBUG
	case oschema.FLOAT:
		err = rw.WriteFloat(buf, value.(float32))
		ogl.Debugf("DEBUG FLOAT: -writeDataVal val: %v\n", value.(float32)) // DEBUG
	case oschema.DOUBLE:
		err = rw.WriteDouble(buf, value.(float64))
		ogl.Debugf("DEBUG DOUBLE: -writeDataVal val: %v\n", value.(float64)) // DEBUG
	case oschema.DATETIME:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue DATETIME NOT YET IMPLEMENTED")
	case oschema.DATE:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue DATE NOT YET IMPLEMENTED")
	case oschema.BINARY:
		err = varint.WriteBytes(buf, value.([]byte))
		ogl.Debugf("DEBUG BINARY: -writeDataVal val: %v\n", value.([]byte)) // DEBUG
	case oschema.EMBEDDEDRECORD:
		panic("ORecordSerializerV0#writeDataValue EMBEDDEDRECORD NOT YET IMPLEMENTED")
	case oschema.EMBEDDEDLIST:
		// val, err = serde.readEmbeddedCollection(buf)
		// ogl.Debugf("DEBUG EMBD-LIST: -writeDataVal val: %v\n", val) // DEBUG
		panic("ORecordSerializerV0#writeDataValue EMBEDDEDLIST NOT YET IMPLEMENTED")
	case oschema.EMBEDDEDSET:
		// val, err = serde.readEmbeddedCollection(buf) // TODO: may need to create a set type as well
		// ogl.Debugf("DEBUG EMBD-SET: -writeDataVal val: %v\n", val) // DEBUG
		panic("ORecordSerializerV0#writeDataValue EMBEDDEDSET NOT YET IMPLEMENTED")
	case oschema.EMBEDDEDMAP:
		ptrPos, ptrVal, err = serde.writeEmbeddedMap(buf, value.(odatastructure.OEmbeddedMap))
		ogl.Debugf("DEBUG EMBEDDEDMAP:  val %v\n", value.(odatastructure.OEmbeddedMap))
	case oschema.LINK:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue LINK NOT YET IMPLEMENTED")
	case oschema.LINKLIST:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue LINKLIST NOT YET IMPLEMENTED")
	case oschema.LINKSET:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue LINKSET NOT YET IMPLEMENTED")
	case oschema.LINKMAP:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue LINKMAP NOT YET IMPLEMENTED")
	case oschema.BYTE:
		err = rw.WriteByte(buf, value.(byte))
		ogl.Debugf("DEBUG BYTE: -writeDataVal val: %v\n", value.(byte)) // DEBUG
	case oschema.CUSTOM:
		// TODO: impl me
		panic("ORecordSerializerV0#writeDataValue CUSTOM NOT YET IMPLEMENTED")
	case oschema.DECIMAL:
		// TODO: impl me -> Java client uses BigDecimal for this
		panic("ORecordSerializerV0#writeDataValue DECIMAL NOT YET IMPLEMENTED")
	case oschema.LINKBAG:
		panic("ORecordSerializerV0#writeDataValue LINKBAG NOT YET IMPLEMENTED")
	default:
		// ANY and TRANSIENT are do nothing ops
	}
	return ptrPos, ptrVal, err
}

//
// writeDataValue reads the next data section from `buf` according
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
		ogl.Debugf("DEBUG BOOL: +readDataVal val: %v\n", val) // DEBUG
	case oschema.INTEGER:
		val, err = varint.ReadVarIntAndDecode32(buf)
		ogl.Debugf("DEBUG INT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.SHORT:
		val, err = rw.ReadShort(buf)
		ogl.Debugf("DEBUG SHORT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.LONG:
		val, err = varint.ReadVarIntAndDecode64(buf)
		ogl.Debugf("DEBUG LONG: +readDataVal val: %v\n", val) // DEBUG
	case oschema.FLOAT:
		val, err = rw.ReadFloat(buf)
		ogl.Debugf("DEBUG FLOAT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.DOUBLE:
		val, err = rw.ReadDouble(buf)
		ogl.Debugf("DEBUG DOUBLE: +readDataVal val: %v\n", val) // DEBUG
	case oschema.DATETIME:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue DATETIME NOT YET IMPLEMENTED")
	case oschema.DATE:
		// TODO: impl me
		panic("ORecordSerializerV0#readDataValue DATE NOT YET IMPLEMENTED")
	case oschema.STRING:
		val, err = varint.ReadString(buf)
		ogl.Debugf("DEBUG STR: +readDataVal val: %v\n", val) // DEBUG
	case oschema.BINARY:
		val, err = varint.ReadBytes(buf)
		ogl.Debugf("DEBUG BINARY: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDRECORD:
		doc := oschema.NewDocument("")
		err = serde.Deserialize(doc, buf)
		val = interface{}(doc)
		// ogl.Debugf("DEBUG EMBEDDEDREC: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDLIST:
		val, err = serde.readEmbeddedCollection(buf)
		// ogl.Debugf("DEBUG EMBD-LIST: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDSET:
		val, err = serde.readEmbeddedCollection(buf) // TODO: may need to create a set type as well
		// ogl.Debugf("DEBUG EMBD-SET: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDMAP:
		val, err = serde.readEmbeddedMap(buf)
		// ogl.Debugf("DEBUG EMBD-MAP: +readDataVal val: %v\n", val) // DEBUG
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
		ogl.Debugf("DEBUG BYTE: +readDataVal val: %v\n", val) // DEBUG
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
// writeEmbeddedMap serializes the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
// TODO: this may not need to be a method -> change to fn ?
func (serde *ORecordSerializerV0) writeEmbeddedMap(buf *bytes.Buffer, m odatastructure.OEmbeddedMap) ([]int, []int, error) {
	// number of entries in the map
	err := varint.EncodeAndWriteVarInt32(buf, int32(m.Len()))
	if err != nil {
		return nil, nil, oerror.NewTrace(err)
	}

	dataBuf := new(bytes.Buffer)

	ptrPos := make([]int, 0, m.Len()) // position in buf where data ptr int needs to be written
	ptrVal := make([]int, 0, m.Len()) // the data ptr value to be written in buf
	subPtrPos := make([]int, 0, 4)
	subPtrVal := make([]int, 0, 4)

	// TODO: do the map entries have to be written in any particular order?  I will assume no for now
	keys, vals, types := m.All()
	for i, k := range keys {
		// key type
		err = rw.WriteByte(buf, byte(oschema.STRING))
		if err != nil {
			return ptrPos, ptrVal, oerror.NewTrace(err)
		}

		// write the key value
		err = varint.WriteString(buf, k)
		if err != nil {
			return ptrPos, ptrVal, oerror.NewTrace(err)
		}

		ptrPos = append(ptrPos, buf.Len())
		// wrote placeholder integer for data ptr
		err = rw.WriteInt(buf, 0)
		if err != nil {
			return ptrPos, ptrVal, oerror.NewTrace(err)
		}

		dataType := types[i]
		if dataType == oschema.UNKNOWN {
			dataType = getDataType(vals[i]) // TODO: not sure this is necessary
		}
		// write data type of the data
		err = rw.WriteByte(buf, dataType)
		if err != nil {
			return ptrPos, ptrVal, oerror.NewTrace(err)
		}

		ptrVal = append(ptrVal, dataBuf.Len())

		dbufpos, dbufvals, err := serde.writeDataValue(dataBuf, vals[i], dataType)
		if err != nil {
			return ptrPos, ptrVal, oerror.NewTrace(err)
		}
		if dbufpos != nil {
			subPtrPos = append(subPtrPos, dbufpos...)
			subPtrVal = append(subPtrVal, dbufvals...)
		}
	}

	// position that ends the key headers
	endHdrPos := buf.Len() // this assumes that buf has all the serialized entries including the serializationVersion and className !!

	// fill in placeholder data ptr positions
	for i := range ptrVal {
		ptrVal[i] += endHdrPos
	}

	// adjust the databuf ptr positions and values which are relative to the start of databuf
	for i := range subPtrPos {
		ptrPos = append(ptrPos, subPtrPos[i]+endHdrPos)
		ptrVal = append(ptrVal, subPtrVal[i]+endHdrPos)
	}

	_, err = buf.Write(dataBuf.Bytes()) // TODO: should check return len
	if err != nil {
		return ptrPos, ptrVal, oerror.NewTrace(err)
	}

	bs := buf.Bytes()
	for i, pos := range ptrPos {
		tmpBuf := bytes.NewBuffer(bs[pos : pos+4])
		tmpBuf.Reset() // reset ptr to start of slice so can overwrite the placeholder value
		err = rw.WriteInt(tmpBuf, int32(ptrVal[i]))
		if err != nil {
			return ptrPos, ptrVal, oerror.NewTrace(err)
		}
	}

	return ptrPos, ptrVal, nil
}

func getDataType(val interface{}) byte {
	// TODO: not added:
	// DATETIME
	// DATE
	// LINK
	// LINKLIST
	// LINKMAP
	// LINKSET
	// DECIMAL
	// CUSTOM
	// LINKBAG

	switch val.(type) {
	case byte:
		return oschema.BYTE
	case bool:
		return oschema.BOOLEAN
	case int32:
		return oschema.INTEGER
	case int16:
		return oschema.SHORT
	case int64:
		return oschema.LONG
	case float32:
		return oschema.FLOAT
	case float64:
		return oschema.DOUBLE
	case string:
		return oschema.STRING
	case []byte:
		return oschema.BINARY
	case *oschema.ODocument: // TODO: not sure this is the only way an EMBEDDEDRECORD can be sent ?? what about JSON?
		return oschema.EMBEDDEDRECORD
	case []interface{}: // TODO: this may require some reflection -> how do I detect any type of slice?
		return oschema.EMBEDDEDLIST
	case map[string]struct{}: // TODO: settle on this convention for how to specify an OrientDB SET  ??
		return oschema.EMBEDDEDSET
	// case map[string]interface{}: // TODO: this may require some reflection -> how does the JSON library detect types?
	case *odatastructure.OEmbeddedMap: // TODO: this may require some reflection -> how does the JSON library detect types?
		return oschema.EMBEDDEDMAP
	default:
		return oschema.ANY
	}
}

//
// readEmbeddedMap handles the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
// TODO: change to: func (serde *ORecordSerializerV0) readEmbeddedMap(buf *bytes.Buffer) (*odatastructure.OEmbeddedMap, error) {  ??
func (serde *ORecordSerializerV0) readEmbeddedMap(buf *bytes.Buffer) (map[string]interface{}, error) {
	numRecs, err := varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	nrecs := int(numRecs)
	m := make(map[string]interface{}) // final map to be returned

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
