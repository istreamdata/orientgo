package obinary

//
// binserde stands for binary Serializer/Deserializer.
// This file holds the interface implementations for SerDes for the
// OrientDB Network Binary Protocol.
//

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary/binserde/varint"
	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/obuf"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
)

//
// ORecordSerializerBinaryV0 implements the ORecordSerializerBinary
// interface for version 0
//
type ORecordSerializerV0 struct{}

//
// The serialization version (the first byte of the serialized record) should
// be stripped off (already read) from the io.Reader being passed in.
//
func (serde ORecordSerializerV0) Deserialize(dbc *DBClient, doc *oschema.ODocument, buf io.Reader) (err error) {
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

	refreshGlobalPropertiesIfRequired(dbc, header)

	for i, prop := range header.properties {
		var ofield *oschema.OField
		if len(prop.name) == 0 {
			globalProp, ok := dbc.GetCurrDB().GlobalProperties[int(prop.id)]
			if !ok {
				panic(oerror.ErrStaleGlobalProperties) // TODO: should return this instead
			}
			ofield = &oschema.OField{
				Id:   prop.id,
				Name: globalProp.Name,
				Typ:  globalProp.Type,
			}

		} else {
			ofield = &oschema.OField{
				Id:   int32(-1),
				Name: string(prop.name),
				Typ:  prop.typ,
			}
		}
		// if data ptr is 0 (NULL), then it has no entry/value in the serialized record
		if header.dataPtrs[i] != 0 {
			val, err := serde.readDataValue(dbc, buf, ofield.Typ)
			if err != nil {
				return err
			}
			ofield.Value = val
		}

		doc.AddField(ofield.Name, ofield)
	}
	doc.SetDirty(false)

	return nil
}

//
// TODO: need to study what exactly this method is supposed to do and not do
//       -> check the Java driver version
//
// IDEA: maybe this could be DeserializeField?  Might be useful for RidBags. Anything else?
//
//
func (serde ORecordSerializerV0) DeserializePartial(doc *oschema.ODocument, buf io.Reader, fields []string) error {
	// TODO: impl me
	return nil
}

func (serde ORecordSerializerV0) Serialize(doc *oschema.ODocument, buf *bytes.Buffer) (err error) {
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

func (serde ORecordSerializerV0) writeSerializedRecord(buf *bytes.Buffer, doc *oschema.ODocument) (err error) {
	nfields := len(doc.FieldNames())
	ptrPos := make([]int, 0, nfields) // position in buf where data ptr int needs to be written
	ptrVal := make([]int, 0, nfields) // data ptr value to write into buf
	subPtrPos := make([]int, 0, 8)
	subPtrVal := make([]int, 0, 8)

	dataBuf := new(bytes.Buffer)

	if doc.Classname == "" {
		// serializing a property or SQL params map -> use propertyName
		for _, fldName := range doc.FieldNames() {
			fld := doc.GetField(fldName)
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

// func (serde ORecordSerializerV0) writeHeader(doc *oschema.ODocument, buf *bytes.Buffer) (ptrPos []int, err error) {
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

func (serde ORecordSerializerV0) SerializeClass(doc *oschema.ODocument, buf *bytes.Buffer) error {
	return nil
}

//
// Either id or name+typ will be filled in but not both.
// So in C this would be a union, not a struct.
//
type headerProperty struct {
	id   int32
	name []byte
	typ  byte
}

//
// header in the schemaless serialization format.
//
type header struct {
	properties []headerProperty
	dataPtrs   []int32
}

/* ---[ helper fns ]--- */

func readClassname(buf io.Reader) (string, error) {
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

	cnameBytes = make([]byte, int(cnameLen))
	n, err := buf.Read(cnameBytes)
	if err != nil {
		return "", oerror.NewTrace(err)
	}
	if n != int(cnameLen) {
		return "",
			fmt.Errorf("Could not read expected number of bytes for className. Expected %d; Read: %d",
				cnameLen, len(cnameBytes))
	}

	return string(cnameBytes), nil
}

func readHeader(buf io.Reader) (header, error) {
	hdr := header{
		properties: make([]headerProperty, 0, 8),
		dataPtrs:   make([]int32, 0, 8),
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
			// have a property, not a document, so the number is a zigzag encoded length
			// for a string (property name)

			// read property name
			data := make([]byte, int(decoded))
			n, err := buf.Read(data)
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}
			if len(data) != n {
				return header{}, oerror.IncorrectNetworkRead{Expected: len(data), Actual: n}
			}
			// hdr.propertyNames = append(hdr.propertyNames, string(data))

			// read data pointer
			ptr, err := rw.ReadInt(buf)
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}

			// read data type
			bsDataType := make([]byte, 1)
			n, err = buf.Read(bsDataType)
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}
			if n != 1 {
				return header{}, oerror.IncorrectNetworkRead{Expected: 1, Actual: n}
			}

			hdrProp := headerProperty{name: data, typ: bsDataType[0]}
			hdr.properties = append(hdr.properties, hdrProp)
			hdr.dataPtrs = append(hdr.dataPtrs, ptr)

		} else {
			// have a document, not a property, so the number is an encoded property id,
			// convert to (positive) property-id
			propertyId := decodeFieldIdInHeader(decoded)

			ptr, err := rw.ReadInt(buf)
			if err != nil {
				return header{}, oerror.NewTrace(err)
			}

			hdrProp := headerProperty{id: propertyId}
			hdr.properties = append(hdr.properties, hdrProp)
			hdr.dataPtrs = append(hdr.dataPtrs, ptr)
		}
	}
	return hdr, nil
}

//
// writeDataValue is part of the Serialize functionality
// TODO: change name to writeSingleValue ?
//
func (serde ORecordSerializerV0) writeDataValue(buf *bytes.Buffer, value interface{}, datatype byte) (ptrPos, ptrVal []int, err error) {
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
		ptrPos, ptrVal, err = serde.writeEmbeddedMap(buf, value.(oschema.OEmbeddedMap))
		ogl.Debugf("DEBUG EMBEDDEDMAP:  val %v\n", value.(oschema.OEmbeddedMap))
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
// readDataValue reads the next data section from `buf` according
// to the type of the property (property.Typ) and updates the OField object
// to have the value.
//
func (serde ORecordSerializerV0) readDataValue(dbc *DBClient, buf io.Reader, datatype byte) (interface{}, error) {
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
		// OrientDB DATETIME is precise to the second
		val, err = serde.readDateTime(buf)
		ogl.Debugf("DEBUG DATEIME: +readDataVal val: %v\n", val) // DEBUG
	case oschema.DATE:
		// OrientDB DATE is precise to the day
		val, err = serde.readDate(buf)
		ogl.Debugf("DEBUG DATE: +readDataVal val: %v\n", val) // DEBUG
	case oschema.STRING:
		val, err = varint.ReadString(buf)
		ogl.Debugf("DEBUG STR: +readDataVal val: %v\n", val) // DEBUG
	case oschema.BINARY:
		val, err = varint.ReadBytes(buf)
		ogl.Debugf("DEBUG BINARY: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDRECORD:
		doc := oschema.NewDocument("")
		err = serde.Deserialize(dbc, doc, buf)
		val = interface{}(doc)
		// ogl.Debugf("DEBUG EMBEDDEDREC: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDLIST:
		val, err = serde.readEmbeddedCollection(dbc, buf)
		// ogl.Debugf("DEBUG EMBD-LIST: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDSET:
		val, err = serde.readEmbeddedCollection(dbc, buf) // TODO: may need to create a set type as well
		// ogl.Debugf("DEBUG EMBD-SET: +readDataVal val: %v\n", val) // DEBUG
	case oschema.EMBEDDEDMAP:
		val, err = serde.readEmbeddedMap(dbc, buf)
		// ogl.Debugf("DEBUG EMBD-MAP: +readDataVal val: %v\n", val) // DEBUG
	case oschema.LINK:
		// a link is two int64's (cluster:record) - we translate it here to a string RID
		val, err = serde.readLink(buf)
		ogl.Debugf("DEBUG LINK: +readDataVal val: %v\n", val) // DEBUG
	case oschema.LINKLIST, oschema.LINKSET:
		val, err = serde.readLinkList(buf)
		ogl.Debugf("DEBUG LINK LIST/SET: +readDataVal val: %v\n", val) // DEBUG
	case oschema.LINKMAP:
		val, err = serde.readLinkMap(buf)
		ogl.Debugf("DEBUG LINKMap: +readDataVal val: %v\n", val) // DEBUG
	case oschema.BYTE:
		val, err = rw.ReadByte(buf)
		ogl.Debugf("DEBUG BYTE: +readDataVal val: %v\n", val) // DEBUG
	case oschema.CUSTOM:
		// TODO: impl me -> how? when is this used?
		panic("ORecordSerializerV0#readDataValue CUSTOM NOT YET IMPLEMENTED")
	case oschema.DECIMAL:
		// TODO: impl me -> Java client uses BigDecimal for this
		panic("ORecordSerializerV0#readDataValue DECIMAL NOT YET IMPLEMENTED")
	case oschema.LINKBAG:
		val, err = serde.readLinkBag(buf)
		ogl.Debugf("DEBUG LINKBAG: +readDataVal val: %v\n", val) // DEBUG
	default:
		// ANY and TRANSIENT are do nothing ops
	}

	return val, err
}

//
// readDateTime reads an OrientDB DATETIME from the stream and converts it to
// a golang time.Time struct. DATETIME is precise to the second.
// The time zone of the time.Time returned should be the Local timezone.
//
// OrientDB server converts a DATETIME type is to millisecond unix epoch and
// stores it as the type LONG.  It is written as a varint long.
//
func (serde ORecordSerializerV0) readDateTime(buf io.Reader) (time.Time, error) {
	dtAsLong, err := varint.ReadVarIntAndDecode64(buf)
	if err != nil {
		return time.Unix(0, 0), oerror.NewTrace(err)
	}
	dtSecs := dtAsLong / 1000
	dtMillis := dtAsLong % 1000
	return time.Unix(dtSecs, dtMillis), nil
}

//
// readDate reads an OrientDB DATE from the stream and converts it to
// a golang time.Time struct. DATE is precise to the day - hour, minute
// and second are zeroed out.  The time zone of the time.Time returned
// should be the Local timezone.
//
// OrientDB server returns DATEs as (varint) longs.
// From the OrientDB schemaless serialization spec on DATE:
//     The date is converted to second unix epoch,moved at midnight UTC+0,
//     divided by 86400(seconds in a day) and stored as the type LONG
//
func (serde ORecordSerializerV0) readDate(buf io.Reader) (time.Time, error) {
	seconds, err := varint.ReadVarIntAndDecode64(buf)
	if err != nil {
		return time.Unix(0, 0), oerror.NewTrace(err)
	}

	dateAsLong := seconds * int64(86400)    // multiple the 86,400 seconds back
	utctm := time.Unix(dateAsLong, 0).UTC() // OrientDB returns it as a UTC date, so start with that
	loctm := utctm.Local()                  // convert to local time
	_, offsetInSecs := loctm.Zone()         // the compute the time zone difference
	offsetInNanos := offsetInSecs * 1000 * 1000 * 1000
	durOffset := time.Duration(offsetInNanos)
	adjustedLocTm := loctm.Add(-durOffset) // and finally adjust the time back to local time

	return adjustedLocTm, nil
}

//
// Returns map of string keys to *oschema.OLink
//
func (serde ORecordSerializerV0) readLinkMap(buf io.Reader) (map[string]*oschema.OLink, error) {
	nentries, err := varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	linkMap := make(map[string]*oschema.OLink)

	for i := 0; i < int(nentries); i++ {
		/* ---[ read map key ]--- */
		datatype, err := rw.ReadByte(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if datatype != byte(7) {
			// FIXME: even though all keys are currently strings, it would be easy to allow other types
			//        using serde.readDataValue(dbc, buf, serde)
			return nil, fmt.Errorf("readLinkMap: datatype for key is NOT string but type: %v", datatype)
		}

		mapkey, err := varint.ReadString(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		/* ---[ read map value (always a RID) ]--- */
		mapval, err := serde.readLink(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		linkMap[mapkey] = mapval
	}

	return linkMap, nil
}

// TODO: remove me
func pause(msg string) {
	fmt.Print(msg, "[Press Enter to Continue]: ")
	var s string
	_, err := fmt.Scan(&s)
	if err != nil {
		panic(err)
	}
}

//
// readLinkBag handles both Embedded and remote Tree-based OLinkBags.
//
func (serde ORecordSerializerV0) readLinkBag(buf io.Reader) (*oschema.OLinkBag, error) {
	bagType, err := rw.ReadByte(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	if bagType == byte(0) {
		return readTreeBasedLinkBag(buf)
	}
	return readEmbeddedLinkBag(buf)
}

func readEmbeddedLinkBag(rdr io.Reader) (*oschema.OLinkBag, error) {
	bs := make([]byte, 1)
	n, err := rdr.Read(bs)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if n != 1 {
		return nil, oerror.IncorrectNetworkRead{Expected: 1, Actual: n}
	}

	if bs[0] == 1 {
		uuid, err := readLinkBagUUID(rdr)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		ogl.Debugf("read uuid %v - now what?\n", uuid)

	} else {
		// if b wasn't zero, then there's no UUID and b was the first byte of an int32
		// specifying the size of the embedded bag collection
		// TODO: I'm not sure this is the right thing - the OrientDB is pretty hazy on how this works
		switch rdr.(type) {
		case *bytes.Buffer:
			buf := rdr.(*bytes.Buffer)
			buf.UnreadByte()

		case *obuf.ByteBuf:
			buf := rdr.(*obuf.ByteBuf)
			buf.UnreadByte()

		default:
			panic("Unknown type of buffer in binserde#readEmbeddedLinkBag")
		}
	}

	bagsz, err := rw.ReadInt(rdr)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	links := make([]*oschema.OLink, bagsz)

	for i := int32(0); i < bagsz; i++ {
		clusterID, err := rw.ReadShort(rdr)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		clusterPos, err := rw.ReadLong(rdr)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		orid := oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}
		links[i] = &oschema.OLink{RID: orid}
	}

	return oschema.NewOLinkBag(links), nil
}

func readLinkBagUUID(buf io.Reader) (int32, error) {
	// TODO: I don't know what form the UUID is - an int32?  How is it serialized?
	panic("This LINKBAG has a UUID; support for UUIDs has not yet been added")
}

//
// Example data section for tree-based LinkBag
//
//                ( --------------------- collectionPointer ----------------------- )  (---size:int--)  (-changes-)
//                (----- fileId:long ----)  ( ---pageIndex:long--- ) (pageOffset:int)
//     TREEBASED             30                         0                 2048                -1             0
//         0,      0, 0, 0, 0, 0, 0, 0, 30,   0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 8, 0,     -1, -1, -1, -1,  0, 0, 0, 0,
//
func readTreeBasedLinkBag(buf io.Reader) (*oschema.OLinkBag, error) {
	fileId, err := rw.ReadLong(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	pageIdx, err := rw.ReadLong(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	pageOffset, err := rw.ReadInt(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// TODO: need to know how to handle the size and changes stuff => advanced feature not needed yet
	size, err := rw.ReadInt(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	_, err = rw.ReadInt(buf) // changes // TODO: is changes always an int32?
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	return oschema.NewTreeOLinkBag(fileId, pageIdx, pageOffset, size), nil
}

func (serde ORecordSerializerV0) readLinkList(buf io.Reader) ([]*oschema.OLink, error) {
	nrecs, err := varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	links := make([]*oschema.OLink, int(nrecs))
	for i := range links {
		lnk, err := serde.readLink(buf)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		links[i] = lnk
	}

	return links, nil
}

//
// readLink reads a two int64's - the cluster and record.
// We translate it here to a string RID (cluster:record) and return it.
//
func (serde ORecordSerializerV0) readLink(buf io.Reader) (*oschema.OLink, error) {
	clusterId, err := varint.ReadVarIntAndDecode64(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	clusterPos, err := varint.ReadVarIntAndDecode64(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	orid := oschema.ORID{ClusterID: int16(clusterId), ClusterPos: clusterPos}
	return &oschema.OLink{RID: orid}, nil
}

//
// writeEmbeddedMap serializes the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
// TODO: this may not need to be a method -> change to fn ?
func (serde ORecordSerializerV0) writeEmbeddedMap(buf *bytes.Buffer, m oschema.OEmbeddedMap) ([]int, []int, error) {
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
	case *oschema.OEmbeddedMap: // TODO: this may require some reflection -> how does the JSON library detect types?
		return oschema.EMBEDDEDMAP
	default:
		return oschema.ANY
	}
}

//
// readEmbeddedMap handles the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
// TODO: change return type to (*oschema.OEmbeddedMap, error) {  ???
func (serde ORecordSerializerV0) readEmbeddedMap(dbc *DBClient, buf io.Reader) (map[string]interface{}, error) {
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
		val, err := serde.readDataValue(dbc, buf, valtypes[i])
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
func (serde ORecordSerializerV0) readEmbeddedCollection(dbc *DBClient, buf io.Reader) ([]interface{}, error) {
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

		val, err := serde.readDataValue(dbc, buf, itemtype)
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

//
// refreshGlobalPropertiesIfRequired iterates through all the fields
// of the binserde header. If any of the fieldIds are NOT in the GlobalProperties
// map of the current ODatabase object, then the GlobalProperties are
// stale and need to be refresh (this likely means CREATE PROPERTY statements
// were recently issued).
//
// If the GlobalProperties data is stale, then it must be refreshed, so
// refreshGlobalProperties is called.
//
func refreshGlobalPropertiesIfRequired(dbc *DBClient, hdr header) error {
	if dbc.GetCurrDB() == nil {
		return nil
	}
	if dbc.GetCurrDB().GlobalProperties == nil {
		return nil
	}
	for _, prop := range hdr.properties {
		if prop.name == nil {
			_, ok := dbc.GetCurrDB().GlobalProperties[int(prop.id)]
			if !ok {
				return refreshGlobalProperties(dbc)
			}
		}
	}
	return nil
}

//
// refreshGlobalProperties is called when it is discovered,
// *while in the middle* of reading the response from the OrientDB
// server, that the GlobalProperties are stale.  The solution
// chosen to get around this is to open a new client connection
// via OpenDatabase, which will automatically read in the
// current state of the GlobalProperties.  The GlobalProperties
// are then copied from the new DBClient.currDb to the old one,
// and the new connection (DBClient) is closed.  This allows
// the code that called this to resume reading from its data
// stream where it left off.
//
func refreshGlobalProperties(dbc *DBClient) error {
	dbctmp, err := NewDBClient(ClientOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return err
	}
	defer dbctmp.Close()
	err = OpenDatabase(dbctmp, dbc.GetCurrDB().Name, constants.DocumentDb, "admin", "admin")
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return err
	}

	dbc.currDb = dbctmp.currDb
	return nil
}
