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
// ORecordSerializerV0 implements the ORecordSerializerBinary
// interface for version 0.
//
// ORecordSerializerV0 is NOT thread safe.  Each DBClient should
// keep its own private serializer object.
//
type ORecordSerializerV0 struct {
	dbc *DBClient // set only temporarility while De/Serializing
}

//
// The serialization version (the first byte of the serialized record) should
// be stripped off (already read) from the io.Reader being passed in.
//
func (serde ORecordSerializerV0) Deserialize(dbc *DBClient, doc *oschema.ODocument, buf *obuf.ByteBuf) (err error) {
	if doc == nil {
		return errors.New("ODocument reference passed into ORecordSerializerBinaryV0.Deserialize was null")
	}

	// temporarily set state for the duration of this Deserialize call
	// dbc is allowed to be nil for reentrant (recursive) calls -- in which
	// case serde.dbc should already be set (not-nil)
	if dbc != nil {
		if serde.dbc != nil {
			return errors.New("Attempted to set dbc again in Serialize when it is already set")
		}
		serde.dbc = dbc
		defer func() {
			serde.dbc = nil
		}()
	} else if serde.dbc == nil {
		return errors.New("dbc *DBClient passed into Deserialize was null and dbc had not already been set in Serializer state")
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

	serde.refreshGlobalPropertiesIfRequired(header)

	for i, prop := range header.properties {
		var ofield *oschema.OField
		if len(prop.name) == 0 {
			globalProp, ok := serde.dbc.GetCurrDB().GlobalProperties[int(prop.id)]
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
			buf.Seek(uint(header.dataPtrs[i] - 1)) // -1 bcs the lead byte (serialization version) was stripped off
			val, err := serde.readDataValue(buf, ofield.Typ)
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

//
// Serialize takes an ODocument and serializes it to bytes in accordance
// with the OrientDB binary serialization spec and writes them to the
// bytes.Buffer passed in.
//
func (serde ORecordSerializerV0) Serialize(dbc *DBClient, doc *oschema.ODocument) ([]byte, error) {
	// temporarily set state for the duration of this Serialize call
	// dbc is allowed to be nil for reentrant (recursive) calls -- in which
	// case serde.dbc should already be set (not-nil)
	if dbc != nil {
		if serde.dbc != nil {
			return nil, errors.New("Attempted to set dbc again in Serialize when it is already set")
		}
		serde.dbc = dbc
		defer func() {
			serde.dbc = nil
		}()
	} else if serde.dbc == nil {
		return nil, errors.New("dbc *DBClient passed into Serialize was null and dbc had not already been set in Serializer state")
	}

	// need to create a new buffer for the serialized record for ptr value calculations,
	// since the incoming buffer (`buf`) already has a lot of stuff written to it (session-id, etc)
	// that are NOT part of the serialized record
	serdebuf := obuf.NewWriteBuffer(80) // holds only the serialized value

	// write the serialization version in so that the buffer size math works
	err := rw.WriteByte(serdebuf, 0)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = serde.serializeDocument(serdebuf, doc)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	return serdebuf.Bytes(), nil
}

//
// serializeDocument writes the classname and the serialized record
// (header and data sections) of the ODocument to the obuf.WriteBuf.
//
// Because this method writes the classname but NOT the serialization
// version, this method is safe for recursive calls for EMBEDDED types.
//
func (serde ORecordSerializerV0) serializeDocument(wbuf *obuf.WriteBuf, doc *oschema.ODocument) error {
	err := varint.WriteString(wbuf, doc.Classname)
	if err != nil {
		return oerror.NewTrace(err)
	}
	ogl.Debugf("serdebuf A: %v\n", wbuf.Bytes()) // DEBUG
	ogl.Debugf("doc A: %v\n", doc)               // DEBUG

	return serde.writeSerializedRecord(wbuf, doc)
}

func (serde ORecordSerializerV0) SerializeClass(doc *oschema.ODocument) ([]byte, error) {
	return nil, nil
}

//
// In Progress attempt to rewrite writeSerializedRecord and related fns
// using a seekable/skipping WriteBuf
//
func (serde ORecordSerializerV0) writeSerializedRecord(wbuf *obuf.WriteBuf, doc *oschema.ODocument) (err error) {
	nfields := len(doc.FieldNames())
	ptrPos := make([]int, 0, nfields) // position in buf where data ptr int needs to be written

	currDB := serde.dbc.GetCurrDB()
	oclass, ok := currDB.Classes[doc.Classname]

	docFields := doc.GetFields()
	for _, fld := range docFields {
		var oprop *oschema.OProperty
		if ok {
			oprop = oclass.Properties[fld.Name]
		}

		// FROM THE JAVA CLIENT:
		// if (properties[i] != null) {
		//   OVarIntSerializer.write(bytes, (properties[i].getId() + 1) * -1);
		//   if (properties[i].getType() != OType.ANY)
		//     pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE);
		//   else
		//     pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);   // TODO: why does ANY required an additional byte?
		// } else {
		//   writeString(bytes, entry.getKey());
		//   pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);

		if oprop != nil {
			// if property is known in the global properties, then
			// just write its encoded id
			varint.EncodeAndWriteVarInt32(wbuf, encodeFieldIdForHeader(oprop.ID))
			ptrPos = append(ptrPos, wbuf.Len())
			wbuf.Skip(4)
			// Note: no need to write property type when writing property ID

		} else {
			// property Name
			err = varint.WriteString(wbuf, fld.Name)
			if err != nil {
				return oerror.NewTrace(err)
			}
			ptrPos = append(ptrPos, wbuf.Len())
			wbuf.Skip(4)

			// property Type
			err = rw.WriteByte(wbuf, fld.Typ)
			if err != nil {
				return oerror.NewTrace(err)
			}
		}
	}
	wbuf.WriteByte(0) // End of Header sentinel

	// now write out the data values
	for i, fld := range docFields {
		currPos := wbuf.Len()
		wbuf.Seek(uint(ptrPos[i]))
		err = rw.WriteInt(wbuf, int32(currPos))
		if err != nil {
			return oerror.NewTrace(err)
		}
		wbuf.Seek(uint(currPos))
		err = serde.writeDataValue(wbuf, fld.Value, fld.Typ)
		if err != nil {
			return oerror.NewTrace(err)
		}
	}

	return nil
}

//
// writeDataValue is part of the Serialize functionality
// TODO: change name to writeSingleValue ?
//
func (serde ORecordSerializerV0) writeDataValue(buf *obuf.WriteBuf, value interface{}, datatype byte) (err error) {
	switch datatype {
	case oschema.STRING:
		err = varint.WriteString(buf, value.(string))
		ogl.Debugf("DEBUG STR: -writeDataVal val: %v\n", value.(string)) // DEBUG

	case oschema.BOOLEAN:
		err = rw.WriteBool(buf, value.(bool))
		ogl.Debugf("DEBUG BOOL: -writeDataVal val: %v\n", value.(bool)) // DEBUG

	case oschema.INTEGER:
		var i32val int32
		i32val, err = toInt32(value)
		if err == nil {
			err = varint.EncodeAndWriteVarInt32(buf, i32val)         // TODO: are serialized integers ALWAYS varint encoded?
			ogl.Debugf("DEBUG INT: -writeDataVal val: %v\n", i32val) // DEBUG
		}

	case oschema.SHORT:
		// TODO: needs toInt16 conversion fn
		err = varint.EncodeAndWriteVarInt32(buf, int32(value.(int16)))
		ogl.Debugf("DEBUG SHORT: -writeDataVal val: %v\n", value.(int16)) // DEBUG

	case oschema.LONG:
		var i64val int64
		i64val, err = toInt64(value)
		if err == nil {
			err = varint.EncodeAndWriteVarInt64(buf, i64val)          // TODO: are serialized longs ALWAYS varint encoded?
			ogl.Debugf("DEBUG LONG: -writeDataVal val: %v\n", i64val) // DEBUG
		}

	case oschema.FLOAT:
		var f32 float32
		f32, err = toFloat32(value)
		if err == nil {
			err = rw.WriteFloat(buf, f32)
		}
		ogl.Debugf("DEBUG FLOAT: -writeDataVal val: %v\n", value) // DEBUG

	case oschema.DOUBLE:
		// TODO: needs toInt64 conversion fn
		err = rw.WriteDouble(buf, value.(float64))
		ogl.Debugf("DEBUG DOUBLE: -writeDataVal val: %v\n", value.(float64)) // DEBUG

	case oschema.DATETIME:
		err = writeDateTime(buf, value)
		ogl.Debugf("DEBUG DATETIME: -writeDataVal val: %v\n", value) // DEBUG

	case oschema.DATE:
		err = writeDate(buf, value)
		ogl.Debugf("DEBUG DATE: -writeDataVal val: %v\n", value) // DEBUG

	case oschema.BINARY:
		err = varint.WriteBytes(buf, value.([]byte))
		ogl.Debugf("DEBUG BINARY: -writeDataVal val: %v\n", value.([]byte)) // DEBUG

	case oschema.EMBEDDED:
		err = serde.serializeDocument(buf, value.(*oschema.ODocument))
		ogl.Debugf("DEBUG EMBEDDED: -writeDataVal val: %v\n", value) // DEBUG

	case oschema.EMBEDDEDLIST:
		err = serde.serializeEmbeddedCollection(buf, value.(oschema.OEmbeddedList))
		ogl.Debugf("DEBUG EMBD-LIST: -writeDataVal val: %v\n", value) // DEBUG

	case oschema.EMBEDDEDSET:
		err = serde.serializeEmbeddedCollection(buf, value.(oschema.OEmbeddedList))
		ogl.Debugf("DEBUG EMBD-SET: -writeDataVal val: %v\n", value) // DEBUG

	case oschema.EMBEDDEDMAP:
		err = serde.writeEmbeddedMap(buf, value.(oschema.OEmbeddedMap))
		ogl.Debugf("DEBUG EMBEDDEDMAP:  val %v\n", value.(oschema.OEmbeddedMap))

	case oschema.LINK:
		err = serde.writeLink(buf, value.(*oschema.OLink))
		ogl.Debugf("DEBUG EMBEDDEDMAP:  val %v\n", value) // DEBUG

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
	return err
}

//
// +-----------------+-----------------+
// |clusterId:varint | recordId:varInt |
// +-----------------+-----------------+
//
func (serde ORecordSerializerV0) writeLink(buf *obuf.WriteBuf, lnk *oschema.OLink) error {
	err := varint.EncodeAndWriteVarInt32(buf, int32(lnk.RID.ClusterID))
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = varint.EncodeAndWriteVarInt64(buf, lnk.RID.ClusterPos)
	if err != nil {
		return oerror.NewTrace(err)
	}
	return nil
}

//
// +-------------+-------------------+
// | size:varint | collection:LINK[] |
// +-------------+-------------------+
//
func (serde ORecordSerializerV0) writeLinkList(buf *obuf.WriteBuf, lnk *oschema.OLink) error {
	// TODO: impl me
	return nil
}

//
// writeEmbeddedMap serializes the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
// TODO: this may not need to be a method -> change to fn ?
func (serde ORecordSerializerV0) writeEmbeddedMap(buf *obuf.WriteBuf, m oschema.OEmbeddedMap) error {
	// number of entries in the map
	err := varint.EncodeAndWriteVarInt32(buf, int32(m.Len()))
	if err != nil {
		return oerror.NewTrace(err)
	}

	ptrPos := make([]int, 0, m.Len()) // position in buf where data ptr int needs to be written

	// TODO: do the map entries have to be written in any particular order?  I will assume no for now
	keys, vals, types := m.All()

	/* ---[ write embedded map header ]--- */
	for i, k := range keys {
		// key type
		err = rw.WriteByte(buf, byte(oschema.STRING))
		if err != nil {
			return oerror.NewTrace(err)
		}

		// write the key value
		err = varint.WriteString(buf, k)
		if err != nil {
			return oerror.NewTrace(err)
		}

		ptrPos = append(ptrPos, buf.Len())
		buf.Skip(4) // placeholder integer for data ptr

		dataType := types[i]
		if dataType == oschema.UNKNOWN {
			dataType = getDataType(vals[i]) // TODO: not sure this is necessary
		}
		// write data type of the data
		err = rw.WriteByte(buf, dataType)
		if err != nil {
			return oerror.NewTrace(err)
		}
	}

	/* ---[ write embedded map data values ]--- */
	for i := 0; i < len(vals); i++ {
		currPos := buf.Len()
		buf.Seek(uint(ptrPos[i]))
		err = rw.WriteInt(buf, int32(currPos))
		if err != nil {
			return oerror.NewTrace(err)
		}
		buf.Seek(uint(currPos))
		err = serde.writeDataValue(buf, vals[i], types[i])
		if err != nil {
			return oerror.NewTrace(err)
		}
	}

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
// readDataValue reads the next data section from `buf` according
// to the type of the property (property.Typ) and updates the OField object
// to have the value.
//
func (serde ORecordSerializerV0) readDataValue(buf *obuf.ByteBuf, datatype byte) (interface{}, error) {
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
		var i64 int64
		i64, err = varint.ReadVarIntAndDecode64(buf)
		if err == nil {
			val = int32(i64)
		}
		ogl.Debugf("DEBUG INT: +readDataVal val: %v\n", val) // DEBUG
	case oschema.SHORT:
		var i32 int32
		i32, err = varint.ReadVarIntAndDecode32(buf)
		if err == nil {
			val = int16(i32)
		}
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
	case oschema.EMBEDDED:
		doc := oschema.NewDocument("")
		err = serde.Deserialize(nil, doc, buf)
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
// writeDateTime takes an interface{} value that must be of type:
//  - time.Time
//  - int or int64, representing milliseconds since Epoch
//
// NOTE: format for OrientDB DATETIME:
//   Golang formatted date: 2006-01-02 03:04:05
//   Example: 2014-11-25 09:14:54
//
// OrientDB server converts a DATETIME type to millisecond unix epoch and
// stores it as the type LONG.  It is written as a varint long to the
// obuf.WriteBuf passed in.
//
func writeDateTime(buf *obuf.WriteBuf, value interface{}) error {
	var millisEpoch int64

	switch value.(type) {
	case int:
		millisEpoch = int64(value.(int))

	case int64:
		millisEpoch = value.(int64)

	case time.Time:
		// UnixNano returns t as a Unix time, the number of nanoseconds elapsed
		// since January 1, 1970 UTC.
		tm := value.(time.Time)
		tt := tm.Round(time.Millisecond)
		millisEpoch = tt.UnixNano() / (1000 * 1000)

	default:
		return oerror.ErrDataTypeMismatch{
			ExpectedDataType: oschema.DATETIME,
			ExpectedGoType:   "time.Time | int64 | int",
			ActualValue:      value,
		}
	}
	err := varint.EncodeAndWriteVarInt64(buf, millisEpoch)
	if err != nil {
		return oerror.NewTrace(err)
	}
	return nil
}

//
// readDateTime reads an OrientDB DATETIME from the stream and converts it to
// a golang time.Time struct. DATETIME is precise to the second.
// The time zone of the time.Time returned should be the Local timezone.
//
// OrientDB server converts a DATETIME type to millisecond unix epoch and
// stores it as the type LONG.  It is written as a varint long.
//
func (serde ORecordSerializerV0) readDateTime(r io.Reader) (time.Time, error) {
	dtAsLong, err := varint.ReadVarIntAndDecode64(r)
	if err != nil {
		return time.Unix(0, 0), oerror.NewTrace(err)
	}
	dtSecs := dtAsLong / 1000
	dtNanos := (dtAsLong % 1000) * 1000000
	return time.Unix(dtSecs, dtNanos), nil
}

//
// writeDateTime takes an interface{} value that must be of type time.Time
//
// NOTE: format for OrientDB DATETIME:
//   Golang formatted date: 2006-01-02 03:04:05
//   Example: 2014-11-25 09:14:54
//
// OrientDB server converts a DATETIME type to millisecond unix epoch and
// stores it as the type LONG.  It is written as a varint long to the
// obuf.WriteBuf passed in.
//
// From the OrientDB schemaless serialization spec on DATE:
//     The date is converted to second unix epoch,moved at midnight UTC+0,
//     divided by 86400(seconds in a day) and stored as the type LONG
//
func writeDate(buf *obuf.WriteBuf, value interface{}) error {
	tm, ok := value.(time.Time)
	if !ok {
		return oerror.ErrDataTypeMismatch{
			ExpectedDataType: oschema.DATE,
			ExpectedGoType:   "time.Time",
			ActualValue:      value,
		}
	}

	tmMidnightUTC := time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.FixedZone("UTC", 0))
	secondsEpoch := tmMidnightUTC.Unix()
	dateAfterDiv := secondsEpoch / int64(86400)

	err := varint.EncodeAndWriteVarInt64(buf, dateAfterDiv)
	if err != nil {
		return oerror.NewTrace(err)
	}
	return nil
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
func (serde ORecordSerializerV0) readDate(r io.Reader) (time.Time, error) {
	seconds, err := varint.ReadVarIntAndDecode64(r)
	if err != nil {
		return time.Unix(0, 0), oerror.NewTrace(err)
	}

	dateAsLong := seconds * int64(86400)    // multiply the 86,400 seconds back
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
	case *oschema.ODocument: // TODO: not sure this is the only way an EMBEDDED can be sent ?? what about JSON?
		return oschema.EMBEDDED
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
func (serde ORecordSerializerV0) readEmbeddedMap(buf *obuf.ByteBuf) (map[string]interface{}, error) {
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
// Serialization format for EMBEDDEDLIST and EMBEDDEDSET
// +-------------+------------+-------------------+
// |size:varInt  | type:Otype | items:item_data[] |
// +-------------+------------+-------------------+
//
// The item_data data structure is:
// +------------------+--------------+
// | data_type:OType  | data:byte[]  |
// +------------------+--------------+
//
func (serde ORecordSerializerV0) serializeEmbeddedCollection(buf *obuf.WriteBuf, ls oschema.OEmbeddedList) error {
	err := varint.EncodeAndWriteVarInt32(buf, int32(ls.Len()))
	if err != nil {
		return oerror.NewTrace(err)
	}

	// following the lead of the Java driver, you don't specify the type of the list overall
	// (I tried to and it doesn't work, at least with OrientDB-2.0.1)
	err = rw.WriteByte(buf, oschema.ANY)
	if err != nil {
		return oerror.NewTrace(err)
	}

	for _, val := range ls.Values() {
		buf.WriteByte(ls.Type())
		err = serde.writeDataValue(buf, val, ls.Type())
		if err != nil {
			return oerror.NewTrace(err)
		}
	}

	return nil
}

//
// readEmbeddedCollection handles both EMBEDDEDLIST and EMBEDDEDSET types.
// Java client API:
//     Collection<?> readEmbeddedCollection(BytesContainer bytes, Collection<Object> found, ODocument document) {
//     `found`` gets added to during the recursive iterations
//
func (serde ORecordSerializerV0) readEmbeddedCollection(buf *obuf.ByteBuf) ([]interface{}, error) {
	nrecs, err := varint.ReadVarIntAndDecode32(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	datatype, err := rw.ReadByte(buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if datatype != oschema.ANY { // OrientDB server always returns ANY
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

func toFloat32(value interface{}) (float32, error) {
	switch value.(type) {
	case float32:
		return value.(float32), nil
	case float64:
		return float32(value.(float64)), nil
	case int:
		return float32(value.(int)), nil
	case int32:
		return float32(value.(int32)), nil
	case int64:
		return float32(value.(int64)), nil
	default:
		return 0, oerror.ErrDataTypeMismatch{ExpectedDataType: oschema.FLOAT, ActualValue: value}
	}
}

func toInt32(value interface{}) (int32, error) {
	v1, ok := value.(int32)
	if ok {
		return v1, nil
	}
	v2, ok := value.(int)
	if ok {
		return int32(v2), nil
	}

	return int32(-1), oerror.ErrDataTypeMismatch{ExpectedDataType: oschema.INTEGER, ActualValue: value}
}

func toInt64(value interface{}) (int64, error) {
	v1, ok := value.(int64)
	if ok {
		return v1, nil
	}
	v2, ok := value.(int)
	if ok {
		return int64(v2), nil
	}

	return int64(-1), oerror.ErrDataTypeMismatch{ExpectedDataType: oschema.LONG, ActualValue: value}
}

func encodeFieldIdForHeader(id int32) int32 {
	return (id + 1) * -1
}

func decodeFieldIdInHeader(decoded int32) int32 {
	return (decoded * -1) - 1
}

//
// refreshGlobalPropertiesIfRequired iterates through all the fields
// of the binserde header. If any of the fieldIds are NOT in the GlobalProperties
// map of the current ODatabase object, then the GlobalProperties are
// stale and need to be refreshed (this likely means CREATE PROPERTY statements
// were recently issued).
//
// If the GlobalProperties data is stale, then it must be refreshed, so
// refreshGlobalProperties is called.
//
func (serde ORecordSerializerV0) refreshGlobalPropertiesIfRequired(hdr header) error {
	if serde.dbc.GetCurrDB() == nil {
		return nil
	}
	if serde.dbc.GetCurrDB().GlobalProperties == nil {
		return nil
	}
	for _, prop := range hdr.properties {
		if prop.name == nil {
			_, ok := serde.dbc.GetCurrDB().GlobalProperties[int(prop.id)]
			if !ok {
				return refreshGlobalProperties(serde.dbc)
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
// are then copied from the new DBClient.currDB to the old one,
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
	err = OpenDatabase(dbctmp, dbc.GetCurrDB().Name, constants.DocumentDB, "admin", "admin")
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v\n", err)
		return err
	}

	dbc.currDB = dbctmp.currDB
	return nil
}
