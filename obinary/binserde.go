package obinary

// binserde stands for binary Serializer/Deserializer.
// This file holds the interface implementations for SerDes for the
// OrientDB Network Binary Protocol.

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	"github.com/istreamdata/orientgo/obinary/binserde/varint"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

type embeddedRecordFunc func(buf *bytes.Reader) (interface{}, error)

// ORecordSerializerV0 implements the ORecordSerializerBinary
// interface for version 0.
type ORecordSerializerV0 struct{}

func (serde ORecordSerializerV0) Version() byte {
	return 0
}

func (serde ORecordSerializerV0) Class() string {
	return serializeTypeBinary
}

// The serialization version (the first byte of the serialized record) should
// be stripped off (already read) from the io.Reader being passed in.
func (serde ORecordSerializerV0) Deserialize(db *Database, doc *oschema.ODocument, buf *bytes.Reader) (err error) {
	defer catch(&err)
	if doc == nil {
		return fmt.Errorf("Empty ODocument")
	}

	classname := readClassname(buf)
	doc.Classname = classname

	ofields, err := serde.deserializeFields(db, buf, func(buf *bytes.Reader) (interface{}, error) {
		doc := oschema.NewDocument("")
		err := serde.Deserialize(db, doc, buf)
		val := interface{}(doc)
		return val, err
	})
	if err != nil {
		return err
	}

	for _, ofield := range ofields {
		doc.AddField(ofield.Name, ofield)
	}
	doc.SetDirty(false)

	return nil
}

func (serde ORecordSerializerV0) deserializeFields(db *Database, buf *bytes.Reader, eFunc embeddedRecordFunc) (ofields []*oschema.OField, err error) {
	header, err := readHeader(buf)
	if err != nil {
		return nil, err
	}

	if err := db.refreshGlobalPropertiesIfRequired(header); err != nil {
		return nil, err
	}

	for _, prop := range header {
		var ofield *oschema.OField
		if prop.name == "" {
			if db == nil {
				return nil, ErrStaleGlobalProperties
			}
			globalProp, ok := db.db.GetGlobalProperty(int(prop.id))
			if !ok {
				return nil, ErrStaleGlobalProperties
			}
			ofield = &oschema.OField{
				Id:   prop.id,
				Name: globalProp.Name,
				Type: globalProp.Type,
			}

		} else {
			ofield = &oschema.OField{
				Id:   int32(-1),
				Name: string(prop.name),
				Type: oschema.OType(prop.typ),
			}
		}
		// if data ptr is 0 (NULL), then it has no entry/value in the serialized record
		if prop.ptr != 0 {
			_, err := buf.Seek(int64(prop.ptr-1), 0) // -1 bcs the lead byte (serialization version) was stripped off
			if err != nil {
				return nil, err
			}
			val, err := serde.readDataValue(buf, ofield.Type, eFunc)
			if err != nil {
				return nil, err
			}
			ofield.Value = val
		}

		ofields = append(ofields, ofield)
	}
	return
}

// TODO: need to study what exactly this method is supposed to do and not do
//       -> check the Java driver version
//
// IDEA: maybe this could be DeserializeField?  Might be useful for RidBags. Anything else?
func (serde ORecordSerializerV0) DeserializePartial(doc *oschema.ODocument, buf *bytes.Reader, fields []string) error {
	return fmt.Errorf("DeserializePartial: Non implemented")
}

// Serialize takes an ODocument and serializes it to bytes in accordance
// with the OrientDB binary serialization spec and writes them to the
// bytes.Buffer passed in.
func (serde ORecordSerializerV0) Serialize(doc *oschema.ODocument, w io.Writer) (err error) {
	defer catch(&err)
	// need to create a new buffer for the serialized record for ptr value calculations,
	// since the incoming buffer (`buf`) already has a lot of stuff written to it (session-id, etc)
	// that are NOT part of the serialized record
	serdebuf := new(bytes.Buffer) // holds only the serialized value

	// write the serialization version in so that the buffer size math works
	rw.WriteByte(serdebuf, 0)

	err = serde.serializeDocument(serdebuf, doc)
	if err != nil {
		return err
	}

	rw.WriteRawBytes(w, serdebuf.Bytes()[1:]) // remove the version byte at the beginning
	return nil
}

// serializeDocument writes the classname and the serialized record
// (header and data sections) of the ODocument to the obuf.WriteBuf.
//
// Because this method writes the classname but NOT the serialization
// version, this method is safe for recursive calls for EMBEDDED types.
func (serde ORecordSerializerV0) serializeDocument(buf *bytes.Buffer, doc *oschema.ODocument) error {
	varint.WriteString(buf, doc.Classname)

	return serde.writeSerializedRecord(buf, doc)
}

func (serde ORecordSerializerV0) SerializeClass(doc *oschema.ODocument, buf *bytes.Buffer) error {
	return fmt.Errorf("not implemented")
}

func (serde ORecordSerializerV0) writeSerializedRecord(buf *bytes.Buffer, doc *oschema.ODocument) (err error) {
	defer catch(&err)
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
			varint.WriteString(buf, fldName)

			ptrPos = append(ptrPos, buf.Len())
			// placeholder data pointer
			rw.WriteInt(buf, 0)
			// data value type
			rw.WriteByte(buf, byte(fld.Type))

			ptrVal = append(ptrVal, dataBuf.Len())
			// write the data value to a separate `data` buffer
			dbufpos, dbufvals, err := serde.writeDataValue(dataBuf, fld.Value, fld.Type)
			if err != nil {
				return err
			}

			if dbufpos != nil {
				subPtrPos = append(subPtrPos, dbufpos...)
				subPtrVal = append(subPtrVal, dbufvals...)
			}
		}

	} else {
		// serializing a full document (not just a property or SQL params map) -> use propertyId
		return fmt.Errorf("writeSerializedRecord(full): Non implemented") // TODO: fix this
	}

	// write End of Header (EOH) marker
	rw.WriteByte(buf, 0)

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
		return err
	}

	bs := buf.Bytes()
	for i, pos := range ptrPos {
		// this buffer works off a slice from the `buf` buffer, so writing to it should modify the underlying `buf` buffer
		tmpBuf := bytes.NewBuffer(bs[pos : pos+4])
		tmpBuf.Reset() // reset ptr to start of slice so can overwrite the placeholder value
		rw.WriteInt(tmpBuf, int32(ptrVal[i]))
	}

	return nil
}

func (serde ORecordSerializerV0) writeDataValue(buf *bytes.Buffer, value interface{}, datatype oschema.OType) (ptrPos, ptrVal []int, err error) {
	defer catch(&err)
	switch datatype {
	case oschema.STRING:
		varint.WriteString(buf, value.(string))
	case oschema.BOOLEAN:
		rw.WriteBool(buf, value.(bool))
	case oschema.INTEGER:
		varint.WriteVarint(buf, toInt64(value))
	case oschema.SHORT:
		rw.WriteShort(buf, value.(int16))
	case oschema.LONG:
		varint.WriteVarint(buf, toInt64(value))
	case oschema.FLOAT:
		rw.WriteFloat(buf, toFloat32(value))
	case oschema.DOUBLE:
		rw.WriteDouble(buf, toFloat64(value))
	case oschema.DATETIME:
		writeDateTime(buf, value)
	case oschema.DATE:
		writeDate(buf, value)
	case oschema.BINARY:
		varint.WriteBytes(buf, value.([]byte))
	case oschema.EMBEDDED:
		err = serde.serializeDocument(buf, value.(*oschema.ODocument))
	case oschema.EMBEDDEDLIST:
		err = serde.serializeEmbeddedCollection(buf, value.(oschema.OEmbeddedList))
	case oschema.EMBEDDEDSET:
		err = serde.serializeEmbeddedCollection(buf, value.(oschema.OEmbeddedList))
	case oschema.EMBEDDEDMAP:
		ptrPos, ptrVal = serde.writeEmbeddedMap(buf, value.(oschema.OEmbeddedMap))
	case oschema.LINK:
		serde.writeLink(buf, value.(*oschema.OLink))
	case oschema.LINKLIST:
		serde.writeLinkList(buf, value.([]*oschema.OLink))
	case oschema.LINKSET:
		serde.writeLinkList(buf, value.([]*oschema.OLink))
	case oschema.LINKMAP:
		serde.writeLinkMap(buf, value.(map[string]*oschema.OLink))
	case oschema.BYTE:
		rw.WriteByte(buf, value.(byte))
	case oschema.DECIMAL:
		return nil, nil, fmt.Errorf("ORecordSerializerBinary#writeDataValue DECIMAL NOT YET IMPLEMENTED")
	case oschema.CUSTOM:
		return nil, nil, fmt.Errorf("ORecordSerializerBinary#writeDataValue CUSTOM NOT YET IMPLEMENTED")
	case oschema.LINKBAG:
		return nil, nil, fmt.Errorf("ORecordSerializerBinary#writeDataValue LINKBAG NOT YET IMPLEMENTED")
	case oschema.ANY, oschema.TRANSIENT:
	// ANY and TRANSIENT are do nothing ops
	default:
		err = fmt.Errorf("unknown data value type: %v", datatype)
	}
	return ptrPos, ptrVal, err
}

// +-----------------+-----------------+
// |clusterId:varint | recordId:varInt |
// +-----------------+-----------------+
func (serde ORecordSerializerV0) writeLink(buf *bytes.Buffer, lnk *oschema.OLink) {
	varint.WriteVarint(buf, int64(lnk.RID.ClusterID))
	varint.WriteVarint(buf, int64(lnk.RID.ClusterPos))
}

// +-------------+-------------------+
// | size:varint | collection:LINK[] |
// +-------------+-------------------+
func (serde ORecordSerializerV0) writeLinkList(buf *bytes.Buffer, lnks []*oschema.OLink) {
	// number of entries in the list
	varint.WriteVarint(buf, int64(len(lnks)))
	for _, lnk := range lnks {
		serde.writeLink(buf, lnk)
	}
}

// The link map allow to have as key the types:
// STRING,SHORT,INTEGER,LONG,BYTE,DATE,DECIMAL,DATETIME,DATA,FLOAT,DOUBLE
// the serialization of the linkmap is a list of entry
//
// +----------------------------+
// | values:link_map_entry[]    |
// +----------------------------+
//
// link_map_entry structure
//
// +--------------+------------------+------------+
// | keyType:byte | keyValue:byte[]  | link:LINK  |
// +--------------+------------------+------------+
//
// keyType -  is the type of the key, can be only one of the listed type.
// keyValue - the value of the key serialized with the serializer of the type
// link -     the link value stored with the formant of a LINK
//
// TODO: right now only supporting string keys, but need to support the
//       datatypes listed above (also for EmbeddedMaps)
func (serde ORecordSerializerV0) writeLinkMap(buf *bytes.Buffer, m map[string]*oschema.OLink) {
	// number of entries in the map
	varint.WriteVarint(buf, int64(len(m)))
	for k, v := range m {
		// keyType
		rw.WriteByte(buf, byte(oschema.STRING))
		// keyValue
		varint.WriteString(buf, k)
		// link
		serde.writeLink(buf, v)
	}
}

// writeEmbeddedMap serializes the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
func (serde ORecordSerializerV0) writeEmbeddedMap(buf *bytes.Buffer, m oschema.OEmbeddedMap) ([]int, []int) {
	// number of entries in the map
	varint.WriteVarint(buf, int64(m.Len()))

	dataBuf := new(bytes.Buffer)

	ptrPos := make([]int, 0, m.Len()) // position in buf where data ptr int needs to be written
	ptrVal := make([]int, 0, m.Len()) // the data ptr value to be written in buf
	subPtrPos := make([]int, 0, 4)
	subPtrVal := make([]int, 0, 4)

	keys, vals, types := m.All()
	for i, k := range keys {
		// key type
		rw.WriteByte(buf, byte(oschema.STRING))

		// write the key value
		varint.WriteString(buf, k)

		ptrPos = append(ptrPos, buf.Len())
		rw.WriteInt(buf, 0) // placeholder integer for data ptr

		dataType := types[i]
		if dataType == oschema.UNKNOWN {
			dataType = getDataType(vals[i]) // TODO: not sure this is necessary
		}
		// write data type of the data
		rw.WriteByte(buf, byte(dataType))

		ptrVal = append(ptrVal, dataBuf.Len())

		dbufpos, dbufvals, err := serde.writeDataValue(dataBuf, vals[i], dataType)
		if err != nil {
			panic(err)
		}
		if dbufpos != nil {
			subPtrPos = append(subPtrPos, dbufpos...)
			subPtrVal = append(subPtrVal, dbufvals...)
		}
	}

	// TODO: simplify this
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

	if _, err := buf.Write(dataBuf.Bytes()); err != nil {
		panic(err)
	}

	bs := buf.Bytes()
	for i, pos := range ptrPos {
		tmpBuf := bytes.NewBuffer(bs[pos : pos+4])
		tmpBuf.Reset() // reset ptr to start of slice so can overwrite the placeholder value
		rw.WriteInt(tmpBuf, int32(ptrVal[i]))
	}
	return ptrPos, ptrVal
}

// Either id or name+typ will be filled in but not both.
// So in C this would be a union, not a struct.
type headerProperty struct {
	id   int32
	name string
	typ  byte
	ptr  int32
}

func readClassname(buf varint.ByteReader) string { // TODO: replace with just varint.ReadBytes?
	cnameLen := int(varint.ReadVarint(buf))
	if cnameLen < 0 {
		panic(fmt.Errorf("Varint for classname len in binary serialization was negative: %d", cnameLen))
	}
	cnameBytes := make([]byte, cnameLen)
	rw.ReadRawBytes(buf, cnameBytes)
	return string(cnameBytes)
}

func readHeader(buf varint.ByteReader) (hdr []headerProperty, err error) {
	defer catch(&err)
	hdr = make([]headerProperty, 0, 8)

	for {
		decoded := int32(varint.ReadVarint(buf))

		if decoded == 0 { // 0 marks end of header
			break

		} else if decoded > 0 {
			// have a property, not a document, so the number is a zigzag encoded length
			// for a string (property name)

			// read property name
			size := int(decoded)
			//data := buf.Next(size) // TODO: do not allocate
			data := make([]byte, size)
			if _, err := io.ReadFull(buf, data); err != nil {
				return nil, err
			}
			// hdr.propertyNames = append(hdr.propertyNames, string(data))

			// read data pointer
			ptr := rw.ReadInt(buf)

			// read data type
			dataType, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			hdr = append(hdr, headerProperty{name: string(data), typ: dataType, ptr: ptr})
		} else {
			// have a document, not a property, so the number is an encoded property id,
			// convert to (positive) property-id
			propertyId := decodeFieldIdInHeader(decoded)
			ptr := rw.ReadInt(buf)
			hdr = append(hdr, headerProperty{id: propertyId, ptr: ptr})
		}
	}
	return hdr, nil
}

// readDataValue reads the next data section from `buf` according
// to the type of the property (property.Typ) and updates the OField object
// to have the value.
func (serde ORecordSerializerV0) readDataValue(buf *bytes.Reader, datatype oschema.OType, eFunc embeddedRecordFunc) (val interface{}, err error) {
	defer catch(&err)

	switch datatype {
	case oschema.BOOLEAN:
		val = rw.ReadBool(buf)
	case oschema.INTEGER:
		val = int32(varint.ReadVarint(buf))
	case oschema.SHORT:
		val = rw.ReadShort(buf)
	case oschema.LONG:
		val = varint.ReadVarint(buf)
	case oschema.FLOAT:
		val = rw.ReadFloat(buf)
	case oschema.DOUBLE:
		val = rw.ReadDouble(buf)
	case oschema.DATETIME:
		val = serde.readDateTime(buf)
	case oschema.DATE:
		val = serde.readDate(buf)
	case oschema.STRING:
		val = varint.ReadString(buf)
	case oschema.BINARY:
		val = varint.ReadBytes(buf)
	case oschema.EMBEDDED:
		val, err = eFunc(buf)
	case oschema.EMBEDDEDLIST:
		val = serde.readEmbeddedCollection(buf, eFunc)
	case oschema.EMBEDDEDSET:
		val = serde.readEmbeddedCollection(buf, eFunc) // TODO: may need to create a set type as well
	case oschema.EMBEDDEDMAP:
		val = serde.readEmbeddedMap(buf, eFunc)
	case oschema.LINK:
		// a link is two int64's (cluster:record) - we translate it here to a string RID
		val = serde.readLink(buf)
	case oschema.LINKLIST, oschema.LINKSET:
		val = serde.readLinkList(buf)
	case oschema.LINKMAP:
		val = serde.readLinkMap(buf)
	case oschema.BYTE:
		val = rw.ReadByte(buf)
	case oschema.LINKBAG:
		val = serde.readLinkBag(buf)
	case oschema.CUSTOM:
		return nil, fmt.Errorf("ORecordSerializerBinary#readDataValue CUSTOM NOT YET IMPLEMENTED")
	case oschema.DECIMAL:
		return nil, fmt.Errorf("ORecordSerializerBinary#readDataValue DECIMAL NOT YET IMPLEMENTED")
	case oschema.ANY, oschema.TRANSIENT:
	// ANY and TRANSIENT are do nothing ops
	default:
		err = fmt.Errorf("unknown data value type: %v", datatype)
	}
	return val, err
}

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
func writeDateTime(buf *bytes.Buffer, value interface{}) error {
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
		return ErrDataTypeMismatch{
			ExpectedDataType: oschema.DATETIME,
			ExpectedGoType:   "time.Time | int64 | int",
			ActualValue:      value,
		}
	}
	varint.WriteVarint(buf, millisEpoch)
	return nil
}

// readDateTime reads an OrientDB DATETIME from the stream and converts it to
// a golang time.Time struct. DATETIME is precise to the second.
// The time zone of the time.Time returned should be the Local timezone.
//
// OrientDB server converts a DATETIME type is to millisecond unix epoch and
// stores it as the type LONG.  It is written as a varint long.
func (serde ORecordSerializerV0) readDateTime(buf io.ByteReader) time.Time {
	dtAsLong := varint.ReadVarint(buf)
	dtSecs := dtAsLong / 1000
	dtMillis := dtAsLong % 1000
	return time.Unix(dtSecs, dtMillis)
}

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
func writeDate(buf *bytes.Buffer, value interface{}) error {
	tm, ok := value.(time.Time)
	if !ok {
		return ErrDataTypeMismatch{
			ExpectedDataType: oschema.DATE,
			ExpectedGoType:   "time.Time",
			ActualValue:      value,
		}
	}

	tmMidnightUTC := time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.FixedZone("UTC", 0))
	secondsEpoch := tmMidnightUTC.Unix()
	dateAfterDiv := secondsEpoch / int64(86400)

	varint.WriteVarint(buf, dateAfterDiv)
	return nil
}

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
func (serde ORecordSerializerV0) readDate(buf io.ByteReader) time.Time {
	seconds := varint.ReadVarint(buf)

	dateAsLong := seconds * int64(86400)    // multiply the 86,400 seconds back
	utctm := time.Unix(dateAsLong, 0).UTC() // OrientDB returns it as a UTC date, so start with that
	loctm := utctm.Local()                  // convert to local time
	_, offsetInSecs := loctm.Zone()         // the compute the time zone difference
	offsetInNanos := offsetInSecs * 1000 * 1000 * 1000
	durOffset := time.Duration(offsetInNanos)
	adjustedLocTm := loctm.Add(-durOffset) // and finally adjust the time back to local time

	return adjustedLocTm
}

// Returns map of string keys to *oschema.OLink
func (serde ORecordSerializerV0) readLinkMap(buf *bytes.Reader) map[string]*oschema.OLink {
	nentries := int(varint.ReadVarint(buf))
	linkMap := make(map[string]*oschema.OLink, nentries)
	for i := 0; i < nentries; i++ {
		// ---[ read map key ]---
		datatype := rw.ReadByte(buf)
		if datatype != byte(oschema.STRING) {
			panic(fmt.Errorf("readLinkMap: datatype for key is NOT string but type: %v", datatype))
		}
		mapkey := varint.ReadString(buf)
		// ---[ read map value (always a RID) ]---
		linkMap[mapkey] = serde.readLink(buf)
	}
	return linkMap
}

// readLinkBag handles both Embedded and remote Tree-based OLinkBags.
func (serde ORecordSerializerV0) readLinkBag(buf *bytes.Reader) *oschema.OLinkBag {
	if bagType := rw.ReadByte(buf); bagType == byte(0) {
		return readTreeBasedLinkBag(buf)
	} else {
		return readEmbeddedLinkBag(buf)
	}
}

func readEmbeddedLinkBag(buf *bytes.Reader) *oschema.OLinkBag {
	bs, err := buf.ReadByte()
	if err != nil {
		panic(err)
	}

	if bs == 1 {
		uuid := readLinkBagUUID(buf)
		glog.Warningf("read uuid %v - now what?", uuid)
	} else {
		// if b wasn't zero, then there's no UUID and b was the first byte of an int32
		// specifying the size of the embedded bag collection
		// TODO: I'm not sure this is the right thing - the OrientDB is pretty hazy on how this works
		buf.UnreadByte()
	}

	bagsz := int(rw.ReadInt(buf))
	links := make([]*oschema.OLink, bagsz)
	for i := range links {
		links[i] = &oschema.OLink{RID: readRID(buf)}
	}

	return oschema.NewOLinkBag(links)
}

func readLinkBagUUID(buf *bytes.Reader) int32 {
	// TODO: I don't know what form the UUID is - an int32?  How is it serialized?
	panic("This LINKBAG has a UUID; support for UUIDs has not yet been added")
}

func readTreeBasedLinkBag(buf *bytes.Reader) *oschema.OLinkBag {
	// java/com/orientechnologies/orient/core/serialization/serializer/stream/OStreamSerializerSBTreeIndexRIDContainer.java
	off, _ := buf.Seek(0, 1)
	const (
		LONG_SIZE    = 8
		INT_SIZE     = 4
		BOOLEAN_SIZE = 1

		FILE_ID_OFFSET  = 0
		EMBEDDED_OFFSET = FILE_ID_OFFSET + LONG_SIZE
		DURABLE_OFFSET  = EMBEDDED_OFFSET + BOOLEAN_SIZE

		EMBEDDED_SIZE_OFFSET   = DURABLE_OFFSET + BOOLEAN_SIZE
		EMBEDDED_VALUES_OFFSET = EMBEDDED_SIZE_OFFSET + INT_SIZE

		SBTREE_ROOTINDEX_OFFSET  = DURABLE_OFFSET + BOOLEAN_SIZE
		SBTREE_ROOTOFFSET_OFFSET = SBTREE_ROOTINDEX_OFFSET + LONG_SIZE
	)

	buf.Seek(off+FILE_ID_OFFSET, 0)
	fileid := rw.ReadLong(buf)

	buf.Seek(off+DURABLE_OFFSET, 0)
	_ = rw.ReadBool(buf) // durable

	buf.Seek(off+EMBEDDED_OFFSET, 0)
	embedded := rw.ReadBool(buf)

	//fmt.Printf("fileId: %v, durable: %v, emb: %v\n", fileid, durable, embedded)
	if embedded {
		buf.Seek(off+EMBEDDED_SIZE_OFFSET, 0)
		size := rw.ReadInt(buf)

		out := make([]*oschema.OLink, 0, size)
		for i := 0; i < int(size); i++ {
			out = append(out, &oschema.OLink{RID: readRID(buf)})
		}
		return oschema.NewOLinkBag(out)
	} else {
		buf.Seek(off+SBTREE_ROOTINDEX_OFFSET, 0)
		pageIndex := rw.ReadLong(buf)

		buf.Seek(off+SBTREE_ROOTOFFSET_OFFSET, 0)
		pageOffset := rw.ReadInt(buf)

		size := rw.ReadInt(buf) // TODO: unverified
		rw.ReadInt(buf)         // TODO: unverified

		rootPointer := bonsaiBucketPtr{
			Index:  pageIndex,
			Offset: pageOffset,
		}
		return oschema.NewTreeOLinkBag(fileid, rootPointer.Index, rootPointer.Offset, size)
	}
}

// readLink reads a two int64's - the cluster and record.
// We translate it here to a string RID (cluster:record) and return it.
func (serde ORecordSerializerV0) readLink(buf io.ByteReader) *oschema.OLink {
	clusterId := varint.ReadVarint(buf)
	clusterPos := varint.ReadVarint(buf)
	return &oschema.OLink{RID: oschema.RID{ClusterID: int16(clusterId), ClusterPos: clusterPos}}
}

func getDataType(val interface{}) oschema.OType { // TODO: oschema.OTypeForValue ?
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

// readEmbeddedMap handles the EMBEDDEDMAP type. Currently, OrientDB only uses string
// types for the map keys, so that is an assumption of this method as well.
//
// TODO: change return type to (*oschema.OEmbeddedMap, error) {  ???
func (serde ORecordSerializerV0) readEmbeddedMap(buf *bytes.Reader, eFunc embeddedRecordFunc) map[string]interface{} {
	nrecs := int(varint.ReadVarint(buf))
	m := make(map[string]interface{}) // final map to be returned

	if nrecs < 0 {
		panic(fmt.Errorf("readEmbeddedMap: invalid records count: %d", nrecs))
	}

	// data structures for reading the map header section, which gives key names, value types and value ptrs

	type item struct {
		Key  string
		Type oschema.OType
		Ptr  int64
	}
	items := make([]item, 0, nrecs)

	// read map headers
	for i := 0; i < nrecs; i++ {
		keytype := oschema.OType(rw.ReadByte(buf))
		if keytype != oschema.STRING {
			panic(fmt.Errorf("ReadEmbeddedMap got a key datatype %v - but it should be 7 (string)", keytype))
		}
		key := varint.ReadString(buf)

		ptr := rw.ReadInt(buf)
		valtype := oschema.OType(rw.ReadByte(buf))

		items = append(items, item{
			Key: key, Type: valtype, Ptr: int64(ptr),
		})
	}

	// read map values
	for _, it := range items {
		if it.Ptr == 0 { // pointer is zero - no data, continue
			m[it.Key] = nil
			continue
		}
		if _, err := buf.Seek(it.Ptr-1, 0); err != nil {
			panic(fmt.Errorf("cannot seek in buffer: %s", err))
		}
		val, err := serde.readDataValue(buf, it.Type, eFunc)
		if err != nil {
			panic(err)
		}
		m[it.Key] = val
	}
	return m
}

// Serialization format for EMBEDDEDLIST and EMBEDDEDSET
// +-------------+------------+-------------------+
// |size:varInt  | type:Otype | items:item_data[] |
// +-------------+------------+-------------------+
//
// The item_data data structure is:
// +------------------+--------------+
// | data_type:OType  | data:byte[]  |
// +------------------+--------------+
func (serde ORecordSerializerV0) serializeEmbeddedCollection(buf *bytes.Buffer, ls oschema.OEmbeddedList) error {
	varint.WriteVarint(buf, int64(ls.Len()))

	// following the lead of the Java driver, you don't specify the type of the list overall
	// (I tried to and it doesn't work, at least with OrientDB-2.0.1)
	rw.WriteByte(buf, byte(oschema.ANY))

	for _, val := range ls.Values() {
		buf.WriteByte(byte(ls.Type()))
		_, _, err := serde.writeDataValue(buf, val, ls.Type())
		if err != nil {
			return err
		}
	}

	return nil
}

// readEmbeddedCollection handles both EMBEDDEDLIST and EMBEDDEDSET types.
// Java client API:
//     Collection<?> readEmbeddedCollection(BytesContainer bytes, Collection<Object> found, ODocument document) {
//     `found`` gets added to during the recursive iterations
func (serde ORecordSerializerV0) readEmbeddedCollection(buf *bytes.Reader, eFunc embeddedRecordFunc) []interface{} {
	nrecs := int(varint.ReadVarint(buf))

	datatype := oschema.OType(rw.ReadByte(buf))
	if datatype != oschema.ANY {
		//debug.PrintStack()
		panic(fmt.Errorf("ReadEmbeddedList got a datatype %v - currently that datatype is not supported", datatype))
		//return nil // TODO: it founds CUSTOM type sometimes
	}

	arr := make([]interface{}, nrecs)
	// loop over all recs
	for i := range arr {
		// if type is ANY (unknown), then the next byte specifies the type of record to follow
		itemtype := oschema.OType(rw.ReadByte(buf))
		if itemtype == oschema.ANY {
			arr[i] = nil // this is what the Java client does
			continue
		}

		val, err := serde.readDataValue(buf, itemtype, eFunc)
		if err != nil {
			panic(err)
		}
		arr[i] = val
	}
	return arr
}

func toFloat32(value interface{}) float32 {
	switch value.(type) {
	case float32:
		return value.(float32)
	case float64:
		return float32(value.(float64))
	case int:
		return float32(value.(int))
	case int32:
		return float32(value.(int32))
	case int64:
		return float32(value.(int64))
	default:
		panic("types missmatch")
	}
}

func toFloat64(value interface{}) float64 {
	switch value.(type) {
	case float64:
		return value.(float64)
	case float32:
		return float64(value.(float32))
	case int:
		return float64(value.(int))
	case int32:
		return float64(value.(int32))
	case int64:
		return float64(value.(int64))
	default:
		panic("types missmatch")
	}
}

func encodeFieldIDForHeader(id int32) int32 {
	return (id + 1) * -1
}

func decodeFieldIdInHeader(decoded int32) int32 {
	propertyId := (decoded * -1) - 1
	return propertyId
}

// refreshGlobalPropertiesIfRequired iterates through all the fields
// of the binserde header. If any of the fieldIds are NOT in the GlobalProperties
// map of the current ODatabase object, then the GlobalProperties are
// stale and need to be refresh (this likely means CREATE PROPERTY statements
// were recently issued).
//
// If the GlobalProperties data is stale, then it must be refreshed, so
// refreshGlobalProperties is called.
func (db *Database) refreshGlobalPropertiesIfRequired(hdr []headerProperty) error {
	if db == nil || db.db == nil {
		return nil
	}
	for _, prop := range hdr {
		if prop.name == "" {
			if _, ok := db.db.GetGlobalProperty(int(prop.id)); !ok {
				return db.refreshGlobalProperties()
			}
		}
	}
	return nil
}

// refreshGlobalProperties is called when it is discovered,
// while in the middle of reading the response from the OrientDB
// server, that the GlobalProperties are stale.
func (db *Database) refreshGlobalProperties() error {
	// ---[ load #0:0 - config record ]---
	oschemaRID, err := db.loadConfigRecord()
	if err != nil {
		return err
	}
	// ---[ load #0:1 - oschema record ]---
	err = db.loadSchema(oschemaRID)
	if err != nil {
		return err
	}
	return nil
}

func (serde ORecordSerializerV0) readLinkList(buf io.ByteReader) []*oschema.OLink {
	nrecs := int(varint.ReadVarint(buf))
	links := make([]*oschema.OLink, nrecs)
	for i := range links {
		links[i] = serde.readLink(buf)
	}
	return links
}

type bonsaiBucketPtr struct {
	Index  int64
	Offset int32
}

func (serde ORecordSerializerV0) ToMap(db *Database, buf *bytes.Reader) (result map[string]interface{}, err error) {
	defer catch(&err)
	result = make(map[string]interface{})
	_ = readClassname(buf)

	ofields, err := serde.deserializeFields(db, buf, func(buf *bytes.Reader) (interface{}, error) {
		return serde.ToMap(db, buf)
	})
	if err != nil {
		return nil, err
	}

	for _, ofield := range ofields {
		result[ofield.Name] = ofield.Value
	}

	return result, nil
}
