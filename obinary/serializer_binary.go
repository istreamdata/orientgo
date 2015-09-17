package obinary

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"time"

	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/binserde/varint"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

func init() {
	orient.RegisterRecordFormat(BinaryFormatName, func() orient.RecordSerializer { return &BinaryRecordFormat{} })
	orient.SetDefaultRecordFormat(BinaryFormatName)
}

const (
	BinaryFormatName           = "ORecordSerializerBinary"
	binaryFormatCurrentVersion = 0
	millisecPerDay             = 86400000
)

const (
	documentSerializableClassName = "__orientdb_serilized_class__ "
)

var nilRID = oschema.RID{ClusterID: -2, ClusterPos: -1}

var (
	binaryFormatVerions = []func() binaryRecordFormat{
		func() binaryRecordFormat { return &binaryRecordFormatV0{} },
	}
)

type bytesReadSeeker interface {
	io.Reader
	io.ByteReader
	io.Seeker
}

type binaryRecordFormat interface {
	Serialize(doc *oschema.ODocument, w io.Writer, off int, classOnly bool) error
	Deserialize(doc *oschema.ODocument, r bytesReadSeeker) error

	SetGlobalPropertyFunc(fnc orient.GlobalPropertyFunc)
}

type BinaryRecordFormat struct {
	fnc orient.GlobalPropertyFunc
}

func (BinaryRecordFormat) String() string { return BinaryFormatName }
func (f *BinaryRecordFormat) SetGlobalPropertyFunc(fnc orient.GlobalPropertyFunc) {
	f.fnc = fnc
}
func (f BinaryRecordFormat) ToStream(w io.Writer, rec interface{}) (err error) {
	defer catch(&err)
	doc, ok := rec.(*oschema.ODocument)
	if !ok {
		return orient.ErrTypeSerialization{Val: rec, Serializer: f}
	}
	// TODO: can send empty document to stream if serialization fails?
	buf := bytes.NewBuffer(nil)
	rw.WriteByte(buf, byte(binaryFormatCurrentVersion))
	off := rw.SizeByte
	// TODO: apply partial serialization to prevent infinite recursion of records
	ser := binaryFormatVerions[binaryFormatCurrentVersion]()
	ser.SetGlobalPropertyFunc(f.fnc)
	if err = ser.Serialize(doc, buf, off, false); err != nil {
		return err
	}
	rw.WriteRawBytes(w, buf.Bytes())
	return
}
func (f BinaryRecordFormat) FromStream(data []byte) (out interface{}, err error) {
	defer catch(&err)

	if len(data) < 1 {
		err = io.ErrUnexpectedEOF
		return
	}

	r := bytes.NewReader(data)
	vers := rw.ReadByte(r)

	// TODO: support partial deserialization (only certain fields)

	ser := binaryFormatVerions[vers]()
	ser.SetGlobalPropertyFunc(f.fnc)
	doc := oschema.NewEmptyDocument()
	if err = ser.Deserialize(doc, r); err != nil {
		return
	}
	return doc, nil
}

type globalProperty struct {
	Name string
	Type oschema.OType
}

type binaryRecordFormatV0 struct {
	getGlobalPropertyFunc orient.GlobalPropertyFunc
}

func (f *binaryRecordFormatV0) SetGlobalPropertyFunc(fnc orient.GlobalPropertyFunc) {
	f.getGlobalPropertyFunc = fnc
}
func (f binaryRecordFormatV0) getGlobalProperty(doc *oschema.ODocument, leng int) oschema.OGlobalProperty {
	id := (leng * -1) - 1

	if f.getGlobalPropertyFunc == nil {
		panic("can't read global properties")
	}
	prop, ok := f.getGlobalPropertyFunc(id)
	if !ok {
		panic("no global properties")
	}
	return prop
}
func (f binaryRecordFormatV0) Deserialize(doc *oschema.ODocument, r bytesReadSeeker) (err error) {
	defer catch(&err)

	className := f.readString(r)
	if len(className) != 0 {
		doc.FillClassNameIfNeeded(className)
	}

	var (
		fieldName string
		valuePos  int
		valueType oschema.OType
		last      int64
	)
	for {
		//var prop core.OGlobalProperty
		leng := int(varint.ReadVarint(r))
		if leng == 0 {
			// SCAN COMPLETED
			break
		} else if leng > 0 {
			// PARSE FIELD NAME
			fieldNameBytes := make([]byte, leng)
			rw.ReadRawBytes(r, fieldNameBytes)
			fieldName = string(fieldNameBytes)
			valuePos = int(f.readInteger(r))
			valueType = f.readOType(r)
		} else {
			// LOAD GLOBAL PROPERTY BY ID
			prop := f.getGlobalProperty(doc, leng)
			fieldName = prop.Name
			valuePos = int(f.readInteger(r))
			if prop.Type != oschema.ANY {
				valueType = prop.Type
			} else {
				valueType = f.readOType(r)
			}
		}

		if doc.RawContainsField(fieldName) {
			continue
		}
		if valuePos != 0 {
			headerCursor, _ := r.Seek(0, 1)
			r.Seek(int64(valuePos), 0)
			value := f.readSingleValue(r, valueType, doc)
			if cur, _ := r.Seek(0, 1); cur > last {
				last = cur
			}
			r.Seek(headerCursor, 0)
			doc.RawSetField(fieldName, value, valueType)
		} else {
			doc.RawSetField(fieldName, nil, oschema.UNKNOWN)
		}
	}

	//doc.ClearSource()

	if cur, _ := r.Seek(0, 1); last > cur {
		r.Seek(last, 0)
	}
	return
}
func (f binaryRecordFormatV0) readByte(r io.Reader) byte {
	return rw.ReadByte(r)
}
func (f binaryRecordFormatV0) readBinary(r bytesReadSeeker) []byte {
	return varint.ReadBytes(r)
}
func (f binaryRecordFormatV0) readString(r bytesReadSeeker) string {
	return varint.ReadString(r)
}
func (f binaryRecordFormatV0) readInteger(r io.Reader) int32 {
	return rw.ReadInt(r)
}
func (f binaryRecordFormatV0) readOType(r io.Reader) oschema.OType {
	return oschema.OType(f.readByte(r))
}
func (f binaryRecordFormatV0) readOptimizedLink(r bytesReadSeeker) oschema.RID {
	return oschema.RID{ClusterID: int16(varint.ReadVarint(r)), ClusterPos: int64(varint.ReadVarint(r))}
}
func (f binaryRecordFormatV0) readLinkCollection(r bytesReadSeeker) []oschema.OIdentifiable {
	n := int(varint.ReadVarint(r))
	out := make([]oschema.OIdentifiable, n)
	for i := range out {
		if id := f.readOptimizedLink(r); id != nilRID {
			out[i] = id
		}
	}
	return out
}
func (f binaryRecordFormatV0) readEmbeddedCollection(r bytesReadSeeker, doc *oschema.ODocument) []interface{} {
	n := int(varint.ReadVarint(r))
	if vtype := f.readOType(r); vtype == oschema.ANY {
		out := make([]interface{}, n) // TODO: convert to determined slice type with reflect?
		for i := range out {
			if itemType := f.readOType(r); itemType != oschema.ANY {
				out[i] = f.readSingleValue(r, itemType, doc)
			}
		}
		return out
	}
	// TODO: @orient: manage case where type is known
	return nil
}
func (f binaryRecordFormatV0) readLinkMap(r bytesReadSeeker, doc *oschema.ODocument) interface{} {
	size := int(varint.ReadVarint(r))
	if size == 0 {
		return nil
	}
	type entry struct {
		Key interface{}
		Val oschema.OIdentifiable
	}
	var (
		result   = make([]entry, 0, size) // TODO: can't return just this slice, need some public implementation
		keyTypes = make(map[oschema.OType]bool, 2)
	)
	for i := 0; i < size; i++ {
		keyType := f.readOType(r)
		keyTypes[keyType] = true
		key := f.readSingleValue(r, keyType, doc)
		value := f.readOptimizedLink(r)
		if value == nilRID {
			result = append(result, entry{Key: key, Val: nil})
		} else {
			result = append(result, entry{Key: key, Val: value})
		}
	}
	if len(keyTypes) == 1 { // TODO: reflect-based converter
		tp := oschema.UNKNOWN
		for k, _ := range keyTypes {
			tp = k
			break
		}
		switch tp {
		case oschema.UNKNOWN:
			return result
		case oschema.STRING:
			mp := make(map[string]oschema.OIdentifiable, len(result))
			for _, kv := range result {
				mp[kv.Key.(string)] = kv.Val
			}
			return mp
		default:
			panic(fmt.Errorf("don't how to make map of type %v", tp))
		}
	} else {
		panic(fmt.Errorf("map with different key type: %+v", keyTypes))
	}
	//return result
}
func (f binaryRecordFormatV0) readEmbeddedMap(r bytesReadSeeker, doc *oschema.ODocument) interface{} {
	size := int(varint.ReadVarint(r))
	if size == 0 {
		return nil
	}
	last := int64(0)
	type entry struct {
		Key interface{}
		Val interface{}
	}
	var (
		result     = make([]entry, 0, size) // TODO: can't return just this slice, need some public implementation
		keyTypes   = make(map[oschema.OType]bool, 1)
		valueTypes = make(map[oschema.OType]bool, 2)
	)
	for i := 0; i < size; i++ {
		keyType := f.readOType(r)
		key := f.readSingleValue(r, keyType, doc)
		valuePos := f.readInteger(r)
		valueType := f.readOType(r)
		keyTypes[keyType] = true
		valueTypes[valueType] = true
		if valuePos != 0 {
			headerCursor, _ := r.Seek(0, 1)
			r.Seek(int64(valuePos), 0)
			value := f.readSingleValue(r, valueType, doc)
			if off, _ := r.Seek(0, 1); off > last {
				last = off
			}
			r.Seek(headerCursor, 0)
			result = append(result, entry{Key: key, Val: value})
		} else {
			result = append(result, entry{Key: key, Val: nil})
		}
	}
	if off, _ := r.Seek(0, 1); last > off {
		r.Seek(last, 0)
	}
	//fmt.Printf("embedded map: types: %+v, vals: %+v\n", keyTypes, valueTypes)
	var (
		keyType reflect.Type
		valType reflect.Type = oschema.UNKNOWN.ReflectType()
	)
	if len(keyTypes) == 1 {
		for k, _ := range keyTypes {
			if k == oschema.UNKNOWN {
				return result
			}
			keyType = k.ReflectType()
			break
		}
	} else {
		panic(fmt.Errorf("map with different key type: %+v", keyTypes))
	}
	if len(valueTypes) == 1 {
		for v, _ := range valueTypes {
			valType = v.ReflectType()
			break
		}
	}
	rv := reflect.MakeMap(reflect.MapOf(keyType, valType))
	for _, kv := range result {
		rv.SetMapIndex(reflect.ValueOf(kv.Key), reflect.ValueOf(kv.Val))
	}
	return rv.Interface()
}
func (f binaryRecordFormatV0) readSingleValue(r bytesReadSeeker, valueType oschema.OType, doc *oschema.ODocument) (value interface{}) {
	switch valueType {
	case oschema.INTEGER:
		value = int32(varint.ReadVarint(r))
	case oschema.LONG:
		value = int64(varint.ReadVarint(r))
	case oschema.SHORT:
		value = int16(varint.ReadVarint(r))
	case oschema.STRING:
		value = f.readString(r)
	case oschema.DOUBLE:
		value = rw.ReadDouble(r)
	case oschema.FLOAT:
		value = rw.ReadFloat(r)
	case oschema.BYTE:
		value = f.readByte(r)
	case oschema.BOOLEAN:
		value = f.readByte(r) == 1
	case oschema.DATETIME:
		longTime := varint.ReadVarint(r)
		value = time.Unix(longTime/1000, longTime%1000)
	case oschema.DATE:
		//	long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
		//	int offset = ODateHelper.getDatabaseTimeZone().getOffset(savedTime);
		//	value = new Date(savedTime - offset);
		savedTime := varint.ReadVarint(r) * millisecPerDay
		t := time.Unix(savedTime/1000, savedTime%1000).UTC().Local()
		_, offset := t.Zone()
		value = t.Add(-time.Duration(offset) * time.Second)
	case oschema.EMBEDDED:
		doc2 := oschema.NewEmptyDocument()
		if err := f.Deserialize(doc2, r); err != nil {
			panic(err)
		}
		value = doc2
	//	if (((ODocument) value).containsField(ODocumentSerializable.CLASS_NAME)) {
	//	String className = ((ODocument) value).field(ODocumentSerializable.CLASS_NAME);
	//	try {
	//	Class<?> clazz = Class.forName(className);
	//	ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
	//	newValue.fromDocument((ODocument) value);
	//	value = newValue;
	//	} catch (Exception e) {
	//	throw new RuntimeException(e);
	//	}
	//	} else
	//	ODocumentInternal.addOwner((ODocument) value, document);
	case oschema.EMBEDDEDSET, oschema.EMBEDDEDLIST:
		value = f.readEmbeddedCollection(r, doc)
	case oschema.LINKSET, oschema.LINKLIST:
		value = f.readLinkCollection(r)
	case oschema.BINARY:
		value = f.readBinary(r)
	case oschema.LINK:
		value = f.readOptimizedLink(r)
	case oschema.LINKMAP:
		value = f.readLinkMap(r, doc)
	case oschema.EMBEDDEDMAP:
		value = f.readEmbeddedMap(r, doc)
	case oschema.DECIMAL:
		_ = int(rw.ReadInt(r)) // scale // TODO: use scale, use big.Float for 1.5
		unscaledValue := rw.ReadBytes(r)
		value = big.NewInt(0).SetBytes(unscaledValue)
	case oschema.LINKBAG: // TODO: implement LinkBag
		panic("can't deserialize LinkBag")
	//	ORidBag bag = new ORidBag();
	//	bag.fromStream(bytes);
	//	bag.setOwner(document);
	//	value = bag;
	case oschema.TRANSIENT:
	case oschema.ANY:
	case oschema.CUSTOM:
		// TODO: implement via Register global function
		panic("CUSTOM type is not supported for now")
		//	try {
		//	String className = readString(bytes);
		//	Class<?> clazz = Class.forName(className);
		//	OSerializableStream stream = (OSerializableStream) clazz.newInstance();
		//	stream.fromStream(readBinary(bytes));
		//	if (stream instanceof OSerializableWrapper)
		//	value = ((OSerializableWrapper) stream).getSerializable();
		//	else
		//	value = stream;
		//	} catch (Exception e) {
		//	throw new RuntimeException(e);
		//	}
	}
	return
}

func (f binaryRecordFormatV0) Serialize(doc *oschema.ODocument, w io.Writer, off int, classOnly bool) (err error) {
	defer catch(&err)

	buf := bytes.NewBuffer(nil)

	f.serializeClass(buf, doc)
	if classOnly {
		f.writeEmptyString(buf)
		return
	}
	fields := doc.GetFields()

	type item struct {
		Pos   int
		Ptr   int
		Field *oschema.ODocEntry
		Type  oschema.OType
	}

	var (
		items = make([]item, 0, len(fields))
	)
	for _, entry := range fields {
		it := item{Field: entry}
		// TODO: use global properties for serialization, if class is known
		f.writeString(buf, entry.Name)
		it.Pos = buf.Len()  // save buffer offset of pointer
		rw.WriteInt(buf, 0) // placeholder for data pointer
		tp := f.getFieldType(entry)
		if tp == oschema.UNKNOWN {
			err = fmt.Errorf("Can't serialize type %T with ODocument binary serializer", entry.Type)
			return
		}
		rw.WriteByte(buf, byte(tp))
		it.Type = tp
		items = append(items, it)
	}
	f.writeEmptyString(buf)
	for i, it := range items {
		if it.Field.Value == nil {
			continue
		}
		ptr := buf.Len()
		if f.writeSingleValue(buf, off+ptr, it.Field.Value, it.Type, f.getLinkedType(doc, it.Type, it.Field.Name)) {
			items[i].Ptr = ptr
		} else {
			items[i].Ptr = 0
		}
	}
	data := buf.Bytes()
	for _, it := range items {
		if it.Ptr != 0 {
			rw.Order.PutUint32(data[it.Pos:], uint32(int32(it.Ptr+off)))
		}
	}
	rw.WriteRawBytes(w, data)
	return
}
func (f binaryRecordFormatV0) serializeClass(w io.Writer, doc *oschema.ODocument) int {
	// TODO: final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document); if (clazz == null) ...
	if doc.Classname == "" {
		return f.writeEmptyString(w)
	} else {
		return f.writeString(w, doc.Classname)
	}
}
func (binaryRecordFormatV0) writeString(w io.Writer, v string) int {
	return varint.WriteString(w, v)
}
func (binaryRecordFormatV0) writeBinary(w io.Writer, v []byte) int {
	return varint.WriteBytes(w, v)
}
func (f binaryRecordFormatV0) writeEmptyString(w io.Writer) int {
	return f.writeBinary(w, nil)
}
func (binaryRecordFormatV0) writeOType(w io.Writer, tp oschema.OType) int {
	rw.WriteByte(w, byte(tp))
	return rw.SizeByte
}
func (f binaryRecordFormatV0) writeNullLink(w io.Writer) (n int) {
	n += varint.WriteVarint(w, int64(nilRID.ClusterID))
	n += varint.WriteVarint(w, int64(nilRID.ClusterPos))
	return
}
func (f binaryRecordFormatV0) writeOptimizedLink(w io.Writer, ide oschema.OIdentifiable) (n int) {
	// TODO: link = recursiveLinkSave(link)
	rid := ide.GetIdentity()
	if !rid.IsValid() {
		panic("cannot serialize invalid link")
	}
	n += varint.WriteVarint(w, int64(rid.ClusterID))
	n += varint.WriteVarint(w, int64(rid.ClusterPos))
	return
}
func (f binaryRecordFormatV0) writeLinkCollection(w io.Writer, col oschema.OIdentifiableCollection) {
	// TODO: assert (!(value instanceof OMVRBTreeRIDSet))
	varint.WriteVarint(w, int64(col.Len()))
	for item := range col.OIdentifiableIterator() {
		if item == nil {
			f.writeNullLink(w)
		} else {
			f.writeOptimizedLink(w, item)
		}
	}
}
func (f binaryRecordFormatV0) writeLinkMap(w io.Writer, o interface{}) {
	m := o.(map[string]oschema.OIdentifiable) // TODO: can use reflect to support map[Stringer]oschema.OIdentifiable
	varint.WriteVarint(w, int64(len(m)))
	for k, v := range m {
		// TODO @orient: check skip of complex types
		// FIXME @orient: changed to support only string key on map
		f.writeOType(w, oschema.STRING)
		f.writeString(w, k)
		if v == nil {
			f.writeNullLink(w)
		} else {
			f.writeOptimizedLink(w, v)
		}
	}
}
func (f binaryRecordFormatV0) writeEmbeddedMap(w io.Writer, off int, o interface{}) {
	mv := reflect.ValueOf(o)
	if mv.Kind() != reflect.Map {
		panic(fmt.Sprintf("only maps are supported as %v, got %T", oschema.EMBEDDEDMAP, o))
	}

	buf := bytes.NewBuffer(nil)

	type item struct {
		Pos  int
		Val  interface{}
		Type oschema.OType
		Ptr  int
	}

	items := make([]item, 0, mv.Len())

	varint.WriteVarint(buf, int64(mv.Len()))

	keys := mv.MapKeys()

	for _, kv := range keys {
		k := kv.Interface()
		v := mv.MapIndex(kv).Interface()
		// TODO @orient: check skip of complex types
		// FIXME @orient: changed to support only string key on map
		f.writeOType(buf, oschema.STRING)
		f.writeString(buf, fmt.Sprint(k)) // convert key to string
		it := item{Pos: buf.Len(), Val: v}
		rw.WriteInt(buf, 0) // ptr placeholder
		tp := f.getTypeFromValueEmbedded(v)
		if tp == oschema.UNKNOWN {
			panic(orient.ErrTypeSerialization{Val: v, Serializer: f})
		}
		it.Type = tp
		f.writeOType(buf, tp)
		items = append(items, it)
	}

	for i := range items {
		ptr := buf.Len()
		if f.writeSingleValue(buf, off+ptr, items[i].Val, items[i].Type, oschema.UNKNOWN) {
			items[i].Ptr = ptr
		} else {
			items[i].Ptr = 0
		}
	}
	data := buf.Bytes()
	for i := range items {
		if items[i].Ptr > 0 {
			rw.Order.PutUint32(data[items[i].Pos:], uint32(int32(items[i].Ptr+off)))
		}
	}
	rw.WriteRawBytes(w, data)
}
func (f binaryRecordFormatV0) writeSingleValue(w io.Writer, off int, o interface{}, tp, linkedType oschema.OType) (written bool) {
	switch tp {
	case oschema.BYTE:
		rw.WriteByte(w, toByte(o))
		written = true
	case oschema.BOOLEAN:
		rw.WriteBool(w, toBool(o))
		written = true
	case oschema.SHORT:
		written = varint.WriteVarint(w, int64(toInt16(o))) != 0
	case oschema.INTEGER:
		written = varint.WriteVarint(w, int64(toInt32(o))) != 0
	case oschema.LONG:
		written = varint.WriteVarint(w, int64(toInt64(o))) != 0
	case oschema.STRING:
		written = f.writeString(w, toString(o)) != 0
	case oschema.FLOAT:
		rw.WriteFloat(w, o.(float32))
		written = true
	case oschema.DOUBLE:
		rw.WriteDouble(w, o.(float64))
		written = true
	case oschema.DATETIME: // unix time in milliseconds
		if t, ok := o.(int64); ok {
			written = varint.WriteVarint(w, t) != 0
		} else {
			t := o.(time.Time)
			it := t.Unix()*1000 + int64(t.Nanosecond())/1e6 // TODO: just UnixNano()/1e6 ?
			written = varint.WriteVarint(w, it) != 0
		}
	case oschema.DATE:
		if t, ok := o.(int64); ok {
			written = varint.WriteVarint(w, t) != 0
		} else {
			t := o.(time.Time)
			it := t.Unix()*1000 + int64(t.Nanosecond())/1e6 // TODO: just UnixNano()/1e6 ?
			var offset int64
			// TODO: int offset = ODateHelper.getDatabaseTimeZone().getOffset(dateValue)
			written = varint.WriteVarint(w, (it+offset)/millisecPerDay) != 0
		}
	case oschema.EMBEDDED:
		written = true
		var edoc *oschema.ODocument
		switch d := o.(type) {
		case oschema.ODocument:
			edoc = &d
		case *oschema.ODocument:
			edoc = d
		case **oschema.ODocument:
			edoc = *d
		default:
			cur, err := o.(orient.DocumentSerializable).ToDocument()
			if err != nil {
				panic(err)
			}
			cur.SetField(documentSerializableClassName, cur.Classname) // TODO: pass empty value as nil?
			edoc = cur
		}
		if err := f.Serialize(edoc, w, off, false); err != nil {
			panic(err)
		}
	case oschema.EMBEDDEDSET, oschema.EMBEDDEDLIST:
		written = true
		f.writeEmbeddedCollection(w, off, o, linkedType)
	case oschema.DECIMAL:
		var d *big.Int
		switch v := o.(type) {
		case big.Int:
			d = &v
		case *big.Int:
			d = v
		default: // TODO: implement for big.Float in 1.5
			panic(orient.ErrTypeSerialization{Val: o, Serializer: f})
		}
		written = true
		rw.WriteInt(w, 0)           // scale value, 0 for ints
		rw.WriteBytes(w, d.Bytes()) // unscaled value
	case oschema.BINARY:
		written = f.writeBinary(w, o.([]byte)) != 0
	case oschema.LINKSET, oschema.LINKLIST:
		written = true
		f.writeLinkCollection(w, o.(oschema.OIdentifiableCollection))
	case oschema.LINK:
		written = f.writeOptimizedLink(w, o.(oschema.OIdentifiable)) != 0
	case oschema.LINKMAP:
		written = true
		f.writeLinkMap(w, o)
	case oschema.EMBEDDEDMAP:
		written = true
		f.writeEmbeddedMap(w, off, o)
	case oschema.LINKBAG:
		written = true
		if err := o.(orient.Serializable).ToStream(w); err != nil { // TODO: actually cast to ORidBag and call ToStream
			panic(err)
		}
	case oschema.CUSTOM:
		written = true
		val := o.(orient.CustomSerializable)
		f.writeString(w, val.GetClassName())
		if err := val.ToStream(w); err != nil {
			panic(err)
		}
	case oschema.TRANSIENT, oschema.ANY:
	default:
		panic(fmt.Errorf("unknown type: %v", tp))
	}
	return written
}
func (f binaryRecordFormatV0) writeEmbeddedCollection(w io.Writer, off int, o interface{}, linkedType oschema.OType) {
	mv := reflect.ValueOf(o)
	// TODO: handle OEmbeddedList
	if mv.Kind() != reflect.Slice && mv.Kind() != reflect.Array {
		panic(fmt.Sprintf("only maps are supported as %v, got %T", oschema.EMBEDDEDMAP, o))
	}

	buf := bytes.NewBuffer(nil)
	varint.WriteVarint(buf, int64(mv.Len()))
	// TODO @orient: manage embedded type from schema and auto-determined.
	f.writeOType(buf, oschema.ANY)
	for i := 0; i < mv.Len(); i++ {
		item := mv.Index(i).Interface()
		// TODO @orient: manage in a better way null entry
		if item == nil {
			f.writeOType(buf, oschema.ANY)
			continue
		}
		var tp oschema.OType = linkedType
		if tp == oschema.UNKNOWN {
			tp = f.getTypeFromValueEmbedded(item)
		}
		if tp != oschema.UNKNOWN {
			f.writeOType(buf, tp)
			ptr := buf.Len()
			f.writeSingleValue(buf, off+ptr, item, tp, oschema.UNKNOWN)
		} else {
			panic(orient.ErrTypeSerialization{Val: item, Serializer: f})
		}
	}
	rw.WriteRawBytes(w, buf.Bytes())
}
func (binaryRecordFormatV0) getLinkedType(doc *oschema.ODocument, tp oschema.OType, key string) oschema.OType {
	if tp != oschema.EMBEDDEDLIST && tp != oschema.EMBEDDEDSET && tp != oschema.EMBEDDEDMAP {
		return oschema.UNKNOWN
	}
	// TODO: OClass clazz = ODocumentInternal.getImmutableSchemaClass(document); if (clazz != null) ...
	return oschema.UNKNOWN
}
func (f binaryRecordFormatV0) getFieldType(fld *oschema.ODocEntry) oschema.OType {
	tp := fld.Type
	if tp != oschema.UNKNOWN {
		return tp
	}
	// TODO: implement this:
	//	final OProperty prop = entry.property;
	//	if (prop != null) type = prop.getType();
	if tp == oschema.UNKNOWN || tp == oschema.ANY {
		tp = oschema.OTypeForValue(fld.Value)
	}
	return tp
}
func (f binaryRecordFormatV0) getTypeFromValueEmbedded(o interface{}) oschema.OType {
	tp := oschema.OTypeForValue(o)
	if tp == oschema.LINK {
		if doc, ok := o.(*oschema.ODocument); ok && doc.GetIdentity().IsValid() {
			tp = oschema.EMBEDDED
		}
	}
	return tp
}

func toByte(o interface{}) byte {
	switch v := o.(type) {
	case byte:
		return v
	default:
		return reflect.ValueOf(o).Convert(reflect.TypeOf(byte(0))).Interface().(byte)
	}
}

func toBool(o interface{}) bool {
	switch v := o.(type) {
	case bool:
		return v
	default:
		return reflect.ValueOf(o).Convert(reflect.TypeOf(bool(false))).Interface().(bool)
	}
}

func toInt16(o interface{}) int16 {
	switch v := o.(type) {
	case int16:
		return v
	default:
		return reflect.ValueOf(o).Convert(reflect.TypeOf(int16(0))).Interface().(int16)
	}
}

func toInt32(o interface{}) int32 {
	switch v := o.(type) {
	case int32:
		return v
	case int:
		return int32(v)
	default:
		return reflect.ValueOf(o).Convert(reflect.TypeOf(int32(0))).Interface().(int32)
	}
}

func toInt64(o interface{}) int64 {
	switch v := o.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case uint:
		return int64(v)
	default:
		return reflect.ValueOf(o).Convert(reflect.TypeOf(int64(0))).Interface().(int64)
	}
}

func toString(o interface{}) string {
	switch v := o.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default: // TODO: use Stringer interface in case of failure?
		return reflect.ValueOf(o).Convert(reflect.TypeOf(string(""))).Interface().(string)
	}
}
