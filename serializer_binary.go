package orient

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"

	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
)

//func init() {
//	RegisterRecordFormat(binaryFormatName, func() RecordSerializer { return &BinaryRecordFormat{} })
//	SetDefaultRecordFormat(binaryFormatName)
//}

const (
	binaryFormatName           = "ORecordSerializerBinary"
	binaryFormatCurrentVersion = 0
	millisecPerDay             = 86400000
)

const (
	documentSerializableClassName = "__orientdb_serilized_class__ "
)

var nilRID = RID{ClusterID: -2, ClusterPos: -1}

var (
	binaryFormatVerions = []func() binaryRecordFormat{
		func() binaryRecordFormat { return &binaryRecordFormatV0{} },
	}
)

type binaryRecordFormat interface {
	Serialize(doc *Document, w io.Writer, off int, classOnly bool) error
	Deserialize(doc *Document, r *rw.ReadSeeker) error

	SetGlobalPropertyFunc(fnc GlobalPropertyFunc)
}

type BinaryRecordFormat struct {
	fnc GlobalPropertyFunc
}

func (BinaryRecordFormat) String() string { return binaryFormatName }
func (f *BinaryRecordFormat) SetGlobalPropertyFunc(fnc GlobalPropertyFunc) {
	f.fnc = fnc
}
func (f BinaryRecordFormat) ToStream(w io.Writer, rec ORecord) error {
	doc, ok := rec.(*Document)
	if !ok {
		return ErrTypeSerialization{Val: rec, Serializer: f}
	}
	// TODO: can send empty document to stream if serialization fails?
	bw := rw.NewWriter(w)
	bw.WriteByte(byte(binaryFormatCurrentVersion))
	off := rw.SizeByte
	// TODO: apply partial serialization to prevent infinite recursion of records
	ser := binaryFormatVerions[binaryFormatCurrentVersion]()
	ser.SetGlobalPropertyFunc(f.fnc)
	if err := bw.Err(); err != nil {
		return err
	}
	if err := ser.Serialize(doc, w, off, false); err != nil {
		return err
	}
	return bw.Err()
}
func (f BinaryRecordFormat) FromStream(data []byte) (out ORecord, err error) {
	if len(data) < 1 {
		err = io.ErrUnexpectedEOF
		return
	}

	r := bytes.NewReader(data)
	br := rw.NewReadSeeker(r)
	vers := br.ReadByte()
	if err = br.Err(); err != nil {
		return
	}

	// TODO: support partial deserialization (only certain fields)

	ser := binaryFormatVerions[vers]()
	ser.SetGlobalPropertyFunc(f.fnc)
	doc := NewEmptyDocument()
	if err = ser.Deserialize(doc, br); err != nil {
		return
	}
	return doc, nil
}

type globalProperty struct {
	Name string
	Type OType
}

type binaryRecordFormatV0 struct {
	getGlobalPropertyFunc GlobalPropertyFunc
}

func (f *binaryRecordFormatV0) SetGlobalPropertyFunc(fnc GlobalPropertyFunc) {
	f.getGlobalPropertyFunc = fnc
}
func (f binaryRecordFormatV0) getGlobalProperty(doc *Document, leng int) OGlobalProperty {
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
func (f binaryRecordFormatV0) Deserialize(doc *Document, r *rw.ReadSeeker) error {

	className := f.readString(r)
	if err := r.Err(); err != nil {
		return err
	}
	if len(className) != 0 {
		doc.FillClassNameIfNeeded(className)
	}

	var (
		fieldName string
		valuePos  int
		valueType OType
		last      int64
	)
	for {
		//var prop core.OGlobalProperty
		leng := int(r.ReadVarint())
		if err := r.Err(); err != nil {
			return err
		}
		if leng == 0 {
			// SCAN COMPLETED
			break
		} else if leng > 0 {
			// PARSE FIELD NAME
			fieldNameBytes := make([]byte, leng)
			r.ReadRawBytes(fieldNameBytes)
			fieldName = string(fieldNameBytes)
			valuePos = int(f.readInteger(r))
			valueType = f.readOType(r)
		} else {
			// LOAD GLOBAL PROPERTY BY ID
			prop := f.getGlobalProperty(doc, leng)
			fieldName = prop.Name
			valuePos = int(f.readInteger(r))
			if prop.Type != ANY {
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
			value, err := f.readSingleValue(r, valueType, doc)
			if err != nil {
				return err
			}
			if cur, _ := r.Seek(0, 1); cur > last {
				last = cur
			}
			r.Seek(headerCursor, 0)
			doc.RawSetField(fieldName, value, valueType)
		} else {
			doc.RawSetField(fieldName, nil, UNKNOWN)
		}
	}

	//doc.ClearSource()

	if cur, _ := r.Seek(0, 1); last > cur {
		r.Seek(last, 0)
	}
	return r.Err()
}
func (f binaryRecordFormatV0) readByte(r *rw.ReadSeeker) byte {
	return r.ReadByte()
}
func (f binaryRecordFormatV0) readBinary(r *rw.ReadSeeker) []byte {
	return r.ReadBytesVarint()
}
func (f binaryRecordFormatV0) readString(r *rw.ReadSeeker) string {
	return r.ReadStringVarint()
}
func (f binaryRecordFormatV0) readInteger(r *rw.ReadSeeker) int32 {
	return r.ReadInt()
}
func (f binaryRecordFormatV0) readOType(r *rw.ReadSeeker) OType {
	return OType(f.readByte(r))
}
func (f binaryRecordFormatV0) readOptimizedLink(r *rw.ReadSeeker) RID {
	return RID{ClusterID: int16(r.ReadVarint()), ClusterPos: int64(r.ReadVarint())}
}
func (f binaryRecordFormatV0) readLinkCollection(r *rw.ReadSeeker) []OIdentifiable {
	n := int(r.ReadVarint())
	out := make([]OIdentifiable, n)
	for i := range out {
		if id := f.readOptimizedLink(r); id != nilRID {
			out[i] = id
		}
	}
	return out
}
func (f binaryRecordFormatV0) readEmbeddedCollection(r *rw.ReadSeeker, doc *Document) ([]interface{}, error) {
	n := int(r.ReadVarint())
	if vtype := f.readOType(r); vtype == ANY {
		out := make([]interface{}, n) // TODO: convert to determined slice type with reflect?
		var err error
		for i := range out {
			if itemType := f.readOType(r); itemType != ANY {
				out[i], err = f.readSingleValue(r, itemType, doc)
				if err != nil {
					return nil, err
				}
			}
		}
		return out, nil
	}
	// TODO: @orient: manage case where type is known
	return nil, r.Err()
}
func (f binaryRecordFormatV0) readLinkMap(r *rw.ReadSeeker, doc *Document) (interface{}, error) {
	size := int(r.ReadVarint())
	if size == 0 {
		return nil, r.Err()
	}
	type entry struct {
		Key interface{}
		Val OIdentifiable
	}
	var (
		result   = make([]entry, 0, size) // TODO: can't return just this slice, need some public implementation
		keyTypes = make(map[OType]bool, 2)
	)
	for i := 0; i < size; i++ {
		keyType := f.readOType(r)
		keyTypes[keyType] = true
		key, err := f.readSingleValue(r, keyType, doc)
		if err != nil {
			return nil, err
		}
		value := f.readOptimizedLink(r)
		if value == nilRID {
			result = append(result, entry{Key: key, Val: nil})
		} else {
			result = append(result, entry{Key: key, Val: value})
		}
	}
	if len(keyTypes) == 1 { // TODO: reflect-based converter
		tp := UNKNOWN
		for k, _ := range keyTypes {
			tp = k
			break
		}
		switch tp {
		case UNKNOWN:
			return result, nil
		case STRING:
			mp := make(map[string]OIdentifiable, len(result))
			for _, kv := range result {
				mp[kv.Key.(string)] = kv.Val
			}
			return mp, nil
		default:
			panic(fmt.Errorf("don't how to make map of type %v", tp))
		}
	} else {
		panic(fmt.Errorf("map with different key type: %+v", keyTypes))
	}
	//return result
}
func (f binaryRecordFormatV0) readEmbeddedMap(r *rw.ReadSeeker, doc *Document) (interface{}, error) {
	size := int(r.ReadVarint())
	if size == 0 {
		return nil, r.Err()
	}
	last := int64(0)
	type entry struct {
		Key interface{}
		Val interface{}
	}
	var (
		result     = make([]entry, 0, size) // TODO: can't return just this slice, need some public implementation
		keyTypes   = make(map[OType]bool, 1)
		valueTypes = make(map[OType]bool, 2)
	)
	for i := 0; i < size; i++ {
		keyType := f.readOType(r)
		key, err := f.readSingleValue(r, keyType, doc)
		if err != nil {
			return nil, err
		}
		valuePos := f.readInteger(r)
		valueType := f.readOType(r)
		keyTypes[keyType] = true
		valueTypes[valueType] = true
		if valuePos != 0 {
			headerCursor, _ := r.Seek(0, 1)
			r.Seek(int64(valuePos), 0)
			value, err := f.readSingleValue(r, valueType, doc)
			if err != nil {
				return nil, err
			}
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
	if err := r.Err(); err != nil {
		return nil, err
	}
	//fmt.Printf("embedded map: types: %+v, vals: %+v\n", keyTypes, valueTypes)
	var (
		keyType reflect.Type
		valType reflect.Type = UNKNOWN.ReflectType()
	)
	if len(keyTypes) == 1 {
		for k, _ := range keyTypes {
			if k == UNKNOWN {
				return result, nil
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
		var value reflect.Value
		if kv.Val == nil {
			value = reflect.Zero(valType)
		} else {
			value = reflect.ValueOf(kv.Val)
		}
		rv.SetMapIndex(reflect.ValueOf(kv.Key), value)
	}
	return rv.Interface(), nil
}
func (f binaryRecordFormatV0) readSingleValue(r *rw.ReadSeeker, valueType OType, doc *Document) (value interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ic, ok := r.(*runtime.TypeAssertionError); ok {
				err = fmt.Errorf("writeSingleValue(%v): %v", valueType, ic)
			} else {
				panic(r)
			}
		}
	}()
	switch valueType {
	case INTEGER:
		value = int32(r.ReadVarint())
	case LONG:
		value = int64(r.ReadVarint())
	case SHORT:
		value = int16(r.ReadVarint())
	case STRING:
		value = f.readString(r)
	case DOUBLE:
		value = r.ReadDouble()
	case FLOAT:
		value = r.ReadFloat()
	case BYTE:
		value = f.readByte(r)
	case BOOLEAN:
		value = f.readByte(r) == 1
	case DATETIME:
		longTime := r.ReadVarint()
		value = time.Unix(longTime/1000, (longTime%1000)*1e6)
	case DATE:
		//	long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
		//	int offset = ODateHelper.getDatabaseTimeZone().getOffset(savedTime);
		//	value = new Date(savedTime - offset);
		savedTime := r.ReadVarint() * millisecPerDay
		t := time.Unix(savedTime/1000, (savedTime%1000)*1e6) //.UTC().Local()
		//		_, offset := t.Zone()
		//		value = t.Add(-time.Duration(offset) * time.Second)
		value = t
	case EMBEDDED:
		doc2 := NewEmptyDocument()
		if err = f.Deserialize(doc2, r); err != nil {
			return nil, err
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
	case EMBEDDEDSET, EMBEDDEDLIST:
		value, err = f.readEmbeddedCollection(r, doc)
	case LINKSET, LINKLIST:
		value = f.readLinkCollection(r)
	case BINARY:
		value = f.readBinary(r)
	case LINK:
		value = f.readOptimizedLink(r)
	case LINKMAP:
		value, err = f.readLinkMap(r, doc)
	case EMBEDDEDMAP:
		value, err = f.readEmbeddedMap(r, doc)
	case DECIMAL:
		value = f.readDecimal(r)
	case LINKBAG:
		bag := NewRidBag()
		if err = bag.FromStream(r); err != nil {
			return nil, err
		}
		bag.SetOwner(doc)
		value = bag
	case TRANSIENT:
	case ANY:
	case CUSTOM:
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
	if err == nil {
		err = r.Err()
	}
	return
}

func (f binaryRecordFormatV0) Serialize(doc *Document, w io.Writer, off int, classOnly bool) error {
	buf := bytes.NewBuffer(nil)
	bw := rw.NewWriter(buf)

	if _, err := f.serializeClass(bw, doc); err != nil {
		return err
	}
	if classOnly {
		f.writeEmptyString(bw)
		if err := bw.Err(); err != nil {
			return err
		}
		return rw.NewWriter(w).WriteRawBytes(buf.Bytes())
	}
	fields := doc.FieldsArray()

	type item struct {
		Pos   int
		Ptr   int
		Field *DocEntry
		Type  OType
	}

	var (
		items = make([]item, 0, len(fields))
	)
	for _, entry := range fields {
		it := item{Field: entry}
		// TODO: use global properties for serialization, if class is known
		f.writeString(bw, entry.Name)
		it.Pos = buf.Len() // save buffer offset of pointer
		bw.WriteInt(0)     // placeholder for data pointer
		tp := f.getFieldType(entry)
		if tp == UNKNOWN {
			return fmt.Errorf("Can't serialize type %T with Document binary serializer", entry.Type)
		}
		bw.WriteByte(byte(tp))
		it.Type = tp
		items = append(items, it)
	}
	f.writeEmptyString(bw)
	for i, it := range items {
		if it.Field.Value == nil {
			continue
		}
		ptr := buf.Len()
		if err := f.writeSingleValue(bw, off+ptr, it.Field.Value, it.Type, f.getLinkedType(doc, it.Type, it.Field.Name)); err != nil {
			return err
		}
		if buf.Len() != ptr {
			items[i].Ptr = ptr
		} else {
			items[i].Ptr = 0
		}
	}
	if err := bw.Err(); err != nil {
		return err
	}
	data := buf.Bytes()
	for _, it := range items {
		if it.Ptr != 0 {
			rw.Order.PutUint32(data[it.Pos:], uint32(int32(it.Ptr+off)))
		}
	}
	return rw.NewWriter(w).WriteRawBytes(data)
}
func (f binaryRecordFormatV0) serializeClass(w *rw.Writer, doc *Document) (int, error) {
	// TODO: final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document); if (clazz == null) ...
	if class := doc.ClassName(); class == "" {
		return f.writeEmptyString(w)
	} else {
		return f.writeString(w, class)
	}
}
func (binaryRecordFormatV0) writeString(w *rw.Writer, v string) (int, error) {
	return w.WriteStringVarint(v)
}
func (binaryRecordFormatV0) writeBinary(w *rw.Writer, v []byte) (int, error) {
	return w.WriteBytesVarint(v)
}
func (f binaryRecordFormatV0) writeEmptyString(w *rw.Writer) (int, error) {
	return f.writeBinary(w, nil)
}
func (binaryRecordFormatV0) writeOType(w *rw.Writer, tp OType) int {
	w.WriteByte(byte(tp))
	return rw.SizeByte
}
func (f binaryRecordFormatV0) writeNullLink(w *rw.Writer) int {
	n1, _ := w.WriteVarint(int64(nilRID.ClusterID))
	n2, _ := w.WriteVarint(int64(nilRID.ClusterPos))
	return n1 + n2
}
func (f binaryRecordFormatV0) writeOptimizedLink(w *rw.Writer, ide OIdentifiable) (int, error) {
	// TODO: link = recursiveLinkSave(link)
	rid := ide.GetIdentity()
	if !rid.IsValid() {
		return 0, fmt.Errorf("cannot serialize invalid link")
	}
	n1, _ := w.WriteVarint(int64(rid.ClusterID))
	n2, _ := w.WriteVarint(int64(rid.ClusterPos))
	return n1 + n2, w.Err()
}
func (f binaryRecordFormatV0) writeLinkCollection(w *rw.Writer, o interface{}) error {
	switch col := o.(type) {
	case []RID:
		w.WriteVarint(int64(len(col)))
		for _, rid := range col {
			if rid == nilRID {
				f.writeNullLink(w)
			} else {
				if _, err := f.writeOptimizedLink(w, rid); err != nil {
					return err
				}
			}
		}
	case []OIdentifiable:
		w.WriteVarint(int64(len(col)))
		for _, item := range col {
			if item.GetIdentity() == nilRID {
				f.writeNullLink(w)
			} else {
				if _, err := f.writeOptimizedLink(w, item); err != nil {
					return err
				}
			}
		}
	case OIdentifiableCollection:
		// TODO: assert (!(value instanceof OMVRBTreeRIDSet))
		w.WriteVarint(int64(col.Len()))
		for item := range col.OIdentifiableIterator() {
			if item == nil {
				f.writeNullLink(w)
			} else {
				if _, err := f.writeOptimizedLink(w, item); err != nil {
					return err
				}
			}
		}
	default:
		panic(fmt.Errorf("not a link collection: %T", o))
	}
	return w.Err()
}
func (f binaryRecordFormatV0) writeLinkMap(w *rw.Writer, o interface{}) error {
	m := o.(map[string]OIdentifiable) // TODO: can use reflect to support map[Stringer]OIdentifiable
	w.WriteVarint(int64(len(m)))
	for k, v := range m {
		// TODO @orient: check skip of complex types
		// FIXME @orient: changed to support only string key on map
		f.writeOType(w, STRING)
		f.writeString(w, k)
		if v == nil {
			f.writeNullLink(w)
		} else {
			if _, err := f.writeOptimizedLink(w, v); err != nil {
				return err
			}
		}
	}
	return w.Err()
}
func (f binaryRecordFormatV0) writeEmbeddedMap(w *rw.Writer, off int, o interface{}) error {
	mv := reflect.ValueOf(o)
	if mv.Kind() != reflect.Map {
		panic(fmt.Sprintf("only maps are supported as %v, got %T", EMBEDDEDMAP, o))
	}

	buf := bytes.NewBuffer(nil)
	bw := rw.NewWriter(buf)

	type item struct {
		Pos  int
		Val  interface{}
		Type OType
		Ptr  int
	}

	items := make([]item, 0, mv.Len())

	bw.WriteVarint(int64(mv.Len()))

	keys := mv.MapKeys()

	for _, kv := range keys {
		k := kv.Interface()
		v := mv.MapIndex(kv).Interface()
		// TODO @orient: check skip of complex types
		// FIXME @orient: changed to support only string key on map
		f.writeOType(bw, STRING)
		f.writeString(bw, fmt.Sprint(k)) // convert key to string
		it := item{Pos: buf.Len(), Val: v}
		bw.WriteInt(0) // ptr placeholder
		tp := f.getTypeFromValueEmbedded(v)
		if tp == UNKNOWN {
			panic(ErrTypeSerialization{Val: v, Serializer: f})
		}
		it.Type = tp
		f.writeOType(bw, tp)
		items = append(items, it)
	}

	for i := range items {
		ptr := buf.Len()
		if err := f.writeSingleValue(bw, off+ptr, items[i].Val, items[i].Type, UNKNOWN); err != nil {
			return err
		}
		if ptr != buf.Len() {
			items[i].Ptr = ptr
		} else {
			items[i].Ptr = 0
		}
	}
	if err := bw.Err(); err != nil {
		return err
	}
	data := buf.Bytes()
	for i := range items {
		if items[i].Ptr > 0 {
			rw.Order.PutUint32(data[items[i].Pos:], uint32(int32(items[i].Ptr+off)))
		}
	}
	return w.WriteRawBytes(data)
}
func (f binaryRecordFormatV0) writeSingleValue(w *rw.Writer, off int, o interface{}, tp, linkedType OType) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if ic, ok := r.(*runtime.TypeAssertionError); ok {
				err = fmt.Errorf("writeSingleValue(%T -> %v): %v", o, tp, ic)
			} else {
				panic(r)
			}
		}
	}()
	switch tp {
	case BYTE:
		w.WriteByte(toByte(o))
	case BOOLEAN:
		w.WriteBool(toBool(o))
	case SHORT:
		w.WriteVarint(int64(toInt16(o)))
	case INTEGER:
		w.WriteVarint(int64(toInt32(o)))
	case LONG:
		w.WriteVarint(int64(toInt64(o)))
	case STRING:
		f.writeString(w, toString(o))
	case FLOAT:
		w.WriteFloat(o.(float32))
	case DOUBLE:
		w.WriteDouble(o.(float64))
	case DATETIME: // unix time in milliseconds
		if t, ok := o.(int64); ok {
			w.WriteVarint(t)
		} else {
			t := o.(time.Time)
			it := t.Unix()*1000 + int64(t.Nanosecond())/1e6
			w.WriteVarint(it)
		}
	case DATE:
		if t, ok := o.(int64); ok {
			w.WriteVarint(t)
		} else {
			t := o.(time.Time)
			it := t.Unix()*1000 + int64(t.Nanosecond())/1e6
			var offset int64
			// TODO: int offset = ODateHelper.getDatabaseTimeZone().getOffset(dateValue)
			w.WriteVarint((it + offset) / millisecPerDay)
		}
	case EMBEDDED:
		var edoc *Document
		switch d := o.(type) {
		case Document:
			edoc = &d
		case *Document:
			edoc = d
		case DocumentSerializable:
			edoc, err = o.(DocumentSerializable).ToDocument()
			if err != nil {
				return
			}
			edoc.SetField(documentSerializableClassName, edoc.ClassName()) // TODO: pass empty value as nil?
		default:
			edoc = NewEmptyDocument()
			if err = edoc.From(o); err != nil {
				return err
			}
			// TODO: set classname of struct?
		}
		err = f.Serialize(edoc, w, off, false)
	case EMBEDDEDSET, EMBEDDEDLIST:
		err = f.writeEmbeddedCollection(w, off, o, linkedType)
	case DECIMAL:
		f.writeDecimal(w, o)
	case BINARY:
		_, err = f.writeBinary(w, o.([]byte))
	case LINKSET, LINKLIST:
		err = f.writeLinkCollection(w, o)
	case LINK:
		_, err = f.writeOptimizedLink(w, o.(OIdentifiable))
	case LINKMAP:
		err = f.writeLinkMap(w, o)
	case EMBEDDEDMAP:
		err = f.writeEmbeddedMap(w, off, o)
	case LINKBAG:
		err = o.(*RidBag).ToStream(w)
	case CUSTOM:
		val := o.(CustomSerializable)
		f.writeString(w, val.GetClassName())
		err = val.ToStream(w)
	case TRANSIENT, ANY:
	default:
		panic(fmt.Errorf("unknown type: %v", tp))
	}
	if err == nil {
		err = w.Err()
	}
	return
}
func (f binaryRecordFormatV0) writeEmbeddedCollection(w *rw.Writer, off int, o interface{}, linkedType OType) error {
	mv := reflect.ValueOf(o)
	// TODO: handle OEmbeddedList
	if mv.Kind() != reflect.Slice && mv.Kind() != reflect.Array {
		panic(fmt.Sprintf("only maps are supported as %v, got %T", EMBEDDEDMAP, o))
	}

	buf := bytes.NewBuffer(nil)
	bw := rw.NewWriter(buf)
	bw.WriteVarint(int64(mv.Len()))
	// TODO @orient: manage embedded type from schema and auto-determined.
	f.writeOType(bw, ANY)
	for i := 0; i < mv.Len(); i++ {
		item := mv.Index(i).Interface()
		// TODO @orient: manage in a better way null entry
		if item == nil {
			f.writeOType(bw, ANY)
			continue
		}
		var tp OType = linkedType
		if tp == UNKNOWN {
			tp = f.getTypeFromValueEmbedded(item)
		}
		if tp != UNKNOWN {
			f.writeOType(bw, tp)
			ptr := buf.Len()
			if err := f.writeSingleValue(bw, off+ptr, item, tp, UNKNOWN); err != nil {
				return err
			}
		} else {
			panic(ErrTypeSerialization{Val: item, Serializer: f})
		}
	}
	if err := bw.Err(); err != nil {
		return err
	}
	return w.WriteRawBytes(buf.Bytes())
}
func (binaryRecordFormatV0) getLinkedType(doc *Document, tp OType, key string) OType {
	if tp != EMBEDDEDLIST && tp != EMBEDDEDSET && tp != EMBEDDEDMAP {
		return UNKNOWN
	}
	// TODO: OClass clazz = ODocumentInternal.getImmutableSchemaClass(document); if (clazz != null) ...
	return UNKNOWN
}
func (f binaryRecordFormatV0) getFieldType(fld *DocEntry) OType {
	tp := fld.Type
	if tp != UNKNOWN {
		return tp
	}
	// TODO: implement this:
	//	final OProperty prop = entry.property;
	//	if (prop != null) type = prop.getType();
	if tp == UNKNOWN || tp == ANY {
		tp = OTypeForValue(fld.Value)
	}
	return tp
}
func (f binaryRecordFormatV0) getTypeFromValueEmbedded(o interface{}) OType {
	tp := OTypeForValue(o)
	if tp == LINK {
		if doc, ok := o.(*Document); ok && doc.GetIdentity().IsValid() {
			tp = EMBEDDED
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
