package obinary

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"time"

	"github.com/istreamdata/orientgo/obinary/binserde/varint"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

func init() {
	RegisterRecordFormat(BinaryRecordFormat{})
	SetDefaultRecordFormat(BinaryFormatName)
}

const (
	BinaryFormatName           = "ORecordSerializerBinary"
	binaryFormatCurrentVersion = 0
	millisecPerDay             = 86400000
)

var nilRID = oschema.RID{ClusterID: -2, ClusterPos: -1}

var (
	binaryFormatVerions = []binaryRecordFormat{
		binaryRecordFormatV0{},
	}
)

type binaryRecordFormat interface {
	Serialize(doc *oschema.ODocument, w io.Writer, off int, classOnly bool) error
}

type BinaryRecordFormat struct{}

func (BinaryRecordFormat) FormatName() string { return BinaryFormatName }
func (f BinaryRecordFormat) ToStream(w io.Writer, rec interface{}) (err error) {
	defer catch(&err)
	doc, ok := rec.(*oschema.ODocument)
	if !ok {
		return ErrTypeSerialization{Val: rec, Serializer: f}
	}
	// TODO: can send empty document to stream if serialization fails
	buf := bytes.NewBuffer(nil)
	rw.WriteByte(buf, byte(binaryFormatCurrentVersion))
	off := rw.SizeByte
	// TODO: apply partial serialization to prevent infinite recursion of records
	if err = binaryFormatVerions[binaryFormatCurrentVersion].Serialize(doc, buf, off, false); err != nil {
		return err
	}
	rw.WriteRawBytes(w, buf.Bytes())
	return
}
func (f BinaryRecordFormat) FromStream(r io.Reader, rec Deserializable) error {
	panic("not implemented")
}

type binaryRecordFormatV0 struct{}

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
		Field *oschema.OField
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
		panic(fmt.Sprintf("only maps are supported as %s, got %T", oschema.EMBEDDEDMAP, o))
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
			panic(ErrTypeSerialization{Val: v, Serializer: f})
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
			cur, err := o.(DocumentSerializable).ToDocument()
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
			panic(ErrTypeSerialization{Val: o, Serializer: f})
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
		if err := o.(Serializable).ToStream(w); err != nil { // TODO: actually cast to ORidBag and call ToStream
			panic(err)
		}
	case oschema.CUSTOM:
		written = true
		val := o.(CustomSerializable)
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
		panic(fmt.Sprintf("only maps are supported as %s, got %T", oschema.EMBEDDEDMAP, o))
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
			panic(ErrTypeSerialization{Val: item, Serializer: f})
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
func (f binaryRecordFormatV0) getFieldType(fld *oschema.OField) oschema.OType {
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
