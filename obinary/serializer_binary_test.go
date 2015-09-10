package obinary

import (
	"bytes"
	"encoding/base64"
	"math/big"
	"testing"

	"github.com/istreamdata/orientgo/oschema"
)

func Test_TMP_SerializerParams(t *testing.T) {
	params := []interface{}{int32(5)}
	data1, _ := serializeSQLParams(serializer(0), params, "parameters")
	data1[len(data1)-2] = 1 // fix type int64 -> int32

	buf := bytes.NewBuffer(nil)
	doc := oschema.NewEmptyDocument()
	doc.SetField("parameters", arrayToParamsMap(params))
	if err := GetDefaultRecordFormat().ToStream(buf, doc); err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(buf.Bytes(), data1) != 0 {
		t.Fatalf("different buffers:\n%v\n%v\n", buf.Bytes(), data1)
	}
}

func testBase64Compare(t *testing.T, out []byte, origBase64 string) {
	orig, _ := base64.StdEncoding.DecodeString(origBase64)
	if bytes.Compare(out, orig) != 0 {
		t.Fatalf("different buffers:\n%v\n%v\n", out, orig)
	}
}

func TestSerializeCommandNoParams(t *testing.T) {
	query := "SELECT FROM V WHERE Id = ?"
	buf := bytes.NewBuffer(nil)
	if err := NewOCommandSQL(query).ToStream(buf); err != nil {
		t.Fatal(err)
	}
	testBase64Compare(t, buf.Bytes(), "AAAAGlNFTEVDVCBGUk9NIFYgV0hFUkUgSWQgPSA/AAA=")
}

func TestSerializeCommandIntParam(t *testing.T) {
	query := "SELECT FROM V WHERE Id = ?"
	buf := bytes.NewBuffer(nil)
	if err := NewOCommandSQL(query, int32(25)).ToStream(buf); err != nil {
		t.Fatal(err)
	}
	testBase64Compare(t, buf.Bytes(), "AAAAGlNFTEVDVCBGUk9NIFYgV0hFUkUgSWQgPSA/AQAAAB0AABRwYXJhbWV0ZXJzAAAAEwwAAgcCMAAAABwBMgA=")
}

func testSerializeEmbMap(t *testing.T, off int, mp interface{}, origBase64 string) {
	buf := bytes.NewBuffer(nil)
	for i := 0; i < off; i++ {
		buf.WriteByte(0)
	}
	binaryRecordFormatV0{}.writeEmbeddedMap(buf, off, mp)
	testBase64Compare(t, buf.Bytes(), origBase64)
}

func TestSerializeEmbeddedMapInt32V0(t *testing.T) {
	testSerializeEmbMap(t, 0,
		map[int32]interface{}{int32(0): int32(25)},
		"AgcCMAAAAAkBMg==",
	)
}

func TestSerializeEmbeddedMapIntV0(t *testing.T) {
	testSerializeEmbMap(t, 0,
		map[int]interface{}{0: 25},
		"AgcCMAAAAAkDMg==",
	)
}

func TestSerializeEmbeddedMapIntOffsV0(t *testing.T) {
	testSerializeEmbMap(t, 4,
		map[int]interface{}{0: 25},
		"AAAAAAIHAjAAAAANAzI=",
	)
}

func TestSerializeEmbeddedMapStringV0(t *testing.T) {
	testSerializeEmbMap(t, 0,
		map[string]interface{}{"one": "two"},
		"AgcGb25lAAAACwcGdHdv",
	)
}

func TestSerializeEmbeddedMapEmptyV0(t *testing.T) {
	testSerializeEmbMap(t, 0,
		map[string]interface{}{},
		"AA==",
	)
}

func testSerializeEmbCol(t *testing.T, off int, col interface{}, origBase64 string) {
	buf := bytes.NewBuffer(nil)
	for i := 0; i < off; i++ {
		buf.WriteByte(0)
	}
	binaryRecordFormatV0{}.writeEmbeddedCollection(buf, off, col, oschema.UNKNOWN)
	testBase64Compare(t, buf.Bytes(), origBase64)
}

func TestSerializeEmbeddedColStringV0(t *testing.T) {
	testSerializeEmbCol(t, 0,
		[]string{"a", "b", "c"},
		"BhcHAmEHAmIHAmM=",
	)
}

func TestSerializeEmbeddedColStringOffsV0(t *testing.T) {
	testSerializeEmbCol(t, 4,
		[]string{"a", "b", "c"},
		"AAAAAAYXBwJhBwJiBwJj",
	)
}

func testSerializeDoc(t *testing.T, doc *oschema.ODocument, origBase64 string) {
	buf := bytes.NewBuffer(nil)
	GetDefaultRecordFormat().ToStream(buf, doc)
	testBase64Compare(t, buf.Bytes(), origBase64)
}

func TestSerializeDocumentEmpty(t *testing.T) {
	doc := oschema.NewEmptyDocument()
	doc.SetField("parameters", map[string]interface{}{})
	testSerializeDoc(t,
		doc,
		"AAAUcGFyYW1ldGVycwAAABMMAAA=",
	)
}

func TestSerializeDocumentFieldStringMap(t *testing.T) {
	doc := oschema.NewEmptyDocument()
	doc.SetField("parameters", map[string]string{"one": "two"})
	testSerializeDoc(t,
		doc,
		"AAAUcGFyYW1ldGVycwAAABMMAAIHBm9uZQAAAB4HBnR3bw==",
	)
}

func TestSerializeDocumentFieldMapAndArr(t *testing.T) {
	doc := oschema.NewEmptyDocument()
	doc.SetField("map", map[string]string{"one": "two"})
	doc.SetField("arr", []string{"a", "b", "c"})
	testSerializeDoc(t,
		doc,
		"AAAGbWFwAAAAFQwGYXJyAAAAJAoAAgcGb25lAAAAIAcGdHdvBhcHAmEHAmIHAmM=",
	)
}

func TestSerializeDecimalV0(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	binaryRecordFormatV0{}.writeSingleValue(buf, 0, big.NewInt(123456789), oschema.DECIMAL, oschema.UNKNOWN)
	testBase64Compare(t, buf.Bytes(), "AAAAAAAAAAQHW80V")
}
