package obinary

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"github.com/istreamdata/orientgo/oschema"
	"strconv"
)

func serializeSQLParams(serde ORecordSerializer, params []interface{}, paramsMapName string) ([]byte, error) {
	// Java client uses Map<Object, Object>
	// Entry: {0=Honda, 1=Accord}, so positional params start with 0
	// OSQLQuery#serializeQueryParameters(Map<O,O> params)
	//   creates an ODocument
	//   params.put("params", convertToRIDsIfPossible(params))
	//   the convertToRIDsIfPossible is the one that handles Set vs. Map vs. ... vs. else -> primitive which is what simple strings are
	//  then the serialization is done via ODocument#toStream -> ORecordSerializer#toStream
	//    serializeClass(document)  => returns null
	//    only field name in the document is "params"
	//    when the embedded map comes in {0=Honda, 1=Accord}, it calls writeSingleValue

	if len(params) == 0 {
		return nil, nil
	}

	doc := oschema.NewDocument("")

	// the params must be serialized as an embedded map of form:
	// {params => {0=>paramVal1, 1=>paramVal2}}
	// which is a Field with:
	//   Field.Name = params
	//   Field.Value = {0=>paramVal1, 1=>paramVal2}} (map[string]interface{})

	dargs, err := driverArgs(params)
	if err != nil {
		return nil, err
	}

	paramsMap := oschema.NewEmbeddedMapWithCapacity(2)
	for i, pval := range dargs {
		paramsMap.Put(strconv.Itoa(i), pval, oschema.OTypeForValue(pval))
	}

	doc.FieldWithType(paramsMapName, paramsMap, oschema.EMBEDDEDMAP)

	buf := new(bytes.Buffer)
	err = buf.WriteByte(serde.Version())
	if err != nil {
		return nil, err
	}
	err = serde.Serialize(doc, buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func driverArgs(args []interface{}) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(args))
	for i, arg := range args {
		switch id := arg.(type) {
		case oschema.RID, oschema.OLink, *oschema.OLink:
			dargs[i] = id
		default:
			var err error
			dargs[i], err = driver.DefaultParameterConverter.ConvertValue(arg)
			if err != nil {
				return nil, fmt.Errorf("sql: converting Exec argument #%d's type: %v", i, err)
			}
		}
	}
	return dargs, nil
}

func catch(err *error) {
	if r := recover(); r != nil {
		switch rr := r.(type) {
		case error:
			*err = rr
		default:
			*err = fmt.Errorf("%v", r)
		}
	}
}
