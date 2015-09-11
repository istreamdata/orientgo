package obinary

import (
	"bytes"
	"io"
	"reflect"
	"time"

	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

var (
	_ Serializable = (*OCommandTextReq)(nil)
	//_ Deserializable = (*serializableDocument)(nil)
)

type OCommandReq struct {
	Limit     int
	FetchPlan string
	UseCache  bool
	Timeout   time.Duration
}

func newOCommandTextReq(text string, params ...interface{}) OCommandTextReq {
	return OCommandTextReq{text: text, params: arrayToParamsMap(params)}
}

func arrayToParamsMap(params []interface{}) interface{} {
	if len(params) == 1 && reflect.TypeOf(params[0]).Kind() == reflect.Map {
		return params[0]
	} else {
		mp := make(map[int32]interface{}, len(params))
		for i, p := range params {
			if ide, ok := p.(oschema.OIdentifiable); ok {
				p = ide.GetIdentity() // use RID only
			}
			mp[int32(i)] = p
		}
		return mp
	}
}

type OCommandTextReq struct {
	//OCommandReq
	text   string
	params interface{} // must be map[int]interface{} for arrays or map[string]interface{} (?)
}

func (rq OCommandTextReq) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteString(w, rq.text)
	if rq.params == nil || reflect.ValueOf(rq.params).Len() == 0 {
		rw.WriteBool(w, false) // simple params are absent
		rw.WriteBool(w, false) // composite keys are absent
		return
	}

	rw.WriteBool(w, true) // simple params
	buf := bytes.NewBuffer(nil)
	doc := oschema.NewEmptyDocument()
	doc.SetField("parameters", rq.params)
	if err = GetDefaultRecordFormat().ToStream(buf, doc); err != nil {
		return
	}
	rw.WriteBytes(w, buf.Bytes())

	// TODO: check for composite keys
	rw.WriteBool(w, false) // composite keys
	return
}

func NewOCommandFunction(name string, params ...interface{}) OCommandFunction {
	return OCommandFunction{
		OCommandTextReq: newOCommandTextReq(name, params),
	}
}

type OCommandFunction struct {
	OCommandTextReq
}

func NewOCommandScript(lang string, text string, params ...interface{}) OCommandScript {
	return OCommandScript{
		lang:            lang,
		OCommandTextReq: newOCommandTextReq(text, params),
	}
}

type OCommandScript struct {
	lang string
	OCommandTextReq
}

func (rq OCommandScript) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteString(w, rq.lang)
	return rq.OCommandTextReq.ToStream(w)
}

func NewOCommandSQL(sql string, params ...interface{}) OCommandSQL {
	return OCommandSQL{newOCommandTextReq(sql, params...)}
}

type OCommandSQL struct {
	OCommandTextReq
}

func NewOSQLQuery(sql string, params ...interface{}) OSQLQuery {
	return OSQLQuery{text: sql, params: params, limit: -1}
}

type OSQLQuery struct {
	text   string
	limit  int
	plan   string
	params []interface{}
}

func (rq OSQLQuery) Limit(n int) OSQLQuery {
	rq.limit = n
	return rq
}

func (rq OSQLQuery) FetchPlan(plan string) OSQLQuery {
	rq.plan = plan
	return rq
}

func (rq OSQLQuery) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteString(w, rq.text)
	rw.WriteInt(w, int32(rq.limit))
	rw.WriteString(w, rq.plan)
	rw.WriteBytes(w, rq.serializeQueryParameters(rq.params))
	return
}

func (rq OSQLQuery) serializeQueryParameters(params []interface{}) []byte {
	if len(params) == 0 {
		return nil
	}
	doc := oschema.NewEmptyDocument()
	doc.SetField("params", arrayToParamsMap(params)) // TODO: convertToRIDsIfPossible
	buf := bytes.NewBuffer(nil)
	if err := GetDefaultRecordFormat().ToStream(buf, doc); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
