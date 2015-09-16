package orient

import (
	"bytes"
	"io"
	"reflect"

	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
)

var (
	_ Serializable = (*textReqCommand)(nil)

	_ CustomSerializable = SQLQuery{}
	_ CustomSerializable = SQLCommand{}
	_ CustomSerializable = ScriptCommand{}
	_ CustomSerializable = FunctionCommand{}
)

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

func newTextReqCommand(text string, params ...interface{}) textReqCommand {
	return textReqCommand{text: text, params: params}
}

// OCommandTextAbstract in Java world
type textReqCommand struct {
	//OCommandReq
	text   string
	params []interface{}
}

func (rq textReqCommand) ToStream(w io.Writer) (err error) {
	defer catch(&err)

	params := arrayToParamsMap(rq.params)

	rw.WriteString(w, rq.text)
	if params == nil || reflect.ValueOf(params).Len() == 0 {
		rw.WriteBool(w, false) // simple params are absent
		rw.WriteBool(w, false) // composite keys are absent
		return
	}

	rw.WriteBool(w, true) // simple params
	buf := bytes.NewBuffer(nil)
	doc := oschema.NewEmptyDocument()
	doc.SetField("parameters", params)
	if err = GetDefaultRecordSerializer().ToStream(buf, doc); err != nil {
		return
	}
	rw.WriteBytes(w, buf.Bytes())

	// TODO: check for composite keys
	rw.WriteBool(w, false) // composite keys
	return
}

// OCommandFunction in Java world
type FunctionCommand struct {
	textReqCommand
}

func NewFunctionCommand(name string, params ...interface{}) FunctionCommand {
	return FunctionCommand{
		textReqCommand: newTextReqCommand(name, params),
	}
}
func (rq FunctionCommand) GetClassName() string {
	return "com.orientechnologies.orient.core.command.script.OCommandFunction"
}

// OCommandScript in Java world
type ScriptCommand struct {
	lang string
	textReqCommand
}

func NewScriptCommand(lang ScriptLang, text string, params ...interface{}) ScriptCommand {
	return ScriptCommand{
		lang:           string(lang),
		textReqCommand: newTextReqCommand(text, params),
	}
}
func (rq ScriptCommand) GetClassName() string { return "s" }
func (rq ScriptCommand) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteString(w, rq.lang)
	return rq.textReqCommand.ToStream(w)
}

// OCommandSQL in Java world
type SQLCommand struct {
	textReqCommand
}

func NewSQLCommand(sql string, params ...interface{}) SQLCommand {
	return SQLCommand{newTextReqCommand(sql, params...)}
}
func (rq SQLCommand) GetClassName() string { return "c" }

// OSQLQuery in Java world
type SQLQuery struct {
	text   string
	limit  int
	plan   string
	params []interface{}
}

func NewSQLQuery(sql string, params ...interface{}) SQLQuery {
	return SQLQuery{text: sql, params: params, limit: -1}
}
func (rq SQLQuery) GetClassName() string { return "q" }
func (rq SQLQuery) Limit(n int) SQLQuery {
	rq.limit = n
	return rq
}
func (rq SQLQuery) FetchPlan(plan FetchPlan) SQLQuery {
	rq.plan = string(plan)
	return rq
}
func (rq SQLQuery) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteString(w, rq.text)
	rw.WriteInt(w, int32(rq.limit))
	rw.WriteString(w, rq.plan)
	rw.WriteBytes(w, rq.serializeQueryParameters(rq.params))
	return
}
func (rq SQLQuery) serializeQueryParameters(params []interface{}) []byte {
	if len(params) == 0 {
		return nil
	}
	doc := oschema.NewEmptyDocument()
	doc.SetField("params", arrayToParamsMap(params)) // TODO: convertToRIDsIfPossible
	buf := bytes.NewBuffer(nil)
	if err := GetDefaultRecordSerializer().ToStream(buf, doc); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
