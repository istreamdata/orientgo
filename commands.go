package orient

import (
	"bytes"
	"io"
	"reflect"

	"github.com/istreamdata/orientgo/obinary/rw"
)

var (
	_ Serializable = (*textReqCommand)(nil)

	_ OCommandRequestText = SQLQuery{}
	_ OCommandRequestText = SQLCommand{}
	_ OCommandRequestText = ScriptCommand{}
	_ OCommandRequestText = FunctionCommand{}
)

// OCommandRequestText is an interface for text-based database commands,
// which can be executed using database.Command function.
type OCommandRequestText interface {
	CustomSerializable
	GetText() string
}

func arrayToParamsMap(params []interface{}) interface{} {
	if len(params) == 1 && reflect.TypeOf(params[0]).Kind() == reflect.Map {
		return params[0]
	}
	mp := make(map[int32]interface{}, len(params))
	for i, p := range params {
		if ide, ok := p.(OIdentifiable); ok {
			p = ide.GetIdentity() // use RID only
		}
		mp[int32(i)] = p
	}
	return mp
}

func newTextReqCommand(text string, params ...interface{}) textReqCommand {
	return textReqCommand{text: text, params: params}
}

// textReqCommand is a generic text-based command.
//
// OCommandTextAbstract in Java world.
type textReqCommand struct {
	//OCommandReq
	text   string
	params []interface{}
}

func (rq textReqCommand) GetText() string {
	return rq.text
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
	doc := NewEmptyDocument()
	doc.SetField("parameters", params)
	if err = GetDefaultRecordSerializer().ToStream(buf, doc); err != nil {
		return
	}
	rw.WriteBytes(w, buf.Bytes())

	// TODO: check for composite keys
	rw.WriteBool(w, false) // composite keys
	return
}

// FunctionCommand is a command to call server-side function.
//
// OCommandFunction in Java world.
type FunctionCommand struct {
	textReqCommand
}

// NewFunctionCommand creates a new call request to server-side function with given name and arguments.
func NewFunctionCommand(name string, params ...interface{}) FunctionCommand {
	return FunctionCommand{
		textReqCommand: newTextReqCommand(name, params),
	}
}

// GetClassName returns Java class name
func (rq FunctionCommand) GetClassName() string {
	return "com.orientechnologies.orient.core.command.script.OCommandFunction"
}

// ScriptCommand is a way to execute batch-like commands.
//
// OCommandScript in Java world.
type ScriptCommand struct {
	lang string
	textReqCommand
}

// NewScriptCommand creates a new script request written in a given language (SQL/JS/Groovy/...),
// with specified body code and params.
//
// Example:
//
//		NewScriptCommand(LangJS, `var out = db.command("SELECT FROM V"); out`)
//
func NewScriptCommand(lang ScriptLang, body string, params ...interface{}) ScriptCommand {
	return ScriptCommand{
		lang:           string(lang),
		textReqCommand: newTextReqCommand(body, params),
	}
}

// GetClassName returns Java class name
func (rq ScriptCommand) GetClassName() string { return "s" }

// ToStream serializes command to specified Writer
func (rq ScriptCommand) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteString(w, rq.lang)
	return rq.textReqCommand.ToStream(w)
}

// SQLCommand is a non-SELECT sql command (EXEC/INSERT/DELETE).
//
// OCommandSQL in Java world.
type SQLCommand struct {
	textReqCommand
}

// NewSQLCommand creates a new SQL command request with given params.
//
// Example:
//
//		NewSQLCommand("INSERT INTO People (id, name) VALUES (?, ?)", id, name)
//
func NewSQLCommand(sql string, params ...interface{}) SQLCommand {
	return SQLCommand{newTextReqCommand(sql, params...)}
}

// GetClassName returns Java class name
func (rq SQLCommand) GetClassName() string { return "c" }

// SQLQuery is a SELECT-like SQL command.
//
// OSQLQuery in Java world.
type SQLQuery struct {
	text   string
	limit  int
	plan   string
	params []interface{}
}

// NewSQLQuery creates a new SQL query with given params.
//
// Example:
//
//		NewSQLQuery("SELECT FROM V WHERE id = ?", id)
//
func NewSQLQuery(sql string, params ...interface{}) SQLQuery {
	return SQLQuery{text: sql, params: params, limit: -1}
}

// GetText returns query text
func (rq SQLQuery) GetText() string { return rq.text }

// GetClassName returns Java class name
func (rq SQLQuery) GetClassName() string { return "q" }

// Limit sets a query record limit
func (rq SQLQuery) Limit(n int) SQLQuery {
	rq.limit = n
	return rq
}

// FetchPlan sets a query fetch plan
func (rq SQLQuery) FetchPlan(plan FetchPlan) SQLQuery {
	rq.plan = string(plan)
	return rq
}

// ToStream serializes command to specified Writer
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
	doc := NewEmptyDocument()
	doc.SetField("params", arrayToParamsMap(params)) // TODO: convertToRIDsIfPossible
	buf := bytes.NewBuffer(nil)
	if err := GetDefaultRecordSerializer().ToStream(buf, doc); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
