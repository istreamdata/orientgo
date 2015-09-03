package obinary

import (
	"bytes"
	"fmt"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
)

func (dbc *Client) rawCommand(class string, payload []byte) (recs orient.Records, err error) {
	defer catch(&err)
	buf := dbc.writeCommandAndSessionId(requestCommand)

	mode := byte('s') // synchronous only supported for now
	rw.WriteByte(buf, mode)

	fullcmd := new(bytes.Buffer)
	rw.WriteString(fullcmd, class)
	rw.WriteRawBytes(fullcmd, payload)

	// command-payload-length and command-payload
	rw.WriteBytes(buf, fullcmd.Bytes())

	// Driver supports only synchronous requests, so we need to wait until previous request is finished
	dbc.mutex.Lock()
	defer func() {
		dbc.mutex.Unlock()
	}()

	// send to the OrientDB server
	rw.WriteRawBytes(dbc.conx, buf.Bytes())

	// ---[ Read Response ]---

	if err = dbc.readStatusCodeAndError(); err != nil {
		return nil, err
	}

	// for synchronous commands the remaining content is an array of form:
	// [(synch-result-type:byte)[(synch-result-content:?)]]+
	// so the final value will by byte(0) to indicate the end of the array
	// and we must use a loop here

	for {
		resType := rw.ReadByte(dbc.conx)
		// This implementation assumes that SQLCommand can never have "supplementary records"
		// from an extended fetchPlan
		if resType == byte(0) {
			break
		}

		switch resultType := rune(resType); resultType {
		case 'n': // null result
			// do nothing
		case 'r': // single record
			recs = append(recs, dbc.readSingleRecord(dbc.conx))
		case 'l': // collection of records
			recs = append(recs, dbc.readResultSet(dbc.conx)...)
		case 'a': // serialized type
			recs = append(recs, SerializedRecord(rw.ReadBytes(dbc.conx)))
		default:
			if class == "q" && resType == 2 { // TODO: always == 2?
				recs = append(recs, orient.SupplementaryRecord{Record: dbc.readSingleRecord(dbc.conx)})
			} else {
				return nil, fmt.Errorf("not supported result type %v, proto: %d, class: %s", resultType, dbc.binaryProtocolVersion, class)
			}
		}
	}
	return recs, err
}

// SQLCommand executes SQL commands that are not queries. Any SQL statement
// that does not being with "SELECT" should be sent here.  All SELECT
// statements should go to the SQLQuery function.
//
// Commands can be optionally paramterized using ?, such as:
//
//     INSERT INTO Foo VALUES(a, b, c) (?, ?, ?)
//
// The values for the placeholders (currently) must be provided as strings.
//
// Constraints (for now):
// 1. cmds with only simple positional parameters allowed
// 2. cmds with lists of parameters ("complex") NOT allowed
// 3. parameter types allowed: string only for now
func (dbc *Client) SQLCommand(sql string, params ...interface{}) (recs orient.Records, err error) {
	// SQLCommand
	var payload []byte
	payload, err = sqlPayload(dbc.defaultSerde(), sql, params...)
	if err != nil {
		return
	}
	return dbc.rawCommand("c", payload)
}

func (dbc *Client) SQLQuery(fetchPlan *orient.FetchPlan, sql string, params ...interface{}) (recs orient.Records, err error) {
	// SQLQuery
	var payload []byte
	if fetchPlan == nil {
		fetchPlan = orient.DefaultFetchPlan
	}
	//	if fetchPlan != DefaultFetchPlan {
	//		return nil, fmt.Errorf("non-default fetch plan is not supported for now") // TODO: related to supplementary records parsing
	//	}
	payload, err = sqlSelectPayload(dbc.defaultSerde(), sql, fetchPlan.Plan, params...)
	if err != nil {
		return
	}
	return dbc.rawCommand("q", payload)
}

func (dbc *Client) execScriptRaw(lang orient.ScriptLang, data []byte) (recs orient.Records, err error) {
	defer catch(&err)
	payload := new(bytes.Buffer)
	rw.WriteStrings(payload, string(lang))
	rw.WriteRawBytes(payload, data)
	return dbc.rawCommand("s", payload.Bytes())
}

func (dbc *Client) ExecScript(lang orient.ScriptLang, script string, params ...interface{}) (recs orient.Records, err error) {
	defer catch(&err)
	var data []byte
	if lang == orient.LangSQL {
		data, err = sqlPayload(dbc.defaultSerde(), script, params...)
	} else {
		data, err = scriptPayload(dbc.defaultSerde(), script, params...)
	}
	if err != nil {
		return
	}
	return dbc.execScriptRaw(lang, data)
}

func scriptPayload(serde ORecordSerializer, text string, params ...interface{}) (data []byte, err error) {
	defer catch(&err)
	if len(params) > 0 {
		return nil, fmt.Errorf("params in scripts are not yet supported")
	}
	//  (text:string)
	//  (has-simple-parameters:boolean)
	//  (simple-paremeters:bytes[])  -> serialized Map (EMBEDDEDMAP??)
	//  (has-complex-parameters:boolean)
	//  (complex-parameters:bytes[])  -> serialized Map (EMBEDDEDMAP??)

	payload := new(bytes.Buffer)
	rw.WriteString(payload, text)

	// has-simple-parameters
	rw.WriteBool(payload, false)

	// has-complex-paramters => HARDCODING FALSE FOR NOW
	rw.WriteBool(payload, false)
	return payload.Bytes(), nil
}

func sqlPayload(serde ORecordSerializer, query string, params ...interface{}) (data []byte, err error) {
	defer catch(&err)
	//  (text:string)
	//  (has-simple-parameters:boolean)
	//  (simple-paremeters:bytes[])  -> serialized Map (EMBEDDEDMAP??)
	//  (has-complex-parameters:boolean)
	//  (complex-parameters:bytes[])  -> serialized Map (EMBEDDEDMAP??)

	payload := new(bytes.Buffer)
	rw.WriteString(payload, query)

	serializedParams, err := serializeSQLParams(serde, params, "parameters")
	if err != nil {
		return nil, err
	}

	// has-simple-parameters
	rw.WriteBool(payload, serializedParams != nil)

	if serializedParams != nil {
		rw.WriteBytes(payload, serializedParams)
	}

	// has-complex-paramters => HARDCODING FALSE FOR NOW
	rw.WriteBool(payload, false)
	return payload.Bytes(), nil
}

func sqlSelectPayload(serde ORecordSerializer, query string, fetchPlan string, params ...interface{}) (data []byte, err error) {
	defer catch(&err)
	//  (text:string)
	//  (non-text-limit:int)
	//  (fetch-plan:string)
	//  (serialized-params:bytes[])  -> serialized Map (EMBEDDEDMAP??)
	payload := new(bytes.Buffer)
	rw.WriteString(payload, query)

	// non-text-limit (-1 = use limit from query text)
	rw.WriteInt(payload, -1)

	// fetch plan
	rw.WriteString(payload, fetchPlan)

	serializedParams, err := serializeSQLParams(serde, params, "params")
	if err != nil {
		return nil, err
	}
	rw.WriteBytes(payload, serializedParams)
	return payload.Bytes(), nil
}
