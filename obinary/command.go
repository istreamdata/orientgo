package obinary

import (
	"bytes"
	"fmt"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"io"
)

func (db *Database) serializer() ORecordSerializer {
	return db.sess.cli.serializer
}

func (db *Database) rawCommand(class string, payload []byte) (recs orient.Records, err error) {
	defer catch(&err)
	fullcmd := bytes.NewBuffer(nil)
	rw.WriteString(fullcmd, class)
	rw.WriteRawBytes(fullcmd, payload)

	// for synchronous commands the remaining content is an array of form:
	// [(synch-result-type:byte)[(synch-result-content:?)]]+
	// so the final value will by byte(0) to indicate the end of the array
	// and we must use a loop here
	err = db.sess.sendCmd(requestCommand, func(w io.Writer) {
		rw.WriteByte(w, byte('s')) // synchronous only supported for now
		rw.WriteBytes(w, fullcmd.Bytes())
	}, func(r io.Reader) {
		for {
			resType := rw.ReadByte(r)
			if resType == byte(0) {
				break
			}
			switch resultType := rune(resType); resultType {
			case 'n': // null result
				// do nothing
			case 'r': // single record
				recs = append(recs, db.readSingleRecord(r))
			case 'l': // collection of records
				recs = append(recs, db.readResultSet(r)...)
			case 'a': // serialized type
				recs = append(recs, SerializedRecord(rw.ReadBytes(r)))
			default:
				if class == "q" && resType == 2 { // TODO: always == 2?
					recs = append(recs, orient.SupplementaryRecord{Record: db.readSingleRecord(r)})
				} else {
					panic(fmt.Errorf("rawCommand: not supported result type %v, class: %s", resultType, class))
				}
			}
		}
	})
	return recs, err
}

// SQLCommand executes SQL commands that are not queries. Any SQL statement
// that does not being with "SELECT" should be sent here.  All SELECT
// statements should go to the SQLQuery function.
//
// Commands can be optionally parametrized using ?, such as:
//
//     INSERT INTO Foo VALUES(a, b, c) (?, ?, ?)
//
// The values for the placeholders (currently) must be provided as strings.
//
// Constraints (for now):
// 1. cmds with only simple positional parameters allowed
// 2. cmds with lists of parameters ("complex") NOT allowed
// 3. parameter types allowed: string only for now
func (db *Database) SQLCommand(sql string, params ...interface{}) (recs orient.Records, err error) {
	buf := bytes.NewBuffer(nil)
	if err = NewOCommandSQL(sql, params...).ToStream(buf); err != nil {
		return
	}
	return db.rawCommand("c", buf.Bytes())
}

func (db *Database) SQLQuery(fetchPlan *orient.FetchPlan, sql string, params ...interface{}) (recs orient.Records, err error) {
	// SQLQuery
	var payload []byte
	if fetchPlan == nil {
		fetchPlan = orient.DefaultFetchPlan
	}
	//	if fetchPlan != DefaultFetchPlan {
	//		return nil, fmt.Errorf("non-default fetch plan is not supported for now") // TODO: related to supplementary records parsing
	//	}
	payload, err = sqlSelectPayload(db.serializer(), sql, fetchPlan.Plan, params...)
	if err != nil {
		return
	}
	return db.rawCommand("q", payload)
}

func (db *Database) execScriptRaw(lang orient.ScriptLang, data []byte) (recs orient.Records, err error) {
	defer catch(&err)
	payload := new(bytes.Buffer)
	rw.WriteStrings(payload, string(lang))
	rw.WriteRawBytes(payload, data)
	return db.rawCommand("s", payload.Bytes())
}

func (db *Database) ExecScript(lang orient.ScriptLang, script string, params ...interface{}) (recs orient.Records, err error) {
	defer catch(&err)
	var data []byte
	if lang == orient.LangSQL {
		data, err = sqlPayload(db.serializer(), script, params...)
	} else {
		data, err = scriptPayload(db.serializer(), script, params...)
	}
	if err != nil {
		return
	}
	return db.execScriptRaw(lang, data)
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
