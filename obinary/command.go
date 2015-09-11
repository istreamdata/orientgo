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
			resType := rune(rw.ReadByte(r))
			if resType == 0 {
				break
			}
			switch resType {
			case 'n': // null result
				// do nothing
			case 'r': // single record
				if rec := db.readSingleRecord(r); rec != nil {
					recs = append(recs, rec)
				}
			case 'l': // collection of records
				recs = append(recs, db.readResultSet(r)...)
			case 'a': // serialized type
				recs = append(recs, SerializedRecord(rw.ReadBytes(r)))
			case 2:
				if rec := db.readSingleRecord(r); rec != nil {
					recs = append(recs, orient.SupplementaryRecord{Record: rec})
				}
			default:
				panic(fmt.Errorf("rawCommand: not supported result type %v, class: %s", resType, class))
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
	if fetchPlan == nil {
		fetchPlan = orient.DefaultFetchPlan
	}
	buf := bytes.NewBuffer(nil)
	if err = NewOSQLQuery(sql, params...).FetchPlan(fetchPlan.Plan).ToStream(buf); err != nil {
		return
	}
	return db.rawCommand("q", buf.Bytes())
}

func (db *Database) CallScriptFunc(name string, params ...interface{}) (recs orient.Records, err error) {
	buf := bytes.NewBuffer(nil)
	if err = NewOCommandFunction(name, params...).ToStream(buf); err != nil {
		return
	}
	return db.rawCommand("com.orientechnologies.orient.core.command.script.OCommandFunction", buf.Bytes())
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
	buf := bytes.NewBuffer(nil)
	if err = NewOCommandScript(string(lang), script, params...).ToStream(buf); err != nil {
		return
	}
	return db.rawCommand("s", buf.Bytes())
}
