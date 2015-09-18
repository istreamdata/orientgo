package obinary

import (
	"fmt"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
	"io"
)

func (db *Database) serializer() orient.RecordSerializer {
	return db.sess.cli.recordFormat
}

func (db *Database) updateCachedRecord(rec interface{}) {
	// TODO: implement records cache
}

func (db *Database) readSynchResult(r io.Reader) (result interface{}) {
	resType := rune(rw.ReadByte(r))
	switch resType {
	case 'n': // null result
		result = nil
	case 'r': // single record
		rec := db.readIdentifiable(r)
		if true { // TODO: try cast to Record
			db.updateCachedRecord(rec)
		}
		result = rec
	case 'l', 's': // collection of records
		n := int(rw.ReadInt(r))
		recs := make([]orient.OIdentifiable, n) // TODO: do something special for Set type?
		for i := range recs {
			rec := db.readIdentifiable(r)
			if true { // TODO: try cast to Record
				db.updateCachedRecord(rec)
			}
			recs[i] = rec
		}
		result = recs
	case 'i':
		var recs []orient.OIdentifiable
		for {
			status := rw.ReadByte(r)
			if status <= 0 {
				break
			}
			if rec := db.readIdentifiable(r); rec == nil {
				continue
			} else if status == 1 {
				if true { // TODO: try cast to Record
					db.updateCachedRecord(rec)
				}
				recs = append(recs, rec)
			}
		}
		result = recs
	case 'a': // serialized type
		s := rw.ReadString(r)
		result = stringRecordFormatAbs{}.FieldTypeFromStream(stringRecordFormatAbs{}.GetType(s), s)
	default:
		panic(fmt.Errorf("readSynchResult: not supported result type %v", resType))
	}
	if db.sess.cli.curProtoVers >= ProtoVersion17 {
		for {
			status := rw.ReadByte(r)
			if status <= 0 {
				break
			}
			rec := db.readIdentifiable(r)
			if rec != nil && status == 2 {
				db.updateCachedRecord(rec)
			}
		}
	}
	return
}

func (db *Database) Command(cmd orient.CustomSerializable) (result interface{}, err error) {
	defer catch(&err)

	var data []byte
	data, err = orient.SerializeAnyStreamable(cmd)
	if err != nil {
		return
	}

	live, async := false, false // synchronous only supported for now

	// for synchronous commands the remaining content is an array of form:
	// [(synch-result-type:byte)[(synch-result-content:?)]]+
	// so the final value will by byte(0) to indicate the end of the array
	// and we must use a loop here
	err = db.sess.sendCmd(requestCommand, func(w io.Writer) {
		if live {
			rw.WriteByte(w, byte('l'))
		} else if async {
			rw.WriteByte(w, byte('a'))
		} else {
			rw.WriteByte(w, byte('s'))
		}
		rw.WriteBytes(w, data)
	}, func(r io.Reader) {
		if async {
			// TODO: async
		} else {
			result = db.readSynchResult(r)
			if live {
				// TODO: live
			}
		}
	})
	return result, err
}
