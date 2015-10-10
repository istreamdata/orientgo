package obinary

import (
	"fmt"
	"gopkg.in/istreamdata/orientgo.v2"
	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
)

func (db *Database) serializer() orient.RecordSerializer {
	return db.sess.cli.recordFormat
}

func (db *Database) updateCachedRecord(rec orient.ORecord) {
	// TODO: implement records cache
}

func (db *Database) readSynchResult(r *rw.Reader) (result interface{}, err error) {
	resType := rune(r.ReadByte())
	if err = r.Err(); err != nil {
		return nil, err
	}
	switch resType {
	case 'n': // null result
		result = nil
	case 'r': // single record
		rec, err := db.readIdentifiable(r)
		if err != nil {
			return nil, err
		}
		if rec, ok := rec.(orient.ORecord); ok {
			db.updateCachedRecord(rec)
		}
		result = rec
	case 'l', 's': // collection of records
		n := int(r.ReadInt())
		recs := make([]orient.OIdentifiable, n) // TODO: do something special for Set type?
		for i := range recs {
			rec, err := db.readIdentifiable(r)
			if err != nil {
				return nil, err
			}
			if rec, ok := rec.(orient.ORecord); ok {
				db.updateCachedRecord(rec)
			}
			recs[i] = rec
		}
		result = recs
	case 'i':
		var recs []orient.OIdentifiable
		for {
			status := r.ReadByte()
			if status <= 0 {
				break
			}
			if rec, err := db.readIdentifiable(r); err != nil {
				return nil, err
			} else if rec == nil {
				continue
			} else if status == 1 {
				if rec, ok := rec.(orient.ORecord); ok {
					db.updateCachedRecord(rec)
				}
				recs = append(recs, rec)
			}
		}
		result = recs
	case 'a': // serialized type
		s := r.ReadString()
		if err = r.Err(); err != nil {
			return nil, err
		}
		format := orient.StringRecordFormatAbs{}
		result = format.FieldTypeFromStream(format.GetType(s), s)
	default:
		panic(fmt.Errorf("readSynchResult: not supported result type %v", resType))
	}
	if db.sess.cli.curProtoVers >= ProtoVersion17 {
		for {
			status := r.ReadByte()
			if status <= 0 {
				break
			}
			rec, err := db.readIdentifiable(r)
			if err != nil {
				return result, err
			}
			if rec != nil && status == 2 {
				if rec, ok := rec.(orient.ORecord); ok {
					db.updateCachedRecord(rec)
				}
			}
		}
	}
	return result, r.Err()
}

func (db *Database) Command(cmd orient.CustomSerializable) (result interface{}, err error) {
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
	err = db.sess.sendCmd(requestCommand, func(w *rw.Writer) error {
		if live {
			w.WriteByte(byte('l'))
		} else if async {
			w.WriteByte(byte('a'))
		} else {
			w.WriteByte(byte('s'))
		}
		w.WriteBytes(data)
		return w.Err()
	}, func(r *rw.Reader) error {
		if async {
			// TODO: async
		} else {
			result, err = db.readSynchResult(r)
			if err != nil {
				return err
			}
			if live {
				// TODO: live
			}
		}
		return r.Err()
	})
	return result, err
}
