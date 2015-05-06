package obinary

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"

	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
)

//
// This file holds the functions related to SQLQuery and SQLCommand,
// the main workhorse functions of the ogonori client.
// All other "database commands" are in the dbCommands.go file.
//

const (
	// binary protocol sentinel values when reading single records
	RecordNull = -2
	RecordRID  = -3
)

//
// SQLQuery
//
// TODO: right now I return the entire resultSet as an array, thus all loaded into memory
//       it would be better to have obinary.dbCommands provide an iterator based model
//       that only needs to read a "row" (ODocument) at a time
// Perhaps SQLQuery() -> iterator/cursor
//         SQLQueryGetAll() -> []*ODocument ??
//
func SQLQuery(dbc *DBClient, sql string, fetchPlan string, params ...string) ([]*oschema.ODocument, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_COMMAND)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	mode := byte('s') // synchronous only supported for now
	err = rw.WriteByte(dbc.buf, mode)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// need a separate buffer to write the command-payload to, so
	// we can calculate its length before writing it to main dbc.buf
	commandBuf := new(bytes.Buffer)

	err = rw.WriteStrings(commandBuf, "q", sql) // q for query
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// non-text-limit (-1 = use limit from query text)
	err = rw.WriteInt(commandBuf, -1)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// fetch plan
	err = rw.WriteString(commandBuf, fetchPlan)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	serializedParams, err := serializeSimpleSQLParams(dbc, params)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if serializedParams != nil {
		rw.WriteBytes(commandBuf, serializedParams)
	}

	serializedCmd := commandBuf.Bytes()

	// command-payload-length and command-payload
	err = rw.WriteBytes(dbc.buf, serializedCmd)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	finalBytes := dbc.buf.Bytes()

	_, err = dbc.conx.Write(finalBytes)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	resType, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	resultType := int32(resType)

	var docs []*oschema.ODocument

	if resultType == 'n' {
		// NOTE: OStorageRemote in Java client just sets result to null and moves on
		ogl.Warn("Result type in SQLQuery is 'n' -> what to do? nothing ???") // DEBUG

	} else if resultType == 'r' {
		ogl.Warn("NOTE NOTE NOTE: this path has NOT YET BEEN TESTED") // DEBUG
		doc, err := readSingleRecord(dbc)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		docs = append(docs, doc)

	} else if resultType == 'l' {
		docs, err = readResultSet(dbc)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

	} else {
		// TODO: I've not yet tested this route of code -> how do so?
		ogl.Warn(">> Not yet supported")
		ogl.Fatal(fmt.Sprintf("NOTE NOTE NOTE: testing the resultType == '%v' (else) route of code -- "+
			"remove this note and test it!!", string(resultType)))
	}

	// any additional records are "supplementary" - from the fetchPlan these
	// need to be hydrated into ODocuments and then put into the primary Docs
	if dbc.binaryProtocolVersion >= int16(17) { // copied from the OrientDB 2.x Java client
		end, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if end != byte(0) {
			mapRIDToDoc, err := readSupplementaryRecords(dbc)
			if err != nil {
				return nil, oerror.NewTrace(err)
			}

			addSupplementaryRecsToPrimaryRecs(docs, mapRIDToDoc)
		}
	}
	return docs, nil
}

//
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
//
// SQL commands in OrientDB tend to return one of two types - a string return value or
// one or more documents. The meaning are specific to the type of query.
//
// ----------------
// For example:
// ----------------
//  for a DELETE statement:
//    retval = number of rows deleted (as a string)
//    docs = empty list
//
//  for an INSERT statement:
//    n = ?
//    docs = ?
//
//  for an CREATE CLASS statement:
//    retval = cluster id of the class (TODO: or it might be number of classes in cluster)
//    docs = empty list
//
//  for an DROP CLASS statement:
//    retval = "true" if successful, "" if class didn't exist (technically it returns null)
//    docs = empty list
//
func SQLCommand(dbc *DBClient, sql string, params ...string) (retval string, docs []*oschema.ODocument, err error) {
	dbc.buf.Reset()

	err = writeCommandAndSessionId(dbc, REQUEST_COMMAND)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	mode := byte('s') // synchronous only supported for now
	err = rw.WriteByte(dbc.buf, mode)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// need a separate buffer to write the command-payload to, so
	// we can calculate its length before writing it to main dbc.buf
	commandBuf := new(bytes.Buffer)

	// "classname" (command-type, really) and the sql command
	err = rw.WriteStrings(commandBuf, "c", sql) // c for command(non-idempotent)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// SQLCommand
	//  (text:string)
	//  (has-simple-parameters:boolean)
	//  (simple-paremeters:bytes[])  -> serialized Map (EMBEDDEDMAP??)
	//  (has-complex-parameters:boolean)
	//  (complex-parameters:bytes[])  -> serialized Map (EMBEDDEDMAP??)

	serializedParams, err := serializeSimpleSQLParams(dbc, params)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// has-simple-parameters
	err = rw.WriteBool(commandBuf, serializedParams != nil)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	if serializedParams != nil {
		rw.WriteBytes(commandBuf, serializedParams)
	}

	// FIXME: no complex parameters yet since I don't understand what they are
	// has-complex-paramters => HARDCODING FALSE FOR NOW
	err = rw.WriteBool(commandBuf, false)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	serializedCmd := commandBuf.Bytes()

	// command-payload-length and command-payload
	err = rw.WriteBytes(dbc.buf, serializedCmd)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// for synchronous commands the remaining content is an array of form:
	// [(synch-result-type:byte)[(synch-result-content:?)]]+
	// so the final value will by byte(0) to indicate the end of the array
	// and we must use a loop here

	for {
		resType, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return "", nil, oerror.NewTrace(err)
		}
		// This implementation assumes that SQLCommand can never have "supplementary records"
		// from an extended fetchPlan
		if resType == byte(0) {
			break
		}

		resultType := rune(resType)
		ogl.Debugf("resultType for SQLCommand: %v (%s)\n", resultType, string(rune(resultType)))

		if resultType == 'n' { // null result
			// do nothing - anything need to be done here?

		} else if resultType == 'r' { // single record
			doc, err := readSingleRecord(dbc)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

			ogl.Debugf("r>doc = %v\n", doc) // DEBUG
			if doc != nil {
				docs = make([]*oschema.ODocument, 1)
				docs[0] = doc
			}

		} else if resultType == 'l' { // collection of records
			ogl.Println("... resultType l")
			collectionDocs, err := readResultSet(dbc)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

			if docs == nil {
				docs = collectionDocs
			} else {
				docs = append(docs, collectionDocs...)
			}

		} else if resultType == 'a' { // serialized type
			serializedRec, err := rw.ReadBytes(dbc.conx)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}
			// TODO: for now I'm going to assume that this always just returns a string
			//       need a use case that violates this assumption
			retval = string(serializedRec)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

		} else {
			_, file, line, _ := runtime.Caller(0)
			// TODO: I've not yet tested this route of code -> how do so?
			ogl.Warnf(">> Got back resultType %v (%v): Not yet supported: line:%d; file:%s\n",
				resultType, string(rune(resultType)), line, file)
			// TODO: returning here is NOT the correct long-term behavior
			return "", nil, fmt.Errorf("Got back resultType %v (%v): Not yet supported: line:%d; file:%s\n",
				resultType, string(rune(resultType)), line, file)
		}
	}

	return retval, docs, err
}

//
// When called the "status byte" should have already been called
// Returns map where keys are RIDs (string) and values are ODocument objs
//
func readSupplementaryRecords(dbc *DBClient) (map[oschema.ORID]*oschema.ODocument, error) {
	mapRIDToDoc := make(map[oschema.ORID]*oschema.ODocument)
	for {
		doc, err := readSingleRecord(dbc)
		mapRIDToDoc[doc.RID] = doc

		status, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if status == byte(0) {
			break
		}
	}

	return mapRIDToDoc, nil
}

//
// When a fetchPlan returns additional (supplementary) records, it means that
// links can be resolved to point to the actual Document records, not just
// their RIDs.  This function resolves all link references that it can,
// updating the ODocument records referenced by the docs param and the Document
// records they link to.
//
// Params:
//  docs - the "primary" ODocuments returned from a query
//  mRIDsToDocs - a map of RID to ODocument for the "supplementary" records retrieved
//    with an extended fetch plan
//
func addSupplementaryRecsToPrimaryRecs(docs []*oschema.ODocument, mRIDsToDocs map[oschema.ORID]*oschema.ODocument) {
	// to resolve all link references, need to construct a new list with all docs
	// and add the primary docs to the mRIDsToDocs map
	allDocs := docs
	for _, doc := range mRIDsToDocs {
		allDocs = append(allDocs, doc)
	}

	for _, doc := range docs {
		mRIDsToDocs[doc.RID] = doc
	}

	// now we can fill in all the references (if present in mRIDsToDocs)
	for _, doc := range allDocs {
		for _, field := range doc.Fields {
			if field.Typ == oschema.LINK {
				lnk := field.Value.(*oschema.OLink)
				assignLinkRecord(lnk, mRIDsToDocs)

			} else if field.Typ == oschema.LINKLIST || field.Typ == oschema.LINKSET {
				lnklist := field.Value.([]*oschema.OLink)
				for _, lnk := range lnklist {
					assignLinkRecord(lnk, mRIDsToDocs)
				}
			} else if field.Typ == oschema.LINKMAP {
				lnkmap := field.Value.(map[string]*oschema.OLink)
				for _, lnk := range lnkmap {
					assignLinkRecord(lnk, mRIDsToDocs)
				}
			}
		}
	}
}

func assignLinkRecord(lnk *oschema.OLink, mRIDsToDocs map[oschema.ORID]*oschema.ODocument) {
	if lnk.Record == nil {
		if linkedDoc, ok := mRIDsToDocs[lnk.RID]; ok { // TODO: this snippet is repeated in all three cases -> DRY UP?
			lnk.Record = linkedDoc
		}
	}
}

// TODO: what datatypes can the params be? => right now allowing only string
func serializeSimpleSQLParams(dbc *DBClient, params []string) ([]byte, error) {
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
	// which in ogonori is a Field with:
	//   Field.Name = params
	//   Field.Value = {0=>paramVal1, 1=>paramVal2}} (map[string]interface{})

	paramsMap := oschema.NewEmbeddedMapWithCapacity(2)
	for i, pval := range params {
		paramsMap.Put(strconv.Itoa(i), pval, oschema.STRING)
	}
	doc.FieldWithType("params", paramsMap, oschema.EMBEDDEDMAP)

	ogl.Debugf("DOC XX: %v\n", doc)
	///////

	buf := new(bytes.Buffer)
	err := buf.WriteByte(dbc.serializationVersion)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	serde := dbc.RecordSerDes[int(dbc.serializationVersion)]
	err = serde.Serialize(doc, buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	ogl.Debugf("serialized params: %v\n", buf.Bytes())

	return buf.Bytes(), nil

	// ------------------------
	// final byte type = network.readByte();
	//  switch (type) {
	//  case 'n':
	//    result = null;
	//    break;

	//  case 'r':
	//    result = OChannelBinaryProtocol.readIdentifiable(network);
	//    if (result instanceof ORecord)
	//      database.getLocalCache().updateRecord((ORecord) result);
	//    break;

	//  case 'l':
	//    final int tot = network.readInt();
	//    final Collection<OIdentifiable> list = new ArrayList<OIdentifiable>(tot);
	//    for (int i = 0; i < tot; ++i) {
	//      final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
	//      if (resultItem instanceof ORecord)
	//        database.getLocalCache().updateRecord((ORecord) resultItem);
	//      list.add(resultItem);
	//    }
	//    result = list;
	//    break;

	//  case 'a':  // 'a' means "serialized result"
	//    final String value = new String(network.readBytes());
	//    result = ORecordSerializerStringAbstract.fieldTypeFromStream(null, ORecordSerializerStringAbstract.getType(value),
	//        value);
	//    break;

	//  default:
	//    OLogManager.instance().warn(this, "Received unexpected result from query: %d", type);
	//  }

	// return nil, nil
}

//
// readSingleRecord should be called to read a single record from the DBClient connection
// stream (from a db query/command).  In particular, this function should be called
// after the resultType has been read from the stream and resultType == 'r' (byte 114).
// When this is called the 'r' byte shown below should have already been read.  This
// function will then read everything else shown here - including the serialized record,
// but *NOT* including the byte after the serialized record (which is 0 to indicate
// End of Transmission).
//
//     Writing byte (1 byte): 114 [OChannelBinaryServer]   <- 'r' (type=single-record)
//     Writing short (2 bytes): 0 [OChannelBinaryServer]   <- 0=full record  (-2=null, -3=RID only)
//     Writing byte (1 byte): 100 [OChannelBinaryServer]   <- 'd'=document ('f'=flat data, 'b'=raw bytes)
//     Writing short (2 bytes): 11 [OChannelBinaryServer]  <- cluster-id  (RID part 1)
//     Writing long (8 bytes): 0 [OChannelBinaryServer]    <- cluster-pos (RID part 2)
//     Writing int (4 bytes): 1 [OChannelBinaryServer]     <- version
//     Writing bytes (4+26=30 bytes): [0, 14, 80, 97, 116, ... , 110, 107, 1] <- serialized record
//
// A new single ODocument pointer is returned.
//
// TODO: this method needs to determine how to handle 'f' (flat data) and 'b' (raw bytes)
//
func readSingleRecord(dbc *DBClient) (*oschema.ODocument, error) {
	var doc *oschema.ODocument
	resultType, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	if resultType == RecordNull { // null record
		// do nothing - return the zero values of the return types
		return nil, nil

	} else if resultType == RecordRID {
		orid, err := readRID(dbc)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		doc = oschema.NewDocument("")
		doc.RID = orid
		ogl.Warn(fmt.Sprintf("readSingleRecord :: Code path not seen before!!: SQLCommand resulted in RID: %s\n", orid))
		// TODO: would now load that record from the DB if the user (Go SQL API) wants it
		return doc, nil

	} else if resultType != int16(0) {
		_, file, line, _ := runtime.Caller(0)
		return nil, fmt.Errorf("Unexpected resultType in SQLCommand (file: %s; line %d): %d",
			file, line+1, resultType)
	}

	// if get here then have a full record, which can be in one of three formats:
	//  - "flat data"
	//  - "raw bytes"
	//  - "document"

	recType, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	if recType == byte('d') {
		return readSingleDocument(dbc)

	} else if recType == byte('f') {
		return readFlatDataRecord(dbc) // ???

	} else if recType == byte('b') {
		return readRawBytesRecord(dbc) // ???

	} else {
		_, file, line, _ := runtime.Caller(0)
		return nil, fmt.Errorf("Unexpected record type. Expected 'd', 'f' or 'b', but was %v (file: %s; line %d)",
			recType, file, line+1)
	}
}

//
// TODO: need example from server to know how to handle this
//
func readFlatDataRecord(dbc *DBClient) (*oschema.ODocument, error) {
	ogl.Warnf("flat record ('f') record type -> haven't seen that before. What is it?")
	return nil, errors.New("flat record ('f') record type -> haven't seen that before. What is it?")
}

//
// TODO: need example from server to know how to handle this
//
func readRawBytesRecord(dbc *DBClient) (*oschema.ODocument, error) {
	ogl.Warnf("raw bytes ('b') record type -> haven't seen that before. Send to Deserializer?")
	return nil, errors.New("raw bytes ('b') record type -> haven't seen that before. Send to Deserializer?")
}

//
// readSingleDocument is called by readSingleRecord when it has determined that the server
// has sent a docuemnt ('d'), not flat data ('f') or raw bytes ('b').
// It should be called *after* the single byte below on the first line has been already
// read and determined to be 'd'.  The rest the stream (NOT including the EOT byte) will
// be read.  The serialized document will be turned into an oschema.ODocument.
//
//     Writing byte (1 byte): 100 [OChannelBinaryServer]   <- 'd'=document ('f'=flat data, 'b'=raw bytes)
//     Writing short (2 bytes): 11 [OChannelBinaryServer]  <- cluster-id  (RID part 1)
//     Writing long (8 bytes): 0 [OChannelBinaryServer]    <- cluster-pos (RID part 2)
//     Writing int (4 bytes): 1 [OChannelBinaryServer]     <- version
//     Writing bytes (4+26=30 bytes): [0, 14, 80, 97, 116, ... , 110, 107, 1] <- serialized record
//
func readSingleDocument(dbc *DBClient) (*oschema.ODocument, error) {
	clusterId, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	clusterPos, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	recVersion, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	recBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	rid := oschema.ORID{ClusterID: clusterId, ClusterPos: clusterPos}
	doc, err := createDocumentFromBytes(rid, recVersion, recBytes, dbc)
	ogl.Debugf("::single record doc:::::: %v\n", doc)
	return doc, err
}

//
// readRID should be called when a single record (as opposed to a collection of
// records) is returned from a db query/command (REQUEST_COMMAND only ???).
// That is when the server sends back:
//     1) Writing byte (1 byte): 0 [OChannelBinaryServer]   -> SUCCESS
//     2) Writing int (4 bytes): 192 [OChannelBinaryServer] -> session-id
//     3) Writing byte (1 byte): 114 [OChannelBinaryServer] -> 'r'  (single record)
//     4) Writing short (2 bytes): 0 [OChannelBinaryServer] -> full record (not null, not RID only)
// Line 3 can be 'l' or possibly other things. For 'l' call readResultSet.
// Line 4 can be 0=full-record, -2=null, -3=RID only.  For -3, call readRID.  For 0, call this readSingleDocument.
//
func readRID(dbc *DBClient) (oschema.ORID, error) {
	// svr response: (-3:short)(cluster-id:short)(cluster-position:long)
	// TODO: impl me -> in the future this may need to call loadRecord for the RID and return the ODocument
	clusterID, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return oschema.NewORID(), oerror.NewTrace(err)
	}
	clusterPos, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return oschema.NewORID(), oerror.NewTrace(err)
	}

	return oschema.ORID{ClusterID: clusterID, ClusterPos: clusterPos}, nil
}

//
// readResultSet should be called for collections (resultType = 'l')
// from a SQLQuery call.
//
func readResultSet(dbc *DBClient) ([]*oschema.ODocument, error) {
	// for Collection
	// next val is: (collection-size:int)
	// and then each record is serialized according to format:
	// (0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)

	resultSetSize, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	rsize := int(resultSetSize)
	docs := make([]*oschema.ODocument, rsize)

	for i := 0; i < rsize; i++ {
		// TODO: move code below to readRecordInResultSet
		// this apparently should always be zero for serialized records -> not sure it's meaning
		zero, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if zero != int16(0) {
			return nil, fmt.Errorf("ERROR: readResultSet: expected short value of 0 but is %d", zero)
		}

		recType, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		// TODO: may need to check recType here => not sure that clusterId, clusterPos and version follow next if
		//       type is 'b' (raw bytes) or 'f' (flat record)
		//       see the readSingleDocument method (and probably call that one instead?)
		clusterId, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		clusterPos, err := rw.ReadLong(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		recVersion, err := rw.ReadInt(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if recType == byte('d') { // Document
			var doc *oschema.ODocument
			rid := oschema.ORID{ClusterID: clusterId, ClusterPos: clusterPos}
			recBytes, err := rw.ReadBytes(dbc.conx)
			if err != nil {
				return nil, oerror.NewTrace(err)
			}
			doc, err = createDocumentFromBytes(rid, recVersion, recBytes, dbc)
			if err != nil {
				return nil, oerror.NewTrace(err)
			}
			docs[i] = doc

		} else {
			_, file, line, _ := runtime.Caller(0)
			return nil, fmt.Errorf("%v: %v: Record type %v is not yet supported", file, line+1, recType)
		}
	} // end for loop

	// end, err := rw.ReadByte(dbc.conx)
	// if err != nil {
	// 	return nil, oerror.NewTrace(err)
	// }
	// if end != byte(0) {
	// 	return nil, fmt.Errorf("Final Byte read from collection result set was not 0, but was: %v", end)
	// }
	return docs, nil
}
