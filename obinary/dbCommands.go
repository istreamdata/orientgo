package obinary

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/binserde"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/istreamdata/orientgo/oschema"
	"io"
)

func (c *Client) openDBSess(dbname string, dbtype orient.DatabaseType, user, pass string) (sess *session, db *ODatabase, err error) {
	defer catch(&err)

	var (
		sessId int32
		//token []byte
		clusters   []OCluster
		clusterCfg []byte
		//serverVers string
	)
	err = c.root.sendCmd(requestDbOpen, func(w io.Writer) {
		rw.WriteStrings(w, driverName, driverVersion) // driver info
		rw.WriteShort(w, CurrentProtoVersion)         // protocol version
		rw.WriteNull(w)                               // client id (needed only for cluster config)
		rw.WriteString(w, c.serializer.Class())
		rw.WriteBool(w, false) // use token (true) or session (false)
		rw.WriteStrings(w, dbname, string(dbtype), user, pass)
	}, func(r io.Reader) {
		sessId = rw.ReadInt(r) // new session id
		_ = rw.ReadBytes(r)    // token - may ignore this in session mode (is nil)

		n := int(rw.ReadShort(r))
		clusters = make([]OCluster, n)
		for i := range clusters {
			name := rw.ReadString(r)
			id := rw.ReadShort(r)
			clusters[i] = OCluster{Name: name, Id: id}
		}
		clusterCfg = rw.ReadBytes(r)
		_ = rw.ReadString(r) // serverVers - unused, OrientDB release info
	})
	if err != nil {
		return nil, nil, err
	} else if sessId <= 0 {
		return nil, nil, fmt.Errorf("wrong session id returned: %d", sessId)
	}
	sess = c.newSess(sessId)
	db = NewDatabase(dbname, dbtype)
	db.Clusters = clusters
	db.ClustCfg = clusterCfg
	return sess, db, nil
}

type Database struct {
	sess *session
	db   *ODatabase
}

// OpenDatabase sends the REQUEST_DB_OPEN command to the OrientDb server to
// open the db in read/write mode.  The database name and type are required, plus
// username and password.  Database type should be one of the obinary constants:
// DocumentDbType or GraphDbType.
func (c *Client) OpenDatabase(dbname string, dbtype orient.DatabaseType, user, pass string) (db *Database, err error) {
	defer catch(&err)
	// TODO: close previous DB? will connection drop in this case?
	var (
		sess *session
		odb  *ODatabase
	)
	sess, odb, err = c.openDBSess(dbname, dbtype, user, pass)
	if err != nil {
		return nil, err
	}
	db = &Database{sess: sess, db: odb}
	c.currmu.Lock()
	c.currdb = db
	c.currmu.Unlock()
	err = db.refreshGlobalProperties()
	return db, err
}

func (c *Client) Open(dbname string, dbtype orient.DatabaseType, user, pass string) (orient.DBSession, error) {
	return c.OpenDatabase(dbname, dbtype, user, pass)
}

// loadConfigRecord loads record #0:0 for the current database, caching
// some of the information returned into OStorageConfiguration
func (db *Database) loadConfigRecord() (oschemaRID oschema.RID, err error) {
	defer catch(&err)
	// The config record comes back as type 'b' (raw bytes), which should
	// just be converted to a string then tokenized by the pipe char
	var recs orient.Records
	rid := oschema.RID{ClusterID: 0, ClusterPos: 0}
	recs, err = db.GetRecordByRID(rid, "*:-1 index:0", true, true) // based on Java client code
	if err != nil {
		return
	}

	var rec orient.Record
	rec, err = recs.One()
	if err != nil {
		return
	}
	raw, ok := rec.(RawRecord)
	if !ok {
		err = fmt.Errorf("expected raw record for config")
		return
	} else if s := string([]byte(raw)); s == "" {
		err = fmt.Errorf("config record is empty")
		return
	} else if err = parseConfigRecord(db.db, s); err != nil {
		err = fmt.Errorf("config parse error: %s", err)
		return
	}
	oschemaRID = db.db.StorageCfg.schemaRID
	return oschemaRID, err
}

// parseConfigRecord takes the pipe-separate values that comes back
// from reading record #0:0 and turns it into an OStorageConfiguration
// object, which it adds to the db database object.
// TODO: move this function to be a method of OStorageConfiguration?
func parseConfigRecord(db *ODatabase, psvData string) error {
	sc := OStorageConfiguration{}

	toks := strings.Split(psvData, "|")

	version, err := strconv.ParseInt(toks[0], 10, 8)
	if err != nil {
		return err
	}

	sc.version = byte(version)
	sc.name = strings.TrimSpace(toks[1])
	sc.schemaRID = oschema.MustParseRID(toks[2])
	sc.dictionaryRID = strings.TrimSpace(toks[3])
	sc.idxMgrRID = oschema.MustParseRID(toks[4])
	sc.localeLang = strings.TrimSpace(toks[5])
	sc.localeCountry = strings.TrimSpace(toks[6])
	sc.dateFmt = strings.TrimSpace(toks[7])
	sc.dateTimeFmt = strings.TrimSpace(toks[8])
	sc.timezone = strings.TrimSpace(toks[9])

	db.StorageCfg = sc

	return nil
}

// loadSchema loads record #0:1 for the current database, caching the
// SchemaVersion, GlobalProperties and Classes info in the current ODatabase
// object (dbc.currDb).
func (db *Database) loadSchema(rid oschema.RID) error {
	recs, err := db.GetRecordByRID(rid, "*:-1 index:0", true, false) // TODO: GetRecordByRIDIfChanged
	if err != nil {
		return err
	}
	rec, err := recs.One()
	if err != nil {
		return err
	}
	var doc *oschema.ODocument
	if err = rec.Deserialize(&doc); err != nil {
		return err
	}

	odb := db.db

	// ---[ schemaVersion ]---
	odb.SchemaVersion = doc.GetField("schemaVersion").Value.(int32)

	// ---[ globalProperties ]---
	globalPropsFld := doc.GetField("globalProperties")

	var globalProperty oschema.OGlobalProperty
	for _, pfield := range globalPropsFld.Value.([]interface{}) {
		pdoc := pfield.(*oschema.ODocument)
		globalProperty = oschema.NewGlobalPropertyFromDocument(pdoc)
		odb.SetGlobalProperty(int(globalProperty.Id), globalProperty)
	}

	// ---[ classes ]---
	var oclass *oschema.OClass
	classesFld := doc.GetField("classes")
	for _, cfield := range classesFld.Value.([]interface{}) {
		cdoc := cfield.(*oschema.ODocument)
		oclass = oschema.NewOClassFromDocument(cdoc)
		odb.Classes[oclass.Name] = oclass
	}
	return nil
}

// CloseDatabase closes down a session with a specific database that
// has already been opened (via OpenDatabase). This should be called
// when exiting an app or before starting a connection to a different
// OrientDB database.
func (db *Database) Close() (err error) {
	defer catch(&err)
	if db == nil || db.db == nil || db.sess == nil {
		return
	}
	err = db.sess.sendCmd(requestDbClose, nil, nil)
	db.sess.cli.closeSess(db.sess.id, db)
	db.sess = nil
	db.db = nil
	return
}

// FetchDatabaseSize retrieves the size of the current database in bytes.
// It is a database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
func (db *Database) Size() (int64, error) {
	return db.getLongFromDB(requestDbSIZE)
}

// FetchNumRecordsInDatabase retrieves the number of records of the current
// database. It is a database-level operation, so OpenDatabase must have
// already been called first in order to start a session with the database.
func (db *Database) CountRecords() (int64, error) {
	return db.getLongFromDB(requestDbCOUNTRECORDS)
}

// DeleteRecordByRID deletes a record specified by its RID and its version.
//
// If nil is returned, delete succeeded.
// If error is returned, delete request was either never issued, or there was
// a problem on the server end or the record did not exist in the database.
func (db *Database) DeleteRecordByRID(rid oschema.RID, recVersion int32) error {
	var status byte
	err := db.sess.sendCmd(requestRecordDELETE, func(w io.Writer) {
		rw.WriteShort(w, rid.ClusterID)
		rw.WriteLong(w, rid.ClusterPos)
		rw.WriteInt(w, recVersion)
		rw.WriteByte(w, 0) // sync mode ; 0 = synchronous; 1 = asynchronous
	}, func(r io.Reader) {
		status = rw.ReadByte(r)
	})
	if err != nil {
		return err
	}
	// status 1 means record was deleted;
	// status 0 means record was not deleted (either failed or didn't exist)
	if status == byte(0) {
		return fmt.Errorf("Record %s was not deleted. Either failed or did not exist.", rid)
	}
	return nil
}

// GetRecordByRID takes an RID and reads that record from the database.
//
// TODO: need to properly handle fetchPlan
// ignoreCache = true
// loadTombstones = false
func (db *Database) GetRecordByRID(rid oschema.RID, fetchPlan string, ignoreCache, loadTombstones bool) (recs orient.Records, err error) {
	err = db.sess.sendCmd(requestRecordLOAD, func(w io.Writer) {
		rw.WriteShort(w, rid.ClusterID)
		rw.WriteLong(w, rid.ClusterPos)
		rw.WriteString(w, fetchPlan)
		rw.WriteBool(w, ignoreCache)
		rw.WriteBool(w, loadTombstones)
	}, func(r io.Reader) {
		// TODO: this query can return multiple records (supplementary only?)
		recs = make(orient.Records, 0, 1)
		for {
			status := rw.ReadByte(r)
			if status == byte(0) {
				break
			}
			recType := rw.ReadByte(r)
			switch tp := rune(recType); tp {
			case 'd':
				//recs = append(recs, db.readSingleDocument(r))
				recVersion := rw.ReadInt(r)
				recBytes := rw.ReadBytes(r)
				recs = append(recs, &RecordData{
					RID:     rid,
					Version: recVersion,
					Data:    recBytes,
					db:      db,
				})
			case 'b':
				_ = rw.ReadInt(r) // record version
				data := rw.ReadBytes(r)
				recs = append(recs, RawRecord(data))
			default:
				panic(ErrBrokenProtocol{fmt.Errorf("GetRecordByRID: unknown record type: %d(%s)", tp, string(tp))})
			}
		}
	})
	return recs, err
}

// ReloadSchema should be called after a schema is altered, such as properties
// added, deleted or renamed.
func (db *Database) ReloadSchema() error {
	return db.loadSchema(oschema.RID{ClusterID: 0, ClusterPos: 1})
}

// FetchClusterDataRange returns the range of record ids for a cluster
func (db *Database) GetClusterDataRange(clusterName string) (begin, end int64, err error) {
	var clusterID int16
	clusterID, err = db.findClusterWithName(clusterName)
	if err != nil {
		return
	}
	err = db.sess.sendCmd(requestDataClusterDATARANGE, func(w io.Writer) {
		rw.WriteShort(w, clusterID)
	}, func(r io.Reader) {
		begin = rw.ReadLong(r)
		end = rw.ReadLong(r)
	})
	return begin, end, err
}

// AddCluster adds a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// The clusterID is returned if the command is successful.
func (db *Database) AddCluster(name string) (clusterID int16, err error) {
	name = strings.ToLower(name)
	err = db.sess.sendCmd(requestDataClusterADD, func(w io.Writer) {
		rw.WriteString(w, name)
		rw.WriteShort(w, -1) // -1 means generate new cluster id
	}, func(r io.Reader) {
		clusterID = rw.ReadShort(r)
	})
	if err == nil {
		db.db.Clusters = append(db.db.Clusters, OCluster{name, clusterID})
	}
	return clusterID, err
}

// DropCluster drops a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// If nil is returned, then the action succeeded.
func (db *Database) DropCluster(clusterName string) error {
	clusterID, err := db.findClusterWithName(clusterName)
	if err != nil {
		return err
	}
	var status byte
	err = db.sess.sendCmd(requestDataClusterDROP, func(w io.Writer) {
		rw.WriteShort(w, clusterID)
	}, func(r io.Reader) {
		status = rw.ReadByte(r)
	})
	if err == nil && status != byte(1) {
		err = fmt.Errorf("Drop cluster failed. Return code: %d.", status)
	}
	return err
}

// FetchEntriesOfRemoteLinkBag fills in the links of an OLinkBag that is remote
// (tree-based) rather than embedded.  This function will fill in the links
// of the passed in OLinkBag, rather than returning the new links. The Links
// will have RIDs only, not full Records (ODocuments).  If you then want the
// Records filled in, call the ResolveLinks function.
func (db *Database) GetEntriesOfRemoteLinkBag(linkBag *oschema.OLinkBag, inclusive bool) (err error) {
	defer catch(&err)
	var (
		firstLink *oschema.OLink
		linkSerde = binserde.OLinkSerializer{}
	)
	firstLink, err = db.GetFirstKeyOfRemoteLinkBag(linkBag)
	if err != nil {
		return err
	}
	linkBytes, err := linkSerde.Serialize(firstLink)
	if err != nil {
		return err
	}

	var linkEntryBytes []byte
	err = db.sess.sendCmd(requestSBTREE_BONSAI_GET_ENTRIES_MAJOR, func(w io.Writer) {
		writeLinkBagCollectionPointer(w, linkBag)
		rw.WriteBytes(w, linkBytes)
		rw.WriteBool(w, inclusive)
		rw.WriteInt(w, 128) // if protoVers >= 21 from Java client OSBTreeBonsaiRemote#fetchEntriesMajor
	}, func(r io.Reader) {
		linkEntryBytes = rw.ReadBytes(r)
	})
	if err != nil {
		return
	}
	r := bytes.NewReader(linkEntryBytes)
	n := int(rw.ReadInt(r))
	var lnk *oschema.OLink
	for i := 0; i < n; i++ { // loop over all the serialized links
		lnk, err = linkSerde.DeserializeLink(r)
		if err != nil {
			return err
		}
		linkBag.AddLink(lnk)

		// FIXME: for some reason the server returns a serialized link
		//        followed by an integer (so far always a 1 in my expts).
		//        Not sure what to do with this int, so ignore for now
		intval := rw.ReadInt(r)

		if intval != int32(1) {
			glog.Warningf("Found a use case where the val pair of a link was not 1: %d", intval)
		}
	}

	return nil
}

// FetchFirstKeyOfRemoteLinkBag is the entry point for retrieving links from
// a remote server-side side LinkBag.  In general, this method should not be
// called by end users. Instead, end users should call FetchEntriesOfRemoteLinkBag
//
// TODO: make this an unexported func?
func (db *Database) GetFirstKeyOfRemoteLinkBag(linkBag *oschema.OLinkBag) (lnk *oschema.OLink, err error) {
	defer catch(&err)

	var firstKeyBytes []byte
	err = db.sess.sendCmd(requestSBTREE_BONSAI_FIRST_KEY, func(w io.Writer) {
		writeLinkBagCollectionPointer(w, linkBag)
	}, func(r io.Reader) {
		firstKeyBytes = rw.ReadBytes(r)
	})
	if err != nil {
		return
	}
	r := bytes.NewReader(firstKeyBytes)
	typeByte := rw.ReadByte(r)
	if typeByte != binserde.LinkSerializer {
		err = fmt.Errorf("GetFirstKeyOfRemoteLinkBag: unknown entry type: %d", typeByte)
		return
	}
	return binserde.OLinkSerializer{}.DeserializeLink(r)
}

func writeLinkBagCollectionPointer(w io.Writer, linkBag *oschema.OLinkBag) {
	// (treePointer:collectionPointer)(changes)
	// where collectionPtr = (fileId:long)(pageIndex:long)(pageOffset:int)
	rw.WriteLong(w, linkBag.GetFileID())
	rw.WriteLong(w, linkBag.GetPageIndex())
	rw.WriteInt(w, linkBag.GetPageOffset())
}

// ResolveLinks iterates over all the OLinks passed in and does a
// FetchRecordByRID for each one that has a null Record.
// TODO: maybe include a fetchplan here?
// TODO: remove it from obinary
func (db *Database) ResolveLinks(links []*oschema.OLink) error {
	fetchPlan := ""
	for i := 0; i < len(links); i++ {
		if links[i].Record == nil {
			recs, err := db.GetRecordByRID(links[i].RID, fetchPlan, true, false)
			if err != nil {
				return err
			}
			docs, err := recs.AsDocuments()
			if err != nil {
				return err
			} else if len(docs) != 1 {
				glog.Warningf("More than one record returned from GetRecordByRID. Please report this use case!")
			}
			links[i].Record = docs[0]
		}
	}
	return nil
}

// Large LinkBags (aka RidBags) are stored on the server. To look up their
// size requires a call to the database.  The size is returned.  Note that the
// Size field of the linkBag is NOT updated.  That is left for the caller to
// decide whether to do.
func (db *Database) GetSizeOfRemoteLinkBag(linkBag *oschema.OLinkBag) (val int, err error) {
	err = db.sess.sendCmd(requestRIDBAG_GET_SIZE, func(w io.Writer) {
		writeLinkBagCollectionPointer(w, linkBag)
		rw.WriteBytes(w, []byte{0, 0, 0, 0}) // changes => TODO: right now not supporting any change -> just writing empty changes
	}, func(r io.Reader) {
		val = int(rw.ReadInt(r))
	})
	return
}

// ClustersCount gets the number of records in all the clusters specified.
func (db *Database) ClustersCount(withDeleted bool, clusterNames ...string) (val int64, err error) {
	clusterIDs := make([]int16, len(clusterNames))
	for i, name := range clusterNames {
		clusterID, err := db.findClusterWithName(name)
		if err != nil {
			return 0, err
		}
		clusterIDs[i] = clusterID
	}
	err = db.sess.sendCmd(requestDataClusterCOUNT, func(w io.Writer) {
		rw.WriteShort(w, int16(len(clusterIDs)))
		for _, id := range clusterIDs {
			rw.WriteShort(w, id)
		}
		rw.WriteBool(w, withDeleted)
	}, func(r io.Reader) {
		val = rw.ReadLong(r)
	})
	return
}

func (db *Database) getLongFromDB(cmd byte) (val int64, err error) {
	val = -1
	err = db.sess.sendCmd(cmd, func(w io.Writer) {
		// nothing extra to send
	}, func(r io.Reader) {
		val = rw.ReadLong(r)
	})
	return
}

// Returns negative number if no cluster with `name` is found in the clusters slice.
func (db *Database) findClusterWithName(name string) (int16, error) {
	name = strings.ToLower(name)
	var id int16 = -1
	for _, cluster := range db.db.Clusters {
		if cluster.Name == name {
			return cluster.Id, nil
		}
	}
	if id < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a FetchClusterCountById(dbc, clusterID)
		return id, fmt.Errorf("fixme: No cluster with name %s is known in database %s", name, db.db.Name)
	}
	return id, nil
}
