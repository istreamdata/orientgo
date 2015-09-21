package obinary

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/obinary/rw"
)

func (c *Client) sendClientInfo(w *rw.Writer) {
	if c.curProtoVers >= ProtoVersion7 {
		w.WriteStrings(driverName, driverVersion) // driver info
		w.WriteShort(int16(c.curProtoVers))       // protocol version
		w.WriteNull()                             // client id (needed only for cluster config)
	}
	if c.curProtoVers > ProtoVersion21 {
		w.WriteString(c.recordFormat.String())
	} else {
		panic("CSV serializer is not supported")
	}
	if c.curProtoVers > ProtoVersion26 {
		w.WriteBool(false) // use token (true) or session (false)
	}
}

func (c *Client) openDBSess(dbname string, dbtype orient.DatabaseType, user, pass string) (*session, *ODatabase, error) {
	var (
		sessId int32
		//token []byte
		clusters   []OCluster
		clusterCfg []byte
		//serverVers string
	)
	err := c.root.sendCmd(requestDbOpen, func(w *rw.Writer) error {
		c.sendClientInfo(w)

		w.WriteString(dbname)
		if c.curProtoVers >= ProtoVersion8 {
			w.WriteString(string(dbtype))
		}
		w.WriteString(user)
		w.WriteString(pass)
		return w.Err()
	}, func(r *rw.Reader) error {
		sessId = r.ReadInt() // new session id
		_ = r.ReadBytes()    // token - may ignore this in session mode (is nil)

		n := int(r.ReadShort())
		clusters = make([]OCluster, n)
		for i := range clusters {
			name := r.ReadString()
			id := r.ReadShort()
			clusters[i] = OCluster{Name: name, Id: id}
		}
		clusterCfg = r.ReadBytes()
		_ = r.ReadString() // serverVers - unused, OrientDB release info
		return r.Err()
	})
	if err != nil {
		return nil, nil, err
	} else if sessId <= 0 {
		return nil, nil, fmt.Errorf("wrong session id returned: %d", sessId)
	}
	sess := c.newSess(sessId)
	db := NewDatabase(dbname, dbtype)
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
	c.recordFormat.SetGlobalPropertyFunc(func(id int) (orient.OGlobalProperty, bool) {
		// TODO: implement global property lookup
		db.refreshGlobalPropertiesIfRequired(id)
		return db.db.GetGlobalProperty(id)
	})
	return db, err
}

func (c *Client) Open(dbname string, dbtype orient.DatabaseType, user, pass string) (orient.DBSession, error) {
	return c.OpenDatabase(dbname, dbtype, user, pass)
}

// refreshGlobalPropertiesIfRequired iterates through all the fields
// of the binserde header. If any of the fieldIds are NOT in the GlobalProperties
// map of the current ODatabase object, then the GlobalProperties are
// stale and need to be refresh (this likely means CREATE PROPERTY statements
// were recently issued).
//
// If the GlobalProperties data is stale, then it must be refreshed, so
// refreshGlobalProperties is called.
func (db *Database) refreshGlobalPropertiesIfRequired(id int) error {
	if db == nil || db.db == nil {
		return nil
	}
	if _, ok := db.db.GetGlobalProperty(id); !ok {
		return db.refreshGlobalProperties()
	}
	return nil
}

// refreshGlobalProperties is called when it is discovered,
// while in the middle of reading the response from the OrientDB
// server, that the GlobalProperties are stale.
func (db *Database) refreshGlobalProperties() error {
	// ---[ load #0:0 - config record ]---
	oschemaRID, err := db.loadConfigRecord()
	if err != nil {
		return err
	}
	// ---[ load #0:1 - oschema record ]---
	err = db.loadSchema(oschemaRID)
	if err != nil {
		return err
	}
	return nil
}

// loadConfigRecord loads record #0:0 for the current database, caching
// some of the information returned into OStorageConfiguration
func (db *Database) loadConfigRecord() (orient.RID, error) {
	// The config record comes back as type 'b' (raw bytes), which should
	// just be converted to a string then tokenized by the pipe char
	rid := orient.RID{ClusterID: 0, ClusterPos: 0}
	rec, err := db.GetRecordByRID(rid, "*:-1 index:0", true) // based on Java client code
	if err != nil {
		return orient.NewEmptyRID(), err
	}
	raw, ok := rec.(*orient.BytesRecord)
	if !ok {
		return orient.NewEmptyRID(), fmt.Errorf("expected raw record for config, got %T", rec)
	} else if len(raw.Data) == 0 {
		return orient.NewEmptyRID(), fmt.Errorf("config record is empty")
	}
	sc := &OStorageConfiguration{}
	if err = sc.parse(string(raw.Data)); err != nil {
		return orient.NewEmptyRID(), fmt.Errorf("config parse error: %s", err)
	}
	db.db.storageMu.Lock()
	db.db.StorageCfg = *sc
	db.db.storageMu.Unlock()
	return sc.schemaRID, nil
}

// loadSchema loads record #0:1 for the current database, caching the
// SchemaVersion, GlobalProperties and Classes info in the current ODatabase
// object (dbc.currDb).
func (db *Database) loadSchema(rid orient.RID) error {
	rec, err := db.GetRecordByRID(rid, "*:-1 index:0", true) // TODO: GetRecordByRIDIfChanged
	if err != nil {
		return err
	}

	drec, ok := rec.(*orient.DocumentRecord)
	if !ok {
		return fmt.Errorf("expected document record for schema, got %T", rec)
	}
	doc, err := drec.ToDocument()
	if err != nil {
		return fmt.Errorf("cannot read document record for schema: %s", err)
	}

	odb := db.db

	// ---[ schemaVersion ]---
	odb.SchemaVersion = int(doc.GetField("schemaVersion").Value.(int32))

	// ---[ globalProperties ]---
	globalPropsFld := doc.GetField("globalProperties")

	var globalProperty orient.OGlobalProperty
	for _, pfield := range globalPropsFld.Value.([]interface{}) {
		pdoc := pfield.(*orient.Document)
		globalProperty = orient.NewGlobalPropertyFromDocument(pdoc)
		odb.SetGlobalProperty(int(globalProperty.Id), globalProperty)
	}

	// ---[ classes ]---
	var oclass *orient.OClass
	classesFld := doc.GetField("classes")
	for _, cfield := range classesFld.Value.([]interface{}) {
		cdoc := cfield.(*orient.Document)
		oclass = orient.NewOClassFromDocument(cdoc)
		odb.Classes[oclass.Name] = oclass
	}
	return nil
}

// CloseDatabase closes down a session with a specific database that
// has already been opened (via OpenDatabase). This should be called
// when exiting an app or before starting a connection to a different
// OrientDB database.
func (db *Database) Close() error {
	if db == nil || db.db == nil || db.sess == nil {
		return nil
	}
	err := db.sess.sendCmd(requestDbClose, nil, nil)
	db.sess.cli.closeSess(db.sess.id, db)
	db.sess = nil
	db.db = nil
	return err
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
func (db *Database) DeleteRecordByRID(rid orient.RID, recVersion int) error {
	var status byte
	err := db.sess.sendCmd(requestRecordDELETE, func(w *rw.Writer) error {
		if err := rid.ToStream(w); err != nil {
			return err
		}
		w.WriteInt(int32(recVersion))
		w.WriteByte(0) // sync mode ; 0 = synchronous; 1 = asynchronous
		return w.Err()
	}, func(r *rw.Reader) error {
		status = r.ReadByte()
		return r.Err()
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
// ignoreCache = true
func (db *Database) GetRecordByRID(rid orient.RID, fetchPlan orient.FetchPlan, ignoreCache bool) (rec orient.ORecord, err error) {
	err = db.sess.sendCmd(requestRecordLOAD, func(w *rw.Writer) error {
		if err := rid.ToStream(w); err != nil {
			return err
		}
		w.WriteString(string(fetchPlan))
		if db.sess.cli.curProtoVers >= ProtoVersion9 {
			w.WriteBool(ignoreCache)
		}
		if db.sess.cli.curProtoVers >= ProtoVersion13 {
			w.WriteBool(false)
		}
		return w.Err()
	}, func(r *rw.Reader) error {
		if r.ReadByte() == 0 {
			return r.Err()
		}
		var (
			content []byte
			version int
			recType orient.RecordType
		)
		if db.sess.cli.curProtoVers <= ProtoVersion27 {
			content = r.ReadBytes()
			version = int(r.ReadInt())
			recType = orient.RecordType(r.ReadByte())
		} else {
			recType = orient.RecordType(r.ReadByte())
			version = int(r.ReadInt())
			content = r.ReadBytes()
		}
		if err := r.Err(); err != nil {
			return err
		}
		rec = orient.NewRecordOfType(recType)
		switch rc := rec.(type) {
		case *orient.DocumentRecord:
			rc.SetSerializer(db.sess.cli.recordFormat)
		}
		if err := rec.Fill(rid, version, content); err != nil {
			return err
		}
		for {
			status := r.ReadByte()
			if status != 2 {
				break
			}
			rec, err := db.readIdentifiable(r)
			if err != nil {
				return err
			}
			db.updateCachedRecord(rec) // .(orient.ORecord)
		}
		return r.Err()
	})
	return rec, err
}

// ReloadSchema should be called after a schema is altered, such as properties
// added, deleted or renamed.
func (db *Database) ReloadSchema() error {
	return db.loadSchema(orient.RID{ClusterID: 0, ClusterPos: 1})
}

// FetchClusterDataRange returns the range of record ids for a cluster
func (db *Database) GetClusterDataRange(clusterName string) (begin, end int64, err error) {
	var clusterID int16
	clusterID, err = db.findClusterWithName(clusterName)
	if err != nil {
		return
	}
	err = db.sess.sendCmd(requestDataClusterDATARANGE, func(w *rw.Writer) error {
		return w.WriteShort(clusterID)
	}, func(r *rw.Reader) error {
		begin = r.ReadLong()
		end = r.ReadLong()
		return r.Err()
	})
	return begin, end, err
}

// AddCluster adds a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// The clusterID is returned if the command is successful.
func (db *Database) AddCluster(name string) (clusterID int16, err error) {
	name = strings.ToLower(name)
	err = db.sess.sendCmd(requestDataClusterADD, func(w *rw.Writer) error {
		w.WriteString(name)
		w.WriteShort(-1) // -1 means generate new cluster id
		return w.Err()
	}, func(r *rw.Reader) error {
		clusterID = r.ReadShort()
		return r.Err()
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
	err = db.sess.sendCmd(requestDataClusterDROP, func(w *rw.Writer) error {
		return w.WriteShort(clusterID)
	}, func(r *rw.Reader) error {
		status = r.ReadByte()
		return r.Err()
	})
	if err == nil && status != byte(1) {
		err = fmt.Errorf("Drop cluster failed. Return code: %d.", status)
	}
	return err
}

/*
// FetchEntriesOfRemoteLinkBag fills in the links of an OLinkBag that is remote
// (tree-based) rather than embedded.  This function will fill in the links
// of the passed in OLinkBag, rather than returning the new links. The Links
// will have RIDs only, not full Records (Documents).  If you then want the
// Records filled in, call the ResolveLinks function.
func (db *Database) GetEntriesOfRemoteLinkBag(linkBag *orient.OLinkBag, inclusive bool) (err error) {
	defer catch(&err)
	var (
		firstLink orient.OIdentifiable
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
	var lnk orient.OIdentifiable
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
func (db *Database) GetFirstKeyOfRemoteLinkBag(linkBag *orient.OLinkBag) (lnk orient.OIdentifiable, err error) {
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

func writeLinkBagCollectionPointer(w io.Writer, linkBag *orient.OLinkBag) {
	// (treePointer:collectionPointer)(changes)
	// where collectionPtr = (fileId:long)(pageIndex:long)(pageOffset:int)
	rw.WriteLong(w, linkBag.GetFileID())
	rw.WriteLong(w, linkBag.GetPageIndex())
	rw.WriteInt(w, linkBag.GetPageOffset())
}

// Large LinkBags (aka RidBags) are stored on the server. To look up their
// size requires a call to the database.  The size is returned.  Note that the
// Size field of the linkBag is NOT updated.  That is left for the caller to
// decide whether to do.
func (db *Database) GetSizeOfRemoteLinkBag(linkBag *orient.OLinkBag) (val int, err error) {
	err = db.sess.sendCmd(requestRIDBAG_GET_SIZE, func(w io.Writer) {
		writeLinkBagCollectionPointer(w, linkBag)
		rw.WriteBytes(w, []byte{0, 0, 0, 0}) // changes => TODO: right now not supporting any change -> just writing empty changes
	}, func(r io.Reader) {
		val = int(rw.ReadInt(r))
	})
	return
}
*/

// ResolveLinks iterates over all the OLinks passed in and does a
// FetchRecordByRID for each one that has a null Record.
// TODO: maybe include a fetchplan here?
// TODO: remove it from obinary
func (db *Database) ResolveLinks(links []orient.OIdentifiable) error {
	fetchPlan := orient.FetchPlan("")
	for i := 0; i < len(links); i++ {
		if links[i].GetRecord() == nil {
			rec, err := db.GetRecordByRID(links[i].GetIdentity(), fetchPlan, true)
			if err != nil {
				return err
			}
			panic(fmt.Errorf("resolve links record type: %T", rec))
			/*
				docs, err := recs.AsDocuments()
				if err != nil {
					return err
				} else if len(docs) != 1 {
					glog.Warningf("More than one record returned from GetRecordByRID. Please report this use case!")
				}
				links[i].Record = docs[0]*/
		}
	}
	return nil
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
	err = db.sess.sendCmd(requestDataClusterCOUNT, func(w *rw.Writer) error {
		w.WriteShort(int16(len(clusterIDs)))
		for _, id := range clusterIDs {
			w.WriteShort(id)
		}
		w.WriteBool(withDeleted)
		return w.Err()
	}, func(r *rw.Reader) error {
		val = r.ReadLong()
		return r.Err()
	})
	return
}

func (db *Database) getLongFromDB(cmd byte) (val int64, err error) {
	val = -1
	err = db.sess.sendCmd(cmd, nil, func(r *rw.Reader) error {
		val = r.ReadLong()
		return r.Err()
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

// Use this to create a new record in the OrientDB database
// you are currently connected to.
// Does REQUEST_RECORD_CREATE OrientDB cmd (binary network protocol).
func (db *Database) CreateRecord(doc *orient.Document) (err error) {
	if doc.Classname == "" {
		return errors.New("classname must be present on Document to call CreateRecord")
	}
	clusterID := int16(-1) // indicates new class/cluster
	oclass, ok := db.db.Classes[doc.Classname]
	if ok {
		clusterID = int16(oclass.DefaultClusterId) // TODO: need way to allow user to specify a non-default cluster
	}
	serde := db.serializer()

	rbuf := bytes.NewBuffer(nil)
	if err = serde.ToStream(rbuf, doc); err != nil {
		return
	}

	err = db.sess.sendCmd(requestRecordCREATE, func(w *rw.Writer) error {
		w.WriteShort(clusterID)
		w.WriteBytes(rbuf.Bytes())
		w.WriteByte(byte('d')) // document record-type
		w.WriteByte(byte(0))   // synchronous mode indicator
		return w.Err()
	}, func(r *rw.Reader) error {
		if err = doc.RID.FromStream(r); err != nil {
			panic(err)
		}
		doc.Version = int(r.ReadInt())
		nCollChanges := r.ReadInt()
		if nCollChanges != 0 {
			panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
		}
		return r.Err()
	})
	// In the Java client, they now a 'select from XXX' at this point -> would that be useful here?
	return
}

// UpdateRecord should be used update an existing record in the OrientDB database.
// It does the REQUEST_RECORD_UPDATE OrientDB cmd (network binary protocol)
func (db *Database) UpdateRecord(doc *orient.Document) (err error) {
	if doc == nil {
		return fmt.Errorf("document is nil")
	} else if doc.RID.ClusterID < 0 || doc.RID.ClusterPos < 0 {
		return fmt.Errorf("document is not updateable - has negative RID values")
	}
	ser := db.serializer()
	rbuf := bytes.NewBuffer(nil)
	if err = ser.ToStream(rbuf, doc); err != nil {
		return
	}
	return db.sess.sendCmd(requestRecordUPDATE, func(w *rw.Writer) error {
		if err := doc.RID.ToStream(w); err != nil {
			return err
		}
		w.WriteBool(true) // update-content flag
		w.WriteBytes(rbuf.Bytes())
		w.WriteInt(int32(doc.Version)) // record version
		w.WriteByte(byte('d'))         // record-type: document // TODO: how support 'b' (raw bytes) & 'f' (flat data)?
		w.WriteByte(0)                 // mode: synchronous
		return w.Err()
	}, func(r *rw.Reader) error {
		doc.Version = int(r.ReadInt())
		nCollChanges := r.ReadInt()
		if nCollChanges != 0 {
			// if > 0, then have to deal with RidBag mgmt:
			// [(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]
			panic("CreateRecord: Found case where number-collection-changes is not zero -> log case and impl code to handle")
		}
		return r.Err()
	})
}
