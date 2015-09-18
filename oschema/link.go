package oschema

import (
	"fmt"
	"github.com/istreamdata/orientgo/obinary/rw"
	"github.com/nu7hatch/gouuid"
	"io"
)

type OIdentifiableCollection interface {
	Len() int
	OIdentifiableIterator() <-chan OIdentifiable
}

func NewRidBag() *RidBag {
	return &RidBag{delegate: newEmbeddedRidBag()}
}

// RidBag can have a tree-based or an embedded representation.
//
// Embedded stores its content directly to the document that owns it.
// It is used when only small numbers of links are stored in the bag.
//
// The tree-based implementation stores its content in a separate data
// structure called on OSBTreeBonsai on the server. It fits great for cases
// when you have a large number of links.  This is used to efficiently
// manage relationships (particularly in graph databases).
//
// The RidBag struct corresponds to ORidBag in Java client codebase.
type RidBag struct {
	id       uuid.UUID
	delegate ridBagDelegate
	owner    *ODocument
}

func (bag *RidBag) SetOwner(doc *ODocument) {
	bag.owner = doc
}
func (bag *RidBag) FromStream(r io.Reader) (err error) {
	defer catch(&err)
	first := rw.ReadByte(r)
	if first&0x1 != 0 {
		bag.delegate = newEmbeddedRidBag()
	} else {
		bag.delegate = newSBTreeRidBag()
	}
	if first&0x2 != 0 {
		rw.ReadRawBytes(r, bag.id[:])
	}
	return bag.delegate.deserializeDelegate(r)
}
func (bag *RidBag) ToStream(w io.Writer) error {
	var first byte
	hasUUID := false // TODO: do we need to send it?
	if !bag.IsRemote() {
		first |= 0x1
	}
	if hasUUID {
		first |= 0x2
	}
	rw.WriteByte(w, first)
	if hasUUID {
		rw.WriteRawBytes(w, bag.id[:])
	}
	return bag.delegate.serializeDelegate(w)
}
func (bag *RidBag) IsRemote() bool {
	switch bag.delegate.(type) {
	case *sbTreeRidBag:
		return true
	default:
		return false
	}
}

type ridBagDelegate interface {
	deserializeDelegate(r io.Reader) error
	serializeDelegate(w io.Writer) error
}

func newEmbeddedRidBag() ridBagDelegate { return &embeddedRidBag{} }

type embeddedRidBag struct {
	links []OIdentifiable
}

func (bag *embeddedRidBag) deserializeDelegate(r io.Reader) (err error) {
	defer catch(&err)
	n := int(rw.ReadInt(r))
	bag.links = make([]OIdentifiable, n)
	for i := range bag.links {
		var rid RID
		if err = rid.FromStream(r); err != nil {
			return
		}
		bag.links[i] = rid
	}
	return nil
}
func (bag *embeddedRidBag) serializeDelegate(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteInt(w, int32(len(bag.links)))
	for _, l := range bag.links {
		if err = l.GetIdentity().ToStream(w); err != nil {
			return
		}
	}
	return nil
}

func newSBTreeRidBag() ridBagDelegate { return &sbTreeRidBag{} }
func newBonsaiCollectionPtr(fileId int64, pageIndex int64, pageOffset int) *bonsaiCollectionPtr {
	return &bonsaiCollectionPtr{
		fileId:     fileId,
		pageIndex:  pageIndex,
		pageOffset: pageOffset,
	}
}

type bonsaiCollectionPtr struct {
	fileId     int64
	pageIndex  int64
	pageOffset int
}
type sbTreeRidBag struct {
	collectionPtr *bonsaiCollectionPtr
	changes       map[RID][]interface{}
	size          int
}

func (bag *sbTreeRidBag) serializeDelegate(w io.Writer) (err error) {
	defer catch(&err)
	if bag.collectionPtr == nil {
		rw.WriteLong(w, -1)
		rw.WriteLong(w, -1)
		rw.WriteInt(w, -1)
	} else {
		rw.WriteLong(w, bag.collectionPtr.fileId)
		rw.WriteLong(w, bag.collectionPtr.pageIndex)
		rw.WriteInt(w, int32(bag.collectionPtr.pageOffset))
	}
	rw.WriteInt(w, -1) // TODO: need a real value for compatibility with <= 1.7.5
	rw.WriteInt(w, 0)  // TODO: support changes in sbTreeRidBag
	return
}
func (bag *sbTreeRidBag) deserializeDelegate(r io.Reader) (err error) {
	defer catch(&err)
	fileId := rw.ReadLong(r)
	pageIndex := rw.ReadLong(r)
	pageOffset := int(rw.ReadInt(r))
	rw.ReadInt(r) // Cached bag size. Not used after 1.7.5
	if fileId == -1 {
		bag.collectionPtr = nil
	} else {
		bag.collectionPtr = newBonsaiCollectionPtr(fileId, pageIndex, pageOffset)
	}
	bag.size = -1
	return bag.deserializeChanges(r)
}
func (bag *sbTreeRidBag) deserializeChanges(r io.Reader) (err error) {
	n := int(rw.ReadInt(r))
	changes := make(map[RID][]interface{})

	type change struct {
		diff bool
		val  int
	}

	for i := 0; i < n; i++ {
		var rid RID
		if err = rid.FromStream(r); err != nil {
			return err
		}
		chval := int(rw.ReadInt(r))
		chtp := int(rw.ReadByte(r))
		arr := changes[rid]
		switch chtp {
		case 1: // abs
			arr = append(arr, change{diff: false, val: chval})
		case 0: // diff
			arr = append(arr, change{diff: true, val: chval})
		default:
			err = fmt.Errorf("unknown change type: %d", chtp)
			return
		}
		changes[rid] = arr
	}
	bag.changes = changes
	return
}
