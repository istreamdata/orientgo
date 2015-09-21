package orient

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
	owner    *Document
}

func (bag *RidBag) SetOwner(doc *Document) {
	bag.owner = doc
}
func (bag *RidBag) FromStream(r io.Reader) error {
	br := rw.NewReader(r)
	first := br.ReadByte()
	if err := br.Err(); err != nil {
		return err
	}
	if first&0x1 != 0 {
		bag.delegate = newEmbeddedRidBag()
	} else {
		bag.delegate = newSBTreeRidBag()
	}
	if first&0x2 != 0 {
		br.ReadRawBytes(bag.id[:])
	}
	if err := br.Err(); err != nil {
		return err
	}
	return bag.delegate.deserializeDelegate(br)
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
	bw := rw.NewWriter(w)
	bw.WriteByte(first)
	if hasUUID {
		bw.WriteRawBytes(bag.id[:])
	}
	return bag.delegate.serializeDelegate(bw)
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
	deserializeDelegate(br *rw.Reader) error
	serializeDelegate(bw *rw.Writer) error
}

func newEmbeddedRidBag() ridBagDelegate { return &embeddedRidBag{} }

type embeddedRidBag struct {
	links []OIdentifiable
}

func (bag *embeddedRidBag) deserializeDelegate(br *rw.Reader) error {
	n := int(br.ReadInt())
	bag.links = make([]OIdentifiable, n)
	for i := range bag.links {
		var rid RID
		if err := rid.FromStream(br); err != nil {
			return err
		}
		bag.links[i] = rid
	}
	return br.Err()
}
func (bag *embeddedRidBag) serializeDelegate(bw *rw.Writer) error {
	bw.WriteInt(int32(len(bag.links)))
	for _, l := range bag.links {
		if err := l.GetIdentity().ToStream(bw); err != nil {
			return err
		}
	}
	return bw.Err()
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

func (bag *sbTreeRidBag) serializeDelegate(bw *rw.Writer) error {
	if bag.collectionPtr == nil {
		bw.WriteLong(-1)
		bw.WriteLong(-1)
		bw.WriteInt(-1)
	} else {
		bw.WriteLong(bag.collectionPtr.fileId)
		bw.WriteLong(bag.collectionPtr.pageIndex)
		bw.WriteInt(int32(bag.collectionPtr.pageOffset))
	}
	bw.WriteInt(-1) // TODO: cached size; need a real value for compatibility with <= 1.7.5
	bw.WriteInt(0)  // TODO: support changes in sbTreeRidBag
	return bw.Err()
}
func (bag *sbTreeRidBag) deserializeDelegate(br *rw.Reader) error {
	fileId := br.ReadLong()
	pageIndex := br.ReadLong()
	pageOffset := int(br.ReadInt())
	br.ReadInt() // Cached bag size. Not used after 1.7.5
	if err := br.Err(); err != nil {
		return err
	}
	if fileId == -1 {
		bag.collectionPtr = nil
	} else {
		bag.collectionPtr = newBonsaiCollectionPtr(fileId, pageIndex, pageOffset)
	}
	bag.size = -1
	return bag.deserializeChanges(br)
}
func (bag *sbTreeRidBag) deserializeChanges(r *rw.Reader) (err error) {
	n := int(r.ReadInt())
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
		chval := int(r.ReadInt())
		chtp := int(r.ReadByte())
		arr := changes[rid]
		switch chtp {
		case 1: // abs
			arr = append(arr, change{diff: false, val: chval})
		case 0: // diff
			arr = append(arr, change{diff: true, val: chval})
		default:
			return fmt.Errorf("unknown change type: %d", chtp)
		}
		changes[rid] = arr
	}
	bag.changes = changes
	return r.Err()
}
