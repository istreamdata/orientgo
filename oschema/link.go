package oschema

import "fmt"

var _ OIdentifiable = (*OLink)(nil)

type OIdentifiableCollection interface {
	Len() int
	OIdentifiableIterator() <-chan OIdentifiable
}

// This file holds LINK type datastructures.
// Namely, for LINK, LINKLIST (LINKSET), LINKMAP and LINKBAG (aka RidBag)

// OLink represents a LINK in the OrientDB system.
// It holds a RID and optionally a Record pointer to
// the ODocument that the RID points to.
type OLink struct {
	RID    RID        // required
	Record *ODocument // optional
}

func (lnk *OLink) GetIdentity() RID {
	if lnk == nil {
		return NewEmptyRID()
	}
	return lnk.RID
}

func (lnk *OLink) GetRecord() interface{} {
	if lnk == nil || lnk.Record == nil {
		return nil
	}
	return lnk.Record
}

func (lnk *OLink) String() string {
	recStr := "<nil>"
	if lnk.Record != nil {
		// fields are not shown to avoid infinite loops when there are circular links
		recStr = lnk.Record.StringNoFields()
	}
	return fmt.Sprintf("<OLink RID: %s, Record: %s>", lnk.RID, recStr)
}

// OLinkBag can have a tree-based or an embedded representation.
//
// Embedded stores its content directly to the document that owns it.
// It is used when only small numbers of links are stored in the bag.
//
// The tree-based implementation stores its content in a separate data
// structure called on OSBTreeBonsai on the server. It fits great for cases
// when you have a large number of links.  This is used to efficiently
// manage relationships (particularly in graph databases).
//
// The OLinkBag struct corresponds to ORidBag in Java client codebase.
type OLinkBag struct {
	Links []*OLink
	ORemoteLinkBag
}

type ORemoteLinkBag struct {
	size              int // this is the size on the remote server
	CollectionPointer *treeCollectionPointer
}

type treeCollectionPointer struct {
	fileID     int64
	pageIndex  int64
	pageOffset int32
}

// GetFileID returns the fileID of the server collection pointer
// if the OLinkBag is an instance of ORemoteLinkBag.
// If the OLinkBAg is not an instance of ORemoteLinkBag,
// than the return value is meaningless, but no error
// is returned/thrown in such a case.
func (lb *ORemoteLinkBag) GetFileID() int64 {
	return lb.CollectionPointer.fileID
}

func (lb *ORemoteLinkBag) GetPageIndex() int64 {
	return lb.CollectionPointer.pageIndex
}

func (lb *ORemoteLinkBag) GetPageOffset() int32 {
	return lb.CollectionPointer.pageOffset
}

func (lb *ORemoteLinkBag) GetRemoteSize() int {
	return lb.size
}

func (lb *ORemoteLinkBag) SetRemoteSize(sz int32) {
	lb.size = int(sz)
}

// AddLink adds an *OLink to the slice of *OLink in the OLinkBag
func (lb *OLinkBag) AddLink(lnk *OLink) {
	lb.Links = append(lb.Links, lnk)
}

// IsRemote indicates where this LinkBag has its data
// stored in an opaque format on the remote OrientDB server.
func (lb *OLinkBag) IsRemote() bool {
	return lb.ORemoteLinkBag.CollectionPointer != nil
}

// NewOLinkBag constructor is called with all the OLink
// objects precreated. Usually appropriate when dealing
// with an embedded LinkBag.
func NewOLinkBag(links []*OLink) *OLinkBag {
	return &OLinkBag{Links: links}
}

// NewTreeOLinkBag constructor is called for remote tree-based
// LinkBags.  This is called by the Deserializer when all it knows
// is the pointer reference to the LinkBag on the remote server.
//
// The OLinkBag returned does not yet know the size of the LinkBag
// nor know what the OLinks are.
func NewTreeOLinkBag(fileID int64, pageIdx int64, pageOffset int32, size int32) *OLinkBag {
	treeptr := treeCollectionPointer{
		fileID:     fileID,
		pageIndex:  pageIdx,
		pageOffset: pageOffset,
	}

	rLinkBag := ORemoteLinkBag{CollectionPointer: &treeptr, size: int(size)}
	return &OLinkBag{ORemoteLinkBag: rLinkBag}
}
