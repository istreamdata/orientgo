package oschema

// ref: com.orientechnologies.orient.core.id

import (
	"fmt"
	"github.com/istreamdata/orientgo/obinary/rw"
	"io"
	"strconv"
	"strings"
)

type OIdentifiable interface {
	GetIdentity() RID
	GetRecord() interface{}
}

var (
	_ OIdentifiable = RID{}
)

const (
	ridPrefix    = '#'
	ridSeparator = ':'

	clusterIdMax      = 32767
	clusterIdInvalid  = -1
	clusterPosInvalid = -1

	RIDSerializedSize = rw.SizeShort + rw.SizeLong
)

// RID encapsulates the two aspects of an OrientDB RecordID - ClusterID:ClusterPos.
// ORecordId in Java world.
type RID struct {
	ClusterID  int16
	ClusterPos int64
}

// NewEmptyRID returns an RID with the default "invalid" settings.
// Invalid settings indicate that the Document has not yet been saved
// to the DB (which assigns it a valid RID) or it indicates that
// it is not a true Document with a Class (e.g., it is a result of a Property query)
func NewEmptyRID() RID {
	return RID{ClusterID: clusterIdInvalid, ClusterPos: clusterPosInvalid}
}

func (rid RID) checkClusterLimits() {
	if rid.ClusterID < -2 {
		panic(fmt.Sprint("RecordId cannot support negative cluster id. You've used:", rid.ClusterID))
	}
	if rid.ClusterID > clusterIdMax {
		panic(fmt.Sprint("RecordId cannot support cluster id major than 32767. You've used:", rid.ClusterID))
	}
}

// NewRID creates a RID with given ClusterId and ClusterPos. It will check value for validity.
func NewRID(cid int16, pos int64) RID {
	rid := RID{ClusterID: cid, ClusterPos: pos}
	rid.checkClusterLimits()
	return rid
}

// NewRIDInCluster creates an empty RID inside specified cluster
func NewRIDInCluster(cid int16) RID {
	rid := RID{ClusterID: cid, ClusterPos: clusterPosInvalid}
	rid.checkClusterLimits()
	return rid
}

// GetIdentity implements OIdentifiable interface on RID
func (r RID) GetIdentity() RID {
	return r
}

func (r RID) GetRecord() interface{} {
	return nil
}

// String converts RID to #N:M string format
func (rid RID) String() string {
	return fmt.Sprintf(
		string(ridPrefix)+"%d"+string(ridSeparator)+"%d",
		rid.ClusterID, rid.ClusterPos,
	)
}

func (r RID) IsValid() bool {
	return r.ClusterID != clusterIdInvalid
}
func (rid RID) IsPersistent() bool {
	return rid.ClusterID > -1 && rid.ClusterPos > clusterPosInvalid
}
func (rid RID) IsNew() bool {
	return rid.ClusterPos < 0
}
func (rid RID) IsTemporary() bool {
	return rid.ClusterID != -1 && rid.ClusterPos < clusterPosInvalid
}

// Next is a shortcut for rid.NextRID().String()
func (rid RID) Next() string {
	return rid.NextRID().String()
}

// NextRID returns next RID in current cluster
func (rid RID) NextRID() RID {
	rid.checkClusterLimits()
	rid.ClusterPos++ // uses local copy of rid
	return rid
}

func (rid *RID) FromStream(r io.Reader) (err error) {
	defer catch(&err)
	buf := make([]byte, RIDSerializedSize)
	rw.ReadRawBytes(r, buf)
	rid.ClusterID = int16(rw.Order.Uint16(buf))
	rid.ClusterPos = int64(rw.Order.Uint64(buf[rw.SizeShort:]))
	return
}

func (rid RID) ToStream(w io.Writer) (err error) {
	defer catch(&err)
	rw.WriteShort(w, rid.ClusterID)
	rw.WriteLong(w, rid.ClusterPos)
	return
}

// ParseRID converts a string of form #N:M or N:M to a RID.
func ParseRID(s string) (RID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return NewEmptyRID(), nil
	} else if !strings.Contains(s, string(ridSeparator)) {
		return NewEmptyRID(), fmt.Errorf("Argument '%s' is not a RecordId in form of string. Format must be: <cluster-id>:<cluster-position>", s)
	}
	s = strings.TrimLeft(s, string(ridPrefix))
	parts := strings.Split(s, string(ridSeparator))
	if len(parts) != 2 {
		return NewEmptyRID(), fmt.Errorf("Argument received '%s' is not a RecordId in form of string. Format must be: #<cluster-id>:<cluster-position>. Example: #3:12", s)
	}
	id, err := strconv.ParseInt(parts[0], 10, 16)
	if err != nil {
		return NewEmptyRID(), fmt.Errorf("Invalid RID string to ParseRID: %s", s)
	}
	pos, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return NewEmptyRID(), fmt.Errorf("Invalid RID string to ParseRID: %s", s)
	}
	return NewRID(int16(id), int64(pos)), nil
}

// MustParseRID is a version of ParseRID which panics on errors
func MustParseRID(s string) RID {
	rid, err := ParseRID(s)
	if err != nil {
		panic(err)
	}
	return rid
}
