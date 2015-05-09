package oschema

import "fmt"

//
// This file holds LINK type datastructures.
// Namely, for LINK, LINKLIST (LINKSET) and LINKMAP.
//

type OLink struct {
	RID    ORID
	Record *ODocument
}

func (lnk *OLink) String() string {
	recStr := "<nil>"
	if lnk.Record != nil {
		// fields are not shown to avoid infinite loops when there are circular links
		recStr = lnk.Record.StringNoFields()
	}
	return fmt.Sprintf("<OLink RID: %s, Record: %s>", lnk.RID, recStr)
}

// // ------

// //
// // Used for both LINKLIST and LINKSET OrientDB types
// //
// type OLinkList struct {
// 	Links []*OLink
// }

// // ------

// type OLinkMap struct {
// 	Links map[string]*OLink
// }
