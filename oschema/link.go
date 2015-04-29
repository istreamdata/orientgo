package oschema

type OLink struct {
	RID    string
	Record *ODocument
}

type OLinkList struct {
	Links []*OLink
}

type OLinkMap struct {
	Links map[string]*OLink
}
