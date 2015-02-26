package oschema

type OClass struct {
	Name             string
	ShortName        string
	Properties       map[string]*OProperty // key=Property.Name
	DefaultClusterId int32
	ClusterIds       []int32
	SuperClass       string
	OverSize         float32
	StrictMode       bool
	AbstractClass    bool
	ClusterSelection string // OClusterSelectionStrategy in Java code - needed?
	CustomFields     map[string]string
}

func NewOClassFromDocument(doc *ODocument) *OClass {
	oclass := &OClass{Properties: make(map[string]*OProperty)}

	if fld, ok := doc.Fields["name"]; ok && fld.Value != nil {
		oclass.Name = fld.Value.(string)
	}
	if fld, ok := doc.Fields["shortName"]; ok && fld.Value != nil {
		oclass.ShortName = fld.Value.(string)
	}

	// properties comes back as an ODocument
	if fld, ok := doc.Fields["properties"]; ok && fld.Value != nil {
		propsDocs := convertToODocumentRefSlice(fld.Value.([]interface{}))
		for _, propDoc := range propsDocs {
			oprop := NewOPropertyFromDocument(propDoc)
			oclass.Properties[oprop.Name] = oprop
		}
	}
	if fld, ok := doc.Fields["defaultClusterId"]; ok && fld.Value != nil {
		oclass.DefaultClusterId = fld.Value.(int32)
	}
	if fld, ok := doc.Fields["clusterIds"]; ok && fld.Value != nil {
		oclass.ClusterIds = convertToInt32Slice(fld.Value.([]interface{}))
	}
	if fld, ok := doc.Fields["superClass"]; ok && fld.Value != nil {
		oclass.SuperClass = fld.Value.(string)
	}
	if fld, ok := doc.Fields["overSize"]; ok && fld.Value != nil {
		oclass.OverSize = fld.Value.(float32)
	}
	if fld, ok := doc.Fields["strictMode"]; ok && fld.Value != nil {
		oclass.StrictMode = fld.Value.(bool)
	}
	if fld, ok := doc.Fields["abstract"]; ok && fld.Value != nil {
		oclass.AbstractClass = fld.Value.(bool)
	}
	if fld, ok := doc.Fields["clusterSelection"]; ok && fld.Value != nil {
		oclass.ClusterSelection = fld.Value.(string)
	}
	if fld, ok := doc.Fields["customFields"]; ok && fld.Value != nil {
		oclass.CustomFields = make(map[string]string)
		panic("customFields handling NOT IMPLEMENTED: Don't know what data structure is coming back from the server (need example)")
	}

	return oclass
}

func convertToODocumentRefSlice(x []interface{}) []*ODocument {
	y := make([]*ODocument, len(x))
	for i, v := range x {
		y[i] = v.(*ODocument)
	}
	return y
}

func convertToInt32Slice(x []interface{}) []int32 {
	y := make([]int32, len(x))
	for i, v := range x {
		y[i] = v.(int32)
	}
	return y
}
