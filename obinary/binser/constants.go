package binser

// DataType values for OrientDB binary schemaless serialization format
// From: https://github.com/orientechnologies/orientdb/wiki/Types
const (
	BOOLEAN         = 0
	INTEGER         = 1
	SHORT           = 2
	LONG            = 3
	FLOAT           = 4
	DOUBLE          = 5
	DATETIME        = 6
	STRING          = 7
	BINARY          = 8
	EMBEDDED_RECORD = 9
	EMBEDDED_LIST   = 10
	EMBEDDED_SET    = 11
	EMBEDDED_MAP    = 12
	LINK            = 13
	LINK_LIST       = 14
	LINK_SET        = 15
	LINK_MAP        = 16
	BYTE            = 17
	TRANSIENT       = 18
	DATE            = 19
	CUSTOM          = 20
	DECIMAL         = 21
	LINK_BAG        = 22
	ANY             = 23
)
