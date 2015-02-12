package obinary

// end user constants
const (
	DocumentDbType = "document" // use in OpenDatabase() call
	GraphDbType    = "graph"    // use in OpenDatabase() call

	PersistentStorageType = "plocal" // use in DatabaseExists() call
	VolatileStorageType   = "memory" // use in DatabaseExists() call
)

// internal client constants
const (
	NoSessionId                       = -1
	MaxSupportedBinaryProtocolVersion = 28 // max protocol supported by this client
	MinSupportedBinaryProtocolVersion = 21 // min protocol supported by this client
	MinBinarySerializerVersion        = 22 // if server protocol version is less, use csv serde, not binary serde
	RequestNewSession                 = -4 // arbitrary negative number sent to start session
	DriverName                        = "ogonori OrientDB Go client"
	DriverVersion                     = "1.0"
	BinarySerialization               = "ORecordSerializerBinary" // do not change: required by server
	CsvSerialization                  = "ORecordDocument2csv"     // do not change: required by server
)

// command and server-related constants
// copied from Java OChannelBinaryProtocol
const (
	// OUTGOING
	REQUEST_SHUTDOWN                       = 1
	REQUEST_CONNECT                        = 2
	REQUEST_DB_OPEN                        = 3
	REQUEST_DB_CREATE                      = 4
	REQUEST_DB_CLOSE                       = 5
	REQUEST_DB_EXIST                       = 6
	REQUEST_DB_DROP                        = 7
	REQUEST_DB_SIZE                        = 8
	REQUEST_DB_COUNTRECORDS                = 9
	REQUEST_DATACLUSTER_ADD                = 10
	REQUEST_DATACLUSTER_DROP               = 11
	REQUEST_DATACLUSTER_COUNT              = 12
	REQUEST_DATACLUSTER_DATARANGE          = 13
	REQUEST_DATACLUSTER_COPY               = 14
	REQUEST_DATACLUSTER_LH_CLUSTER_IS_USED = 16 // since 1.2.0
	REQUEST_DATASEGMENT_ADD                = 20
	REQUEST_DATASEGMENT_DROP               = 21
	REQUEST_RECORD_METADATA                = 29 // since 1.4.0
	REQUEST_RECORD_LOAD                    = 30
	REQUEST_RECORD_CREATE                  = 31
	REQUEST_RECORD_UPDATE                  = 32
	REQUEST_RECORD_DELETE                  = 33
	REQUEST_RECORD_COPY                    = 34
	REQUEST_POSITIONS_HIGHER               = 36 // since 1.3.0
	REQUEST_POSITIONS_LOWER                = 37 // since 1.3.0
	REQUEST_RECORD_CLEAN_OUT               = 38 // since 1.3.0
	REQUEST_POSITIONS_FLOOR                = 39 // since 1.3.0
	REQUEST_COUNT                          = 40 // DEPRECATED: USE REQUEST_DATACLUSTER_COUNT
	REQUEST_COMMAND                        = 41
	REQUEST_POSITIONS_CEILING              = 42 // since 1.3.0
	REQUEST_RECORD_HIDE                    = 43 // since 1.7
	REQUEST_TX_COMMIT                      = 60
	REQUEST_CONFIG_GET                     = 70
	REQUEST_CONFIG_SET                     = 71
	REQUEST_CONFIG_LIST                    = 72
	REQUEST_DB_RELOAD                      = 73 // SINCE 1.0rc4
	REQUEST_DB_LIST                        = 74 // SINCE 1.0rc6
	REQUEST_PUSH_DISTRIB_CONFIG            = 80
	// DISTRIBUTED
	REQUEST_DB_COPY     = 90 // SINCE 1.0rc8
	REQUEST_REPLICATION = 91 // SINCE 1.0
	REQUEST_CLUSTER     = 92 // SINCE 1.0
	REQUEST_DB_TRANSFER = 93 // SINCE 1.0.2
	// Lock + sync
	REQUEST_DB_FREEZE           = 94 // SINCE 1.1.0
	REQUEST_DB_RELEASE          = 95 // SINCE 1.1.0
	REQUEST_DATACLUSTER_FREEZE  = 96
	REQUEST_DATACLUSTER_RELEASE = 97
	// REMOTE SB-TREE COLLECTIONS
	REQUEST_CREATE_SBTREE_BONSAI            = 110
	REQUEST_SBTREE_BONSAI_GET               = 111
	REQUEST_SBTREE_BONSAI_FIRST_KEY         = 112
	REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR = 113
	REQUEST_RIDBAG_GET_SIZE                 = 114

	// INCOMING
	RESPONSE_STATUS_OK    = 0
	RESPONSE_STATUS_ERROR = 1
	PUSH_DATA             = 3

	// CONSTANTS
	RECORD_NULL = -2
	RECORD_RID  = -3

	// FOR MORE INFO:
	// https://github.com/orientechnologies/orientdb/wiki/Network-Binary-Protocol#wiki-Compatibility
	PROTOCOL_VERSION_21      = 21
	PROTOCOL_VERSION_24      = 24
	PROTOCOL_VERSION_25      = 25
	PROTOCOL_VERSION_26      = 26
	PROTOCOL_VERSION_27      = 27
	CURRENT_PROTOCOL_VERSION = 28 // SENT AS SHORT AS FIRST PACKET AFTER SOCKET CONNECTION
)
