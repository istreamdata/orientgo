package obinary

// constants specific to the Network Binary Protocol

// internal client constants
const (
	noSessionId                = -1
	MaxProtocolVersion         = 31 // max protocol supported by this client
	MinProtocolVersion         = 28 // min protocol supported by this client
	minBinarySerializerVersion = 22 // if server protocol version is less, use csv serde, not binary serde
	driverName                 = "OrientDB Go client"
	driverVersion              = "1.0"
	serializeTypeBinary        = "ORecordSerializerBinary" // do not change: required by server
	serializeTypeCsv           = "ORecordDocument2csv"     // do not change: required by server
)

const (
	// binary protocol sentinel values when reading single records
	RecordNull = -2
	RecordRID  = -3
)

// command and server-related constants
// copied from Java OChannelBinaryProtocol
const (
	// OUTGOING
	requestShutdown                      = 1
	requestConnect                       = 2
	requestDbOpen                        = 3
	requestDbCreate                      = 4
	requestDbClose                       = 5
	requestDbExists                      = 6
	requestDbDrop                        = 7
	requestDbSIZE                        = 8
	requestDbCOUNTRECORDS                = 9
	requestDataClusterADD                = 10
	requestDataClusterDROP               = 11
	requestDataClusterCOUNT              = 12
	requestDataClusterDATARANGE          = 13
	requestDataClusterCOPY               = 14
	requestDataClusterLH_CLUSTER_IS_USED = 16 // since 1.2.0
	requestDataSegmentADD                = 20
	requestDataSegmentDROP               = 21
	requestRecordMETADATA                = 29 // since 1.4.0
	requestRecordLOAD                    = 30
	requestRecordCREATE                  = 31
	requestRecordUPDATE                  = 32
	requestRecordDELETE                  = 33
	requestRecordCOPY                    = 34
	requestPositionsHIGHER               = 36 // since 1.3.0
	requestPositionsLOWER                = 37 // since 1.3.0
	requestRecordCLEAN_OUT               = 38 // since 1.3.0
	requestPositionsFLOOR                = 39 // since 1.3.0
	requestCount                         = 40 // DEPRECATED: USE REQUEST_DATACLUSTER_COUNT
	requestCommand                       = 41
	requestPositionsCEILING              = 42 // since 1.3.0
	requestRecordHIDE                    = 43 // since 1.7
	requestTxCommit                      = 60
	requestConfigGET                     = 70
	requestConfigSET                     = 71
	requestConfigLIST                    = 72
	requestDbRELOAD                      = 73 // SINCE 1.0rc4
	requestDbLIST                        = 74 // SINCE 1.0rc6
	requestPushDistribConfig             = 80
	// DISTRIBUTED
	requestDbCOPY      = 90 // SINCE 1.0rc8
	requestREPLICATION = 91 // SINCE 1.0
	requestCLUSTER     = 92 // SINCE 1.0
	requestDbTRANSFER  = 93 // SINCE 1.0.2
	// Lock + sync
	requestDbFREEZE           = 94 // SINCE 1.1.0
	requestDbRELEASE          = 95 // SINCE 1.1.0
	requestDataClusterFREEZE  = 96
	requestDataClusterRELEASE = 97
	// REMOTE SB-TREE COLLECTIONS
	requestCREATE_SBTREE_BONSAI            = 110
	requestSBTREE_BONSAI_GET               = 111
	requestSBTREE_BONSAI_FIRST_KEY         = 112
	requestSBTREE_BONSAI_GET_ENTRIES_MAJOR = 113
	requestRIDBAG_GET_SIZE                 = 114

	// INCOMING
	responseStatusOk    = 0
	responseStatusError = 1
	responseStatusPush  = 3

	// FOR MORE INFO:
	// https://github.com/orientechnologies/orientdb/wiki/Network-Binary-Protocol#wiki-Compatibility
	ProtoVersion21      = 21
	ProtoVersion24      = 24
	ProtoVersion25      = 25
	ProtoVersion26      = 26
	ProtoVersion27      = 27
	CurrentProtoVersion = 28
)
