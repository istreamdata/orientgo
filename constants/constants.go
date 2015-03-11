package constants

const (
	MaxUint = ^uint32(0)
	MinUint = 0
	MaxInt  = int32(MaxUint >> 1)
	MinInt  = -MaxInt - 1

	MaxUint64 = ^uint64(0)
	MinUint64 = 0
	MaxInt64  = int64(MaxUint64 >> 1)
	MinInt64  = -MaxInt64 - 1
)

// end user constants => TODO: these should be given types
const (
	DocumentDbType = "document" // use in OpenDatabase() call
	GraphDbType    = "graph"    // use in OpenDatabase() call

	PersistentStorageType = "plocal" // use in DatabaseExists() call
	VolatileStorageType   = "memory" // use in DatabaseExists() call
)
