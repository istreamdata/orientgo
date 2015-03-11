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

type DatabaseType string
type StorageType string

const (
	DocumentDb DatabaseType = "document" // use in OpenDatabase() call
	GraphDb    DatabaseType = "graph"    // use in OpenDatabase() call

	Persistent StorageType = "plocal" // use in DatabaseExists() call
	Volatile   StorageType = "memory" // use in DatabaseExists() call
)
