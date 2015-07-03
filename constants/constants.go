//
// Useful global constants for use in ogonori
//
package constants

const (
	// TODO: put 32 suffix on these first for constants
	MaxUint32 = ^uint32(0)
	MinUint32 = 0
	MaxInt32  = int32(MaxUint32 >> 1)
	MinInt32  = -MaxInt32 - 1

	MaxUint64 = ^uint64(0)
	MinUint64 = 0
	MaxInt64  = int64(MaxUint64 >> 1)
	MinInt64  = -MaxInt64 - 1
)

// ----

type DatabaseType string
type StorageType string

const (
	DocumentDB DatabaseType = "document" // use in obinary.OpenDatabase() call
	GraphDB    DatabaseType = "graph"    // use in obinary.OpenDatabase() call

	Persistent StorageType = "plocal" // use in obinary.DatabaseExists() call
	Volatile   StorageType = "memory" // use in obinary.DatabaseExists() call
)
