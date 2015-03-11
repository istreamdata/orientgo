package obinary

import (
	"testing"

	"github.com/quux00/ogonori/constants"
)

func TestValidDbType(t *testing.T) {
	assert(t, !validDbType("foo"), "foo should not be valid")
	assert(t, validDbType(constants.DocumentDbType), "DocumentDbType should be valid")
	assert(t, validDbType(constants.GraphDbType), "GraphDbType should be valid")
}

func TestValidStorageType(t *testing.T) {
	assert(t, !validStorageType("foo"), "foo should not be valid")
	assert(t, validStorageType(constants.PersistentStorageType), "PersistentStorageType should be valid")
	assert(t, validStorageType(constants.VolatileStorageType), "PersistentStorageType should be valid")
}
