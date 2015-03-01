package obinary

import "testing"

func TestValidDbType(t *testing.T) {
	assert(t, !validDbType("foo"), "foo should not be valid")
	assert(t, validDbType(DocumentDbType), "DocumentDbType should be valid")
	assert(t, validDbType(GraphDbType), "GraphDbType should be valid")
}

func TestValidStorageType(t *testing.T) {
	assert(t, !validStorageType("foo"), "foo should not be valid")
	assert(t, validStorageType(PersistentStorageType), "PersistentStorageType should be valid")
	assert(t, validStorageType(VolatileStorageType), "PersistentStorageType should be valid")
}
