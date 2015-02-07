package obinary

import "fmt"

type UnsupportedVersionError struct {
	serverVersion int16
}

func (e UnsupportedVersionError) Error() string {
	return fmt.Sprintf("server binary protocol version `%d` is outside client supported version range: %d-%d",
		e.serverVersion, MinSupportedBinaryProtocolVersion, MaxSupportedBinaryProtocolVersion)
}

// ------

type SessionNotInitialized struct{}

func (e SessionNotInitialized) Error() string {
	return "Session not initialized. Call OpenDatabase or CreateServerSession first."
}

// ------

type InvalidStorageType struct {
	typeRequested string
}

func (e InvalidStorageType) Error() string {
	return "Storage Type is not valid: " + e.typeRequested
}

// ------

type InvalidDatabaseType struct {
	typeRequested string
}

func (e InvalidDatabaseType) Error() string {
	return "Database Type is not valid: " + e.typeRequested
}

// ------
