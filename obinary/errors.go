package obinary

import "fmt"

// ------

type UnsupportedVersionError struct {
	serverVersion int16
}

func (e UnsupportedVersionError) Error() string {
	return fmt.Sprintf("server binary protocol version `%d` is outside client supported version range: %d-%d",
		e.serverVersion, MinSupportedBinaryProtocolVersion, MaxSupportedBinaryProtocolVersion)
}

// ------

type IncorrectNetworkRead struct {
	expected int
	actual   int
}

func (e IncorrectNetworkRead) Error() string {
	return fmt.Sprintf("Incorrect number of bytes read from connection. Expected: %d; Actual: %d",
		e.expected, e.actual)
}

// ------

type SessionNotInitialized struct{}

func (e SessionNotInitialized) Error() string {
	return "Session not initialized. Call OpenDatabase or ServerConnect" // TODO: `ServerConnect` is probably the wrong name here
}
