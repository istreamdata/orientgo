package obinary

import "fmt"

type UnsupportedVersionError struct {
	serverVersion int16
}

func (e UnsupportedVersionError) Error() string {
	return fmt.Sprintf("server binary protocol version `%d` is outside client supported version range: %d-%d",
		e.serverVersion, MinSupportedBinaryProtocolVersion, MaxSupportedBinaryProtocolVersion)
}
