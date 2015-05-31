package obinary

import "fmt"

type ErrUnsupportedVersion struct {
	serverVersion int16
}

func (e ErrUnsupportedVersion) Error() string {
	return fmt.Sprintf("server binary protocol version `%d` is outside client supported version range: %d-%d",
		e.serverVersion, MinSupportedBinaryProtocolVersion, MaxSupportedBinaryProtocolVersion)
}
