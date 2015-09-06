package obinary

import "io"

func ReadErrorResponse(r io.Reader) (serverException error) {
	return readErrorResponse(r)
}
