package obinary

import (
	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
)

func ReadErrorResponse(r *rw.Reader) (serverException error) {
	return readErrorResponse(r, CurrentProtoVersion)
}
