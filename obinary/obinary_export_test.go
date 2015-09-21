package obinary

import (
	"github.com/istreamdata/orientgo/obinary/rw"
)

func ReadErrorResponse(r *rw.Reader) (serverException error) {
	return readErrorResponse(r)
}
