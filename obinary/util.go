package obinary

import (
	"fmt"
)

func catch2(err *error) {
	if r := recover(); r != nil {
		switch rr := r.(type) {
		case error:
			*err = rr
		default:
			*err = fmt.Errorf("%v", r)
		}
	}
}
