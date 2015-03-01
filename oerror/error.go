package oerror

import (
	"fmt"
	"runtime"
	"strings"
)

type Trace struct {
	File  string
	Line  int
	Cause error
}

func (e Trace) Error() string {
	idx := strings.Index(e.File, "github.com") // strip off abs path
	return fmt.Sprintf("%s:%d; cause: %v", e.File[idx:], e.Line, e.Cause)
}

func NewTrace(cause error) Trace {
	_, file, line, _ := runtime.Caller(1)
	return Trace{file, line - 2, cause}
}
