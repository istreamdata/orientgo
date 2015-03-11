//
// Error types and error functions for the ogonori library
//
package oerror

import (
	"fmt"
	"runtime"
	"strings"
)

//
// Trace is a wrapper struct for errors so we can preseve information
// about the call stack an error passes through.
// Trace implements the Error interface.
//
type Trace struct {
	File  string
	Line  int
	Cause error
}

func (e Trace) Error() string {
	idx := strings.Index(e.File, "github.com") // strip off abs path
	return fmt.Sprintf("%s:%d; cause: %v", e.File[idx:], e.Line, e.Cause)
}

//
// ExtractCause will recurse down a "stack" of Trace errors until
// it gets to an error that is not of type Trace and return that.
// If an error not of type Trace is passed it, it is simply returned.
//
func ExtractCause(err error) error {
	switch err.(type) {
	case Trace:
		return ExtractCause(err.(Trace).Cause)
	default:
		return err
	}
}

//
// NewTrace creates a Trace Error wrapper that retains the underlying
// error ("cause") and the filename and line number of the previous call
// where 2 is subtracted from the line number.  So it's usage is appropriate
// with this form of code:
//
//     err = DoSomething()
//     if err != nil {
//         return oerror.NewTrace(err)  // line - 2 refers to the DoSomething() line
//     }
//
func NewTrace(cause error) Trace {
	_, file, line, _ := runtime.Caller(1)
	return Trace{file, line - 2, cause}
}

// ------

//
// SessionNotInitialized is an Error that indicates that no session was started
// before trying to issue a command to the OrientDB server or one of its databases.
//
type SessionNotInitialized struct{}

func (e SessionNotInitialized) Error() string {
	return "Session not initialized. Call OpenDatabase or CreateServerSession first."
}

// ------

//
// InvalidDatabaseType is an Error that indicates that the db type value
// is not one that the OrientDB server will recognize.  For OrientDB 2.x, the
// valid types are "document" or "graph".  Constants for these values are
// provided in the obinary ogonori code base.
//
type InvalidDatabaseType struct {
	TypeRequested string
}

func (e InvalidDatabaseType) Error() string {
	return "Database Type is not valid: " + e.TypeRequested
}

// ------

//
// Exception (Java-based) from the OrientDB server-side.
// Class = Java exception class
// Message = error message from the server
//
type OServerException struct {
	Class   string
	Message string
}

// ------

type IncorrectNetworkRead struct {
	Expected int
	Actual   int
}

func (e IncorrectNetworkRead) Error() string {
	return fmt.Sprintf("Incorrect number of bytes read from connection. Expected: %d; Actual: %d",
		e.Expected, e.Actual)
}
