//
// Error types and error functions for the ogonori library
//
package oerror

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"

	"gopkg.in/istreamdata/orientgo.v1/oschema"
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

func (e Trace) traceInfo() string {
	idx := strings.Index(e.File, "github.com") // strip off abs path
	return fmt.Sprintf("%s:%d", e.File[idx:], e.Line)
}

//
// GetFullTrace extracts an error "stack" trace for the Trace error
// wrappers down to the ultimate cause. If a non-Trace error is
// passed in then just the "Cause" info is returned.
//
func GetFullTrace(err error) string {
	var buf bytes.Buffer
	buf.Write([]byte("Trace:"))
OUTER:
	for {
		buf.Write([]byte("  "))
		switch err.(type) {
		case Trace:
			buf.WriteString(err.(Trace).traceInfo())
			err = err.(Trace).Cause

		default:
			buf.Write([]byte("Cause: "))
			buf.WriteString(err.Error())
			break OUTER
		}
		buf.WriteRune('\n')
	}
	return buf.String()
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
// InvalidDatabaseType is an Error that indicates that the db type value
// is not one that the OrientDB server will recognize.  For OrientDB 2.x, the
// valid types are "document" or "graph".  Constants for these values are
// provided in the obinary ogonori code base.
//
type ErrDataTypeMismatch struct {
	ExpectedDataType oschema.ODataType
	ExpectedGoType   string
	ActualValue      interface{}
}

func (e ErrDataTypeMismatch) Error() string {
	gotype := ""
	if e.ExpectedGoType != "" {
		gotype = " (" + e.ExpectedGoType + ")"
	}
	return fmt.Sprintf("DataTypeMismatch: Actual: %v of type %T; Expected %s%s",
		e.ActualValue, e.ActualValue, oschema.ODataTypeNameFor(e.ExpectedDataType),
		gotype)
}

// ------

//
// OServerException encapsulates Java-based Exceptions from
// the OrientDB server. OrientDB can return multiple exceptions
// for a single query/command, so they are all encapsulated in
// one ogonori OServerException object.
// Class = Java exception class
// Message = error message from the server
//
type OServerException struct {
	Classes  []string
	Messages []string
}

func (e OServerException) Error() string {
	var buf bytes.Buffer
	buf.WriteString("OrientDB Server Exception: ")
	for i, cls := range e.Classes {
		buf.WriteString("\n  ")
		buf.WriteString(cls)
		buf.WriteString(": ")
		buf.WriteString(e.Messages[i])
	}
	return buf.String()
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

// ------

type ErrInvalidConn struct {
	Msg string
}

func (e ErrInvalidConn) Error() string {
	return "Invalid Connection: %s" + e.Msg
}

// ------

var ErrStaleGlobalProperties error
