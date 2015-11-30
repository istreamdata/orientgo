package orient

import (
	"bytes"
	"fmt"
)

var (
	exceptions = make(map[string]func(e Exception) Exception)
)

// RegException registers a function to convert server exception based on it's class.
func RegException(class string, fnc func(e Exception) Exception) {
	exceptions[class] = fnc
}

func init() {
	RegException("com.orientechnologies.orient.core.exception.OConcurrentModificationException", func(e Exception) Exception {
		return ErrConcurrentModification{e}
	})
}

// Exception is an interface for Java-based Exceptions.
type Exception interface {
	error
	// Returns Java exception class
	ExcClass() string
	// Returns exception message
	ExcMessage() string
}

// UnknownException is an arbitrary exception from Java side.
// If exception class is not recognized by this driver, it will return UnknownException.
type UnknownException struct {
	Class   string
	Message string
}

// ExcClass returns Java exception class
func (e UnknownException) ExcClass() string {
	return e.Class
}

// ExcMessage returns exception message
func (e UnknownException) ExcMessage() string {
	return e.Message
}
func (e UnknownException) Error() string {
	return e.Class + ": " + e.Message
}

// OServerException encapsulates Java-based Exceptions from
// the OrientDB server. OrientDB can return multiple exceptions
// for a single query/command, so they are all encapsulated in
// one OServerException object.
type OServerException struct {
	Exceptions []Exception
}

func (e OServerException) Error() string {
	var buf bytes.Buffer
	buf.WriteString("OrientDB Server Exception: ")
	for _, ex := range e.Exceptions {
		buf.WriteString("\n  ")
		buf.WriteString(ex.Error())
	}
	return buf.String()
}

// ErrInvalidConn is returned than DB functions are called without active DB connection
type ErrInvalidConn struct {
	Msg string
}

func (e ErrInvalidConn) Error() string {
	return "Invalid Connection: %s" + e.Msg
}

// ErrNoRecord is returned when trying to deserialize an empty result set into a single value.
var ErrNoRecord = fmt.Errorf("no records returned, while expecting one")

// ErrMultipleRecords is returned when trying to deserialize a result set with multiple records into a single value.
type ErrMultipleRecords struct {
	N   int
	Err error
}

func (e ErrMultipleRecords) Error() string {
	return fmt.Sprintf("multiple records returned (%d), while expecting one: %s", e.N, e.Err)
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	if errs, ok := err.(OServerException); ok {
		for _, e := range errs.Exceptions {
			if fnc, ok := exceptions[e.ExcClass()]; ok {
				return fnc(e)
			}
		}
	}
	return err
}

type ErrConcurrentModification struct {
	Exception
}

func (e ErrConcurrentModification) Error() string {
	return fmt.Sprintf("concurrent modification: %v", e.Exception)
}
