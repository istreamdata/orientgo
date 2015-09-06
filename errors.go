package orient

import "bytes"

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

func (e UnknownException) ExcClass() string {
	return e.Class
}
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

type ErrInvalidConn struct {
	Msg string
}

func (e ErrInvalidConn) Error() string {
	return "Invalid Connection: %s" + e.Msg
}
