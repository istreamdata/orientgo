package obinary

import (
	"fmt"
	"github.com/istreamdata/orientgo"
	"github.com/istreamdata/orientgo/oschema"
	"regexp"
	"strings"
)

type ErrUnsupportedVersion int

func (e ErrUnsupportedVersion) Error() string {
	return fmt.Sprintf("server protocol version %d is not supported (valid: %d-%d)", int(e), MinProtocolVersion, MaxProtocolVersion)
}

var ErrClosedConnection = fmt.Errorf("closed connection")

type ErrBrokenProtocol struct {
	Reason error
}

func (e ErrBrokenProtocol) Error() string {
	return fmt.Sprintf("connection stream broken: %s", e.Reason.Error())
}

// InvalidDatabaseType is an Error that indicates that the db type value
// is not one that the OrientDB server will recognize.  For OrientDB 2.x, the
// valid types are "document" or "graph".  Constants for these values are
// provided in the obinary ogonori code base.
type ErrDataTypeMismatch struct {
	ExpectedDataType oschema.OType
	ExpectedGoType   string
	ActualValue      interface{}
}

func (e ErrDataTypeMismatch) Error() string {
	gotype := ""
	if e.ExpectedGoType != "" {
		gotype = " (" + e.ExpectedGoType + ")"
	}
	return fmt.Sprintf("DataTypeMismatch: Actual: %v of type %T; Expected %s%s",
		e.ActualValue, e.ActualValue, e.ExpectedDataType,
		gotype)
}

var ErrStaleGlobalProperties = fmt.Errorf("stale global properties")

type ODuplicatedRecordException struct {
	orient.OServerException
}

func (e ODuplicatedRecordException) Error() string {
	re := regexp.MustCompile(".* found duplicated key '(?P<key>.+)' in index " +
		"'(?P<index>[\\w\\s\\.]+)' previously assigned to the record [\\d\\#\\:]*")
	for _, ex := range e.Exceptions {
		message := ex.ExcMessage()
		if re.MatchString(message) {
			key := fmt.Sprintf("${%s}", re.SubexpNames()[1])
			key = re.ReplaceAllString(message, key)
			index := fmt.Sprintf("${%s}", re.SubexpNames()[2])
			index = re.ReplaceAllString(message, index)
			i := strings.Split(index, ".")
			if len(i) != 2 {
				break
			}
			className := i[0]
			propertyName := i[1]
			return fmt.Sprintf(`%s with %s "%s" already exists`, className, strings.ToLower(propertyName), key)
		}
	}
	return e.OServerException.Error()
}
