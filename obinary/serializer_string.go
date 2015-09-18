package obinary

import (
	"fmt"
	"github.com/istreamdata/orientgo"
	"math"
	"strconv"
	"strings"
)

const (
	string_LINK                 = '#'
	string_EMBEDDED_BEGIN       = '('
	string_EMBEDDED_END         = ')'
	string_LIST_BEGIN           = '['
	string_LIST_END             = ']'
	string_SET_BEGIN            = '<'
	string_SET_END              = '>'
	string_MAP_BEGIN            = '{'
	string_MAP_END              = '}'
	string_BAG_BEGIN            = '%'
	string_BAG_END              = ';'
	string_BINARY_BEGINEND      = '_'
	string_CUSTOM_TYPE          = '^'
	string_ENTRY_SEPARATOR      = ':'
	string_PARAMETER_NAMED      = ':'
	string_PARAMETER_POSITIONAL = '?'

	string_DECIMAL_SEPARATOR = '.'
)

var (
	string_MaxInt = strconv.Itoa(math.MaxInt32)
)

type stringRecordFormatAbs struct{}

func (stringRecordFormatAbs) GetType(s string) orient.OType {
	if s == "" {
		return orient.UNKNOWN
	}
	rs := []rune(s)
	firstChar := rs[0]
	switch firstChar {
	case string_LINK: // RID
		return orient.LINK
	case '\'', '"':
		return orient.STRING
	case string_BINARY_BEGINEND:
		return orient.BINARY
	case string_EMBEDDED_BEGIN:
		return orient.EMBEDDED
	case string_LIST_BEGIN:
		return orient.EMBEDDEDLIST
	case string_SET_BEGIN:
		return orient.EMBEDDEDSET
	case string_MAP_BEGIN:
		return orient.EMBEDDEDMAP
	case string_CUSTOM_TYPE:
		return orient.CUSTOM
	}

	// BOOLEAN?
	if ls := strings.ToLower(s); ls == "true" || ls == "false" {
		return orient.BOOLEAN
	}

	// NUMBER OR STRING?
	integer := true
	for i, c := range rs {
		if c >= '0' && c <= '9' {
			continue
		} else if i == 0 && (c == '+' || c == '0') {
			continue
		} else if c == string_DECIMAL_SEPARATOR {
			integer = false // maybe float, seek for other string char to be sure
		} else {
			if i == 0 {
				return orient.STRING
			}
			if !integer && (c == 'E' || c == 'e') {
				// CHECK FOR SCIENTIFIC NOTATION
				if i+1 < len(rs) {
					if rs[i+1] == '-' {
						// JUMP THE DASH IF ANY (NOT MANDATORY)
						i++
					}
					continue
				}
			} else {
				switch c {
				case 'f':
					return orient.FLOAT
				case 'c':
					return orient.DECIMAL
				case 'l':
					return orient.LONG
				case 'd':
					return orient.DOUBLE
				case 'b':
					return orient.BYTE
				case 'a':
					return orient.DATE
				case 't':
					return orient.DATETIME
				case 's':
					return orient.SHORT
				}
			}
			return orient.STRING
		}
	}

	if integer {
		// AUTO CONVERT TO LONG IF THE INTEGER IS TOO BIG
		if n, mn := len(rs), len(string_MaxInt); n > mn || (n == mn && s > string_MaxInt) {
			return orient.LONG
		}
		return orient.INTEGER
	}

	if _, err := strconv.ParseFloat(s, 32); err == nil {
		return orient.FLOAT
	} else if _, err = strconv.ParseFloat(s, 64); err == nil {
		return orient.DOUBLE
	} else {
		return orient.DECIMAL
	}
}
func (f stringRecordFormatAbs) FieldTypeFromStream(tp orient.OType, s string) interface{} {
	if s == "" {
		return nil
	} else if tp == orient.UNKNOWN {
		tp = orient.EMBEDDED
	}

	switch tp {
	case orient.STRING:
		return s // TODO: implement in a right way
	case orient.INTEGER:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			panic(err)
		}
		return int32(v)
	case orient.LONG:
		v, err := strconv.ParseInt(strings.TrimSuffix(s, "l"), 10, 64)
		if err != nil {
			panic(err)
		}
		return int64(v)
	case orient.BOOLEAN:
		switch strings.ToLower(s) {
		case "true":
			return true
		case "false":
			return false
		default:
			panic(fmt.Errorf("unknown val for bool: '%s'", s))
		}
	default: // TODO: more types
		panic(fmt.Errorf("unsupported type for stringRecordFormatAbs: %s", tp))
	}
}
