package orient

import (
	"unicode"
)

func isExported(s string) bool {
	if s == "" {
		return false
	}
	return unicode.IsUpper(([]rune(s))[0])
}
