package ogl

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

type Level int

const (
	DEBUG  Level = iota
	NORMAL Level = iota
	WARN   Level = iota
)

var level = NORMAL
var errMsgFmt string

func init() {
	if runtime.GOOS == "windows" {
		errMsgFmt = "%s: %s:%d: %s\n"

		// LEFT OFF
	} else {
		errMsgFmt = "\033[31m%s: %s:%d: %s\033[39m\n"
	}
}

func SetLevel(lvl Level) {
	level = lvl
}

func GetLevel() Level {
	return level
}

func Debugf(format string, a ...interface{}) {
	if level == DEBUG {
		Printf(format, a...)
	}
}

func Debugln(a ...interface{}) {
	if level == DEBUG {
		Println(a...)
	}
}

func Printf(format string, a ...interface{}) {
	if level < WARN {
		fmt.Printf(format, a...)
	}
}

func Println(a ...interface{}) {
	if level < WARN {
		fmt.Println(a...)
	}
}

func Warn(warnMsg string) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf(errMsgFmt, "WARN", filepath.Base(file), line, warnMsg)
}

func Warnf(format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf(errMsgFmt, "WARN", filepath.Base(file), line, fmt.Sprintf(format, a...))
}

func Fatal(msg string) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf(errMsgFmt, "FATAL", filepath.Base(file), line, msg)
	os.Exit(1)
}

func Fatale(err error) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf(errMsgFmt, "FATAL", filepath.Base(file), line, err.Error())
	os.Exit(1)
}
