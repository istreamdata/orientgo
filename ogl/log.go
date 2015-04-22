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
	fmt.Printf("\033[31mWARN: %s:%d: "+warnMsg+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
}

func Warnf(format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\033[31mWARN: %s:%d: "+fmt.Sprintf(format, a...)+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
}

func Fatal(msg string) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\033[31mFATAL: %s:%d: "+msg+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
	os.Exit(1)
}

func Fatale(err error) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\033[31mFATAL: %s:%d: "+err.Error()+"\033[39m\n\n",
		append([]interface{}{filepath.Base(file), line})...)
	os.Exit(1)
}
