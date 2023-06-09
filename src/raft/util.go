package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
)

// Debugging
const Debug = true

var once sync.Once

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Info(format string, v ...interface{}) {
	logMessage("INFO", fmt.Sprintf(format, v...))
}

func Warning(format string, v ...interface{}) {
	logMessage("WARN", fmt.Sprintf(format, v...))
}

func Error(format string, v ...interface{}) {
	logMessage("ERROR", fmt.Sprintf(format, v...))
}

func FATAL(format string, v ...interface{}) {
	logMessage("FATAL", fmt.Sprintf(format, v...))
}

func logMessage(level string, message string) {
	_, file, line, _ := runtime.Caller(2)
	pid := os.Getpid()
	once.Do(func() {
		log.SetFlags(log.Lmicroseconds)
	})
	if !Debug && (level == "INFO" || level == "WARN") {
		return
	}

	if level == "FATAL" {
		log.Fatalf("[%s:%d] [%d] [%s] %s\n", file, line, pid, level, message)
	} else {
		log.Printf("[%s:%d] [%d] [%s] %s\n", file, line, pid, level, message)
	}
}
