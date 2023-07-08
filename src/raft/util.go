package raft

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Debugging
const DebugOpen = false
const LogOpen = false
const OpenLoggingLock = false

var once sync.Once

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if LogOpen {
		log.Printf(format, a...)
	}
	return
}

func Debug(format string, v ...interface{}) {
	logMessage("DEBUG", fmt.Sprintf(format, v...))
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
	if !LogOpen && (level == "INFO" || level == "WARN" || level == "DEBUG") {
		return
	}

	if !DebugOpen && level == "DEBUG" {
		return
	}

	if level == "FATAL" {
		log.Fatalf("[%s:%d] [%d] [%s] %s\n", file, line, pid, level, message)
	} else {
		log.Printf("[%s:%d] [%d] [%s] %s\n", file, line, pid, level, message)
	}
}

type LoggingMutex struct {
	m       sync.Mutex
	Me      int
	Locklog []string
}

func (lm *LoggingMutex) Lock() {
	if OpenLoggingLock {
		_, file, line, _ := runtime.Caller(1)
		lm.Locklog = append(lm.Locklog, fmt.Sprintf("[%d] [%d] Locking from %s:%d",
			time.Now().Second(), lm.Me, filepath.Base(file), line))
		Info("Lockpeer[%d]:lock %s", lm.Me, lm.Locklog[len(lm.Locklog)-1])
	}
	lm.m.Lock()
}

func (lm *LoggingMutex) Unlock() {
	if OpenLoggingLock {
		_, file, line, _ := runtime.Caller(1)
		lm.Locklog = append(lm.Locklog, fmt.Sprintf("[%d] [%d] Unlocking from %s:%d",
			time.Now().Second(), lm.Me, filepath.Base(file), line))
		Info("Lockpeer[%d]:unlock %s", lm.Me, lm.Locklog[len(lm.Locklog)-1])
	}
	lm.m.Unlock()
}
