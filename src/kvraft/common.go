package kvraft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
)

// Debugging
const DebugOpen = false
const LogOpen = false

var once sync.Once

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

const (
	OK           = "OK"
	ErrNoKey     = "ErrNoKey"
	ErrNotLeader = "ErrNotLeader"
	ErrUnknown   = "ErrUnknown"
)

type Err string

const (
	PutAppendRPC = "KVServer.PutAppend"
	GetRPC       = "KVServer.Get"
	IsLeaderRPC  = "KVServer.IsLeader"

	OpNil    = "NilOp"
	OpPut    = "Put"
	OpAppend = "Append"
)

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	Serial   int64
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key    string
	Serial int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type GetLeaderArgs struct{}

type GetLeaderReply struct {
	IsLeader bool
}
