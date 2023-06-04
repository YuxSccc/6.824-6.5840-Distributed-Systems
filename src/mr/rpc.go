package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	None TaskType = iota
	Map
	Reduce
)

type HeartBeatTaskDesc struct {
	TaskType  TaskType
	NReduce   int
	TaskIndex int
	// for map task InputPath use as filename, reduce task use as directory
	InputPath string
	// for map task OutputPath use as directory, reduce task use as filename
	OutputPath   string
	TaskIsFinish bool
}

type HeartBeatArgs struct {
	Pid           int
	CanAccessWork bool
	TaskDesc      HeartBeatTaskDesc
}

type HeartBeatReply struct {
	Pid       int
	Teardown  bool
	IsNewTask bool
	IsAck     bool
	TaskDesc  HeartBeatTaskDesc
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
