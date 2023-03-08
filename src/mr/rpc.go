package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	WaitTask   = "wait"
	DoneTask   = "done"
)

// Add your RPC definitions here.
type RequestTaskArgs struct{}

type RequestTaskReply struct {
	FileName      string
	ReduceFiles   []string
	MapTaskNum    int
	ReduceTaskNum int
	NReduce       int
	TaskType      string
}

type MapResponseArgs struct {
	ID      int
	Buckets []IntTasks
}
type IntTasks struct {
	FileName  string
	MapNum    int
	ReduceNum int
}
type MapResponseReply struct{}

type ReduceResponseArgs struct {
	ReduceN int
}
type ReduceResponseReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
