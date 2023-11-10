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

// type ExampleArgs struct {
// 	X int
// }

//	type ExampleReply struct {
//		Y int
//	}
type KeyValue_ofmap struct {
	Key   int
	Value string
}

type KeyValue_ofreduce struct {
	Key   int
	Value []KeyValue
}

type KeyValue_ofconverge struct {
	Key  int
	Num1 int
	Num2 int
}

type Request struct {
	Action string
}

type Reply struct {
	Task     string
	NReduce  int
	Content  KeyValue_ofmap
	Content2 KeyValue_ofreduce
	Content3 KeyValue_ofconverge
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
