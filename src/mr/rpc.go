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

type ApplyTaskArgs struct {
}

type AssignTaskReply struct {
	Status      int  // 0: map task; 1: reduce task; 2: finished
	IsAvailable bool // true if there are available task in pool

	MapFileName string // map input file name
	MapTaskNum  int    // map task number
	NMap        int

	ReduceTaskNum int
	NReduce       int
}

type FinishTaskArgs struct {
	Type int // 0: map; 1: reduce
	Mtn  int // map task number
	Rtn  int // reduce task number
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
