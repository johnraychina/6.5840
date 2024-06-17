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

type FetchTaskArgs struct {
	WorkerId int // WorkerId, also as reducer index Y
}

type FetchTaskReply struct {
	TaskType int // 0-no task, 1-map, 2-reduce
	FileName string
	X        int // during mapping, we need index of reduce task to merge mr-Y-[X]
	NReduce  int // during mapping, we need reduce task counts to split to mr-[Y]-X
	Y        int // during reducing, we need index of reduce task to merge mr-[Y]-*
}

const TaskTypeNone = 0
const TaskTypeMap = 1
const TaskTypeReduce = 2
const TaskTypeWait = 10
const TaskTypeExit = 999

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/6.5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
