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

type ReportArgs struct {
	WorkerId int // who am I

	TaskType int // 0-no task, 1-map, 2-reduce
	X        int // map task id
	Y        int // reduce task id

	TaskStatus TaskStatus // task status
	Cost       int64      // time cost
}

type ReportReply struct {
	Ok  bool
	Msg string
}

type FetchTaskArgs struct {
	WorkerId int // who am I
}

type FetchTaskReply struct {
	TaskType int // 0-no task, 1-map, 2-reduce
	FileName string
	X        int // mapper task id to process files[X]
	NReduce  int // to iterate over reducers
	Y        int // reducer task id to merge mr-[Y]-X
	NMap     int // to iterate over N map files
}

// const TaskTypeNone = 0
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
