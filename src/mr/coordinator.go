package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// task count
	NMap    int
	NReduce int

	// task status management
	mu               sync.Mutex   // to protect the following global states
	mapTaskStatus    []TaskStatus // init, doing, done
	reduceTaskStatus []TaskStatus // init, doing, done

	// task distribute
	inputFiles   []string
	mapFileCh    chan int // file index to do map
	reduceTaskCh chan int // reducer ids
}

type TaskStatus int

const (
	TaskStatusInit = iota
	TaskStatusDoing
	TaskStatusDone
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {

	select {
	case X := <-c.mapFileCh:
		// get map task
		reply.TaskType = TaskTypeMap
		reply.FileName = c.inputFiles[X]
		reply.X = X
		reply.NReduce = c.NReduce
	default:

		// maybe all map task is processing
		// all map task must finish until reduce task can start
		if !c.allMapTaskDone() {
			reply.TaskType = TaskTypeWait
			return nil
		}

		// if all map task done, check reduce tasks
		select {
		// if no map task, get reduce task
		case reducerId := <-c.reduceTaskCh:
			reply.TaskType = TaskTypeReduce
			reply.Y = reducerId
		default:
			// if no reduce task return
			reply.TaskType = TaskTypeNone
		}

	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) allMapTaskDone() bool {
	for _, s := range c.mapTaskStatus {
		if s != TaskStatusDone {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	nMap := len(files)

	c := Coordinator{
		NMap:             nMap,
		NReduce:          nReduce,
		mapTaskStatus:    make([]TaskStatus, nMap),
		reduceTaskStatus: make([]TaskStatus, nReduce),
		mapFileCh:        make(chan int, nMap),
		reduceTaskCh:     make(chan int, nReduce),
		inputFiles:       files,
	}

	// Your code here.
	for i := range files {
		c.mapFileCh <- i
	}

	c.server()
	return &c
}
