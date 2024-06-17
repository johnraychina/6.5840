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
	mapTaskCnt    int
	reduceTaskCnt int

	// task status management
	mu               sync.Mutex   // to protect the following global states
	mapTaskStatus    []TaskStatus // init, doing, done
	reduceTaskStatus []TaskStatus // init, doing, done

	// task distribute
	mapTaskCh    chan int   // file[X], index to do map
	reduceTaskCh []chan int // mr-Y-X, file index to do reduce
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
	case mapTaskIdx := <-c.mapTaskCh:
		// get map task
		reply.TaskType = TaskTypeMap
		reply.X = mapTaskIdx
	default:

		// maybe all map task is processing
		// all map task must finish until reduce task can start
		if !c.allMapTaskDone() {
			reply.TaskType = TaskTypeWait
			return nil
		}

		// if all map task done, check reduce tasks
		if args.WorkerId < len(c.reduceTaskCh) {
			select {
			// if no map task, get reduce task
			case reduceTaskIdx := <-c.reduceTaskCh[args.WorkerId]:
				reply.TaskType = TaskTypeReduce
				reply.Y = args.WorkerId
				reply.X = reduceTaskIdx
			default:
				// if no reduce task return
				reply.TaskType = TaskTypeNone
			}
		} else {
			// no task for you
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
		mapTaskCnt:       nMap,
		reduceTaskCnt:    nReduce,
		mapTaskStatus:    make([]TaskStatus, nMap),
		reduceTaskStatus: make([]TaskStatus, nReduce),
		mapTaskCh:        make(chan int, nMap),
		reduceTaskCh:     make([]chan int, nReduce),
	}

	// Your code here.
	for i := range files {
		c.mapTaskCh <- i
	}

	c.server()
	return &c
}
