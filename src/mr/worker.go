package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	workerId := os.Getpid()

	// loop to get task or exit if all works are done
	for {
		// get Map or Reduce task from coordinator
		task := FetchTask(workerId)

		// execute task
		switch task.TaskType {
		case TaskTypeNone:
			continue
		case TaskTypeMap:
			log.Printf("start map task:%+v", task)
			contents := readAll(task.FileName)
			kva := mapf(task.FileName, contents)
			sort.Sort(ByKey(kva))
			writeIntermediate(kva, task.X, task.NReduce)

			// report task result
		case TaskTypeReduce:
			log.Printf("start reduce task:%+v", task)
			//todo reducef(key, values)
			// todoreport task result
		case TaskTypeWait:
			log.Printf("no task for the moment, sleep")
			time.Sleep(10 * time.Second)
		case TaskTypeExit:
			log.Printf("all task done, exit")
			return
		}
	}
}

// write key-value arrays to intermediate files: mr-Y-X
func writeIntermediate(intermediate []KeyValue, X int, NReduce int) []string {

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-Y-X.
	intermediateFiles := make([]string, NReduce)
	tempFiles := make([]*os.File, NReduce)
	for Y := 0; Y < NReduce; Y++ {
		intermediateFiles[Y] = fmt.Sprintf("mr-%d-%d", Y, X)
		temp, _ := os.CreateTemp("mr-tmp", intermediateFiles[Y]+"-*")
		tempFiles[Y] = temp
	}

	// write kv entries to temp files of mr-Y-X
	for _, kv := range intermediate {
		Y := ihash(kv.Key) % NReduce
		_, err := fmt.Fprintf(tempFiles[Y], "%v\n", kv.Key)
		if err != nil {
			panic(err)
		}
	}

	for Y := 0; Y < NReduce; Y++ {
		tempFiles[Y].Close()
		if err := os.Rename(tempFiles[Y].Name(), intermediateFiles[Y]); err != nil {
			panic(err)
		}
	}

	return intermediateFiles
}

func readAll(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func FetchTask(workerId int) FetchTaskReply {

	// declare an argument structure.
	args := FetchTaskArgs{WorkerId: workerId}

	// declare a reply structure.
	reply := FetchTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.FetchTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply %+v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
