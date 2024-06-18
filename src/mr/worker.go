package mr

import (
	"encoding/json"
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

		//case TaskTypeNone:
		//	continue

		case TaskTypeMap:
			doMap(workerId, task, mapf)

		case TaskTypeReduce:
			doReduce(workerId, task, reducef)

		case TaskTypeWait:
			log.Printf("no task for the moment, sleep")
			time.Sleep(5 * time.Second)

		case TaskTypeExit:
			log.Printf("all task done, exit")
			return
		}
	}
}

func doReduce(workerId int, task FetchTaskReply, reducef func(string, []string) string) {
	log.Printf("start reduce task:%+v", task)

	defer func() {
		if e := recover(); e != nil {
			log.Printf("reduce task failed:%+v", e)
			ReportTask(workerId, task, TaskStatusInit) // task failed, reset to init
		}
	}()

	outFileName := fmt.Sprintf("mr-out-%d", task.Y)
	outFile, err := os.OpenFile(outFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	// for each files mr-Y-X
	// reduce to mr-Y.out
	var kva []KeyValue
	for X := 0; X < task.NMap; X++ {
		inFileName := fmt.Sprintf("mr-%d-%d", task.Y, X)

		// open file
		iFile, err1 := os.Open(inFileName)
		if err1 != nil {
			panic(err1)
		}

		// decode
		kva = append(kva, decode(iFile)...)
		iFile.Close()
	}

	reduceAll(kva, reducef, outFile)

	// report task result
	ReportTask(workerId, task, TaskStatusDone)
}

func reduceAll(kva []KeyValue, reducef func(string, []string) string, outFile *os.File) {

	sort.Sort(ByKey(kva))

	// group by key and reduce
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		_, err2 := fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		if err2 != nil {
			panic(err2)
		}
		i = j
	}
}

func decode(file *os.File) (kva []KeyValue) {
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func doMap(workerId int, task FetchTaskReply, mapf func(string, string) []KeyValue) {
	log.Printf("start map task:%+v", task)
	defer func() {
		if e := recover(); e != nil {
			log.Printf("map task failed:%+v", e)
			ReportTask(workerId, task, TaskStatusInit) // task failed, reset to init
		}
	}()

	contents := readAll(task.FileName)
	kva := mapf(task.FileName, contents)
	sort.Sort(ByKey(kva))
	writeIntermediate(kva, task.X, task.NReduce)

	// report task result
	ReportTask(workerId, task, TaskStatusDone)
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
		// encode kv to file
		enc := json.NewEncoder(tempFiles[Y])
		err := enc.Encode(kv)
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

func ReportTask(workerId int, task FetchTaskReply, status TaskStatus) ReportReply {

	// declare an argument structure.
	args := ReportArgs{
		WorkerId:   workerId,
		TaskType:   task.TaskType,
		X:          task.X,
		Y:          task.Y,
		TaskStatus: status,
		Cost:       0, //todo
	}

	// declare a reply structure.
	reply := ReportReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.ReportTask", &args, &reply)
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
