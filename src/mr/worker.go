package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	guuid "github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker continuously calls the coordinator for new tasks to execute until
// the coordinator either errors or indicates there is no more work to do.
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Create a unique ID for this worker, currently only used for logging purposes
	workerID := guuid.NewString()
	for {
		// RPC call to get task to work on
		task, callFailed := RequestTask()
		// Uncomment for debugging
		// fmt.Printf("worker-%s: taskType: %s, mNum: %d, rNum: %d\n", workerID, task.TaskType, task.MapTaskNum, task.ReduceTaskNum)

		// Work may be done or there was an error, return
		if callFailed {
			fmt.Printf("Worker-%s: call to coordinator failed, exiting.\n", workerID)
			return
		}
		switch task.TaskType {
		case DoneTask:
			// No more work to do, return
			fmt.Printf("Worker-%s: all tasks finished, exiting.\n", workerID)
			return
		case MapTask:
			// Handle map task
			WorkMap(task, mapf)
		case ReduceTask:
			// Handle reduce task
			WorkReduce(task, reducef)
		case WaitTask:
			// Let other workers finish before requesting a new task
			time.Sleep(500 * time.Millisecond)
		default:
			// Error case, return
			fmt.Printf("Worker-%s: received unknown TaskType: %s, exiting.\n", workerID, task.TaskType)
			return
		}
	}
}

// WorkMap maps a task using the given map function.
func WorkMap(task RequestTaskReply, mapf func(string, string) []KeyValue) {
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	mapTaskNum := task.MapTaskNum
	nReduce := task.NReduce

	kva := mapf(filename, string(content))
	buckets := make([]IntTasks, 0)
	intFileKvs := make(map[int][]KeyValue)

	// Map each keyvalue pair to a bucket
	// Initially I tried to create/append to intermediate files on the fly
	// save program memory by needing to not create intFileKvs,
	// but that is ultimately much slower on the test datasets and causes
	// inconsistencies with the total counts on my machine (unless each
	// worker is always forced to wait after each task, which is not an
	// acceptable fix). My guess is the inconsistencies are related to the
	// massive amounts of various file opening/closing, but wasn't able to
	// fully debug the issue.
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % nReduce
		intFileKvs[reduceTaskNum] = append(intFileKvs[reduceTaskNum], kv)
	}
	// Create and write to intermediate files one at a time
	for rNum, vals := range intFileKvs {
		intFileName := fmt.Sprintf("mr-%d-%d", mapTaskNum, rNum)
		intTask := IntTasks{
			FileName:  intFileName,
			MapNum:    mapTaskNum,
			ReduceNum: rNum,
		}
		buckets = append(buckets, intTask)
		intFile, err := os.Create(intFileName)
		if err != nil {
			fmt.Println("error opening file, panicing")
			panic(err)
		}
		enc := json.NewEncoder(intFile)
		for _, kv := range vals {
			err = enc.Encode(&kv)
			if err != nil {
				// TODO: handle error
				fmt.Println("encoding err")
			}
		}
		intFile.Close()
	}
	ReturnMapResponse(buckets, mapTaskNum, filename)
}

// WorkReduce reduces a task using the given reduce function.
func WorkReduce(task RequestTaskReply, reducef func(string, []string) string) {
	kva := make(map[string][]string)
	for _, filename := range task.ReduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// TODO: This assumes err is an EOF, if it's not
				// we should handle the different err properly
				break
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
		file.Close()
	}
	oname := fmt.Sprintf("mr-out-%d", task.ReduceTaskNum)
	ofile, err := os.Create(oname)
	if err != nil {
		panic(err)
	}
	for key, vals := range kva {
		output := reducef(key, vals)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	ofile.Close()
	ReturnReduceResponse(task.ReduceTaskNum)
}

// RequestTask sends an RPC call to the coordinator for a new task.
func RequestTask() (RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("Coordinator.RequestTask call failed!\n")
		return reply, true
	}
	return reply, false
}

// ReturnMapResponse sends an RPC to the coordinator indicating the
// completion of a map task.
func ReturnMapResponse(buckets []IntTasks, id int, fileName string) {
	args := MapResponseArgs{Buckets: buckets, ID: id}
	reply := MapResponseReply{}
	ok := call("Coordinator.FinishMap", &args, &reply)
	if !ok {
		fmt.Printf("Coordinator.FinishMap call failed!\n")
	}
}

// ReturnReduceResponse sends an RPC to the coordinator indicating the
// completion of a reduce task.
func ReturnReduceResponse(reduceN int) {
	args := ReduceResponseArgs{ReduceN: reduceN}
	reply := ReduceResponseReply{}
	ok := call("Coordinator.FinishReduce", &args, &reply)
	if !ok {
		fmt.Printf("Coordinator.FinishReduce call failed!\n")
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

	fmt.Printf("call error: %s\n:", err)
	return false
}
