package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	NotStarted = iota
	InProgress
	Finished
)

type Coordinator struct {
	// Your definitions here.
	numMapTasks int
	nReduce     int

	mapTasksInProgress    int
	reduceTasksInProgress int

	mapProgress    map[int]*MapJob
	reduceProgress map[int]*ReduceJob

	finishedMaps    int
	finishedReduces int
	mapFinished     bool
	reduceFinished  bool

	mux sync.Mutex
}

type MapJob struct {
	FileName string
	Status   int
	// Used in timeout function to signal completion.
	// struct{} is used as we don't care about sending a particular value.
	FinishSignal chan struct{}
}

type ReduceJob struct {
	Files  []string
	Status int
	// Used in timeout function to signal completion.
	// struct{} is used as we don't care about sending a particular value.
	FinishSignal chan struct{}
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// Check if there are un-run map tasks
	if mapJobID, fileName, mapJobToRun := c.fetchMapJob(); mapJobToRun {
		reply.TaskType = MapTask
		reply.FileName = fileName
		reply.NReduce = c.nReduce
		reply.MapTaskNum = mapJobID
		// Fire off a timer in another thread to track worker failure
		go c.timeMapJob(mapJobID)
		return nil
	}

	// Check if maps are still completing (and may need to be re-run)
	if c.waitForMapFinish() {
		// Tell worker to wait for maps to finish
		reply.TaskType = WaitTask
		return nil
	}

	// Check if there are un-run reduce tasks
	if reduceID, bucket, reduceJobToRun := c.fetchReduceJob(); reduceJobToRun {
		reply.TaskType = ReduceTask
		reply.ReduceTaskNum = reduceID
		reply.ReduceFiles = bucket
		// Fire off a timer in another thread to track worker failure
		go c.timeReduceJob(reduceID)
		return nil
	}

	// Check if reduces are still completing (and may need to be re-run)
	if c.waitForReduceFinish() {
		// Tell worker to wait for reduces to finish
		reply.TaskType = WaitTask
		return nil
	}

	reply.TaskType = DoneTask
	return nil
}

func (c *Coordinator) fetchMapJob() (int, string, bool) {
	found := false
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.mapFinished {
		return -1, "", found
	}
	for id, job := range c.mapProgress {
		//fmt.Println(job)
		if job.Status == NotStarted {
			c.mapProgress[id].Status = InProgress
			c.mapTasksInProgress++
			found = true
			return id, job.FileName, found
		}
	}
	return -1, "", found
}

func (c *Coordinator) waitForMapFinish() bool {
	wait := false
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.mapTasksInProgress > 0 {
		wait = true
	}
	return wait
}

func (c *Coordinator) fetchReduceJob() (int, []string, bool) {
	found := false
	c.mux.Lock()
	defer c.mux.Unlock()
	for id, job := range c.reduceProgress {
		if job.Status == NotStarted {
			c.reduceProgress[id].Status = InProgress
			c.reduceTasksInProgress++
			found = true
			return id, c.reduceProgress[id].Files, found
		}
	}
	return -1, nil, found
}

func (c *Coordinator) waitForReduceFinish() bool {
	wait := false
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.reduceTasksInProgress > 0 {
		wait = true
	}
	return wait
}

func (c *Coordinator) timeMapJob(id int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	// This introduces data race
	// defer close(c.mapProgress[id].FinishSignal)
	for {
		select {
		case <-c.mapProgress[id].FinishSignal:
			// Task signaled finished, exit early
			return
		case <-ticker.C:
			c.mux.Lock()
			defer c.mux.Unlock()
			c.mapProgress[id].Status = NotStarted
			c.mapTasksInProgress--
			return
		default:
			// Could sleep a bit here to reduce how often this loop
			// is checking
		}
	}
}

func (c *Coordinator) timeReduceJob(id int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	// This introduces data race
	// defer close(c.reduceProgress[id].FinishSignal)
	for {
		select {
		case <-c.reduceProgress[id].FinishSignal:
			// Task signaled finished, exit early
			return
		case <-ticker.C:
			c.mux.Lock()
			defer c.mux.Unlock()
			c.reduceProgress[id].Status = NotStarted
			c.reduceTasksInProgress--
			return
		default:
			// Could sleep a bit here to reduce how often this loop
			// is checking
		}
	}
}

func (c *Coordinator) FinishMap(args *MapResponseArgs, reply *MapResponseReply) error {
	c.mux.Lock()
	c.mapProgress[args.ID].Status = Finished

	for _, bucket := range args.Buckets {
		c.reduceProgress[bucket.ReduceNum].Files = append(c.reduceProgress[bucket.ReduceNum].Files, bucket.FileName)
	}
	c.mapTasksInProgress--
	c.finishedMaps++
	if c.finishedMaps == c.numMapTasks {
		c.mapFinished = true
	}
	c.mux.Unlock()
	c.mapProgress[args.ID].FinishSignal <- struct{}{}
	// Since this seems to work does it mean the channel can still receive after it is closed?
	// That is - does close block sending only?
	// See: The Channel Closing Principle
	// close(c.mapProgress[args.ID].FinishSignal)
	return nil
}

func (c *Coordinator) FinishReduce(args *ReduceResponseArgs, reply *ReduceResponseReply) error {
	c.mux.Lock()
	c.reduceProgress[args.ReduceN].Status = Finished
	c.reduceTasksInProgress--
	c.finishedReduces++
	if c.finishedReduces == c.nReduce {
		c.reduceFinished = true
	}
	c.mux.Unlock()
	c.reduceProgress[args.ReduceN].FinishSignal <- struct{}{}
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
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.mapFinished && c.reduceFinished {
		fmt.Println("DONE!")
	}
	return c.mapFinished && c.reduceFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		numMapTasks:     len(files),
		nReduce:         nReduce,
		mapFinished:     false,
		finishedMaps:    0,
		finishedReduces: 0,
		reduceFinished:  false,

		mapTasksInProgress:    0,
		reduceTasksInProgress: 0,
	}

	c.mapProgress = make(map[int]*MapJob)
	c.reduceProgress = make(map[int]*ReduceJob)

	for i, file := range files {
		c.mapProgress[i] = &MapJob{
			FileName:     file,
			Status:       NotStarted,
			FinishSignal: make(chan struct{}, 1),
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceProgress[i] = &ReduceJob{
			Status:       NotStarted,
			FinishSignal: make(chan struct{}, 1),
		}
	}

	fmt.Printf("num files: %d\n", len(files))

	c.server()
	return &c
}
