package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type task struct {
	State     int       //0 is unassigned, 1 is ongoing, 2 is done
	StartedAt time.Time //timestamp of starting to count 10 seconds
}
type Coordinator struct {
	mTasks        []task
	rTasks        []task
	files         []string
	nReduce       int
	isReducePhase bool //false for map phase, true for reduce phase
	lock          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(requestTask *RequestTask, reply *Reply) error {
	mapChanged := false
	reduceChanged := false
	if !c.isReducePhase {
		c.lock.Lock()
		for i := 0; i < len(c.mTasks); i++ {
			if c.mTasks[i].State == 0 || (c.mTasks[i].State == 1 && c.mTasks[i].StartedAt.Before(time.Now().Add(-time.Second*10))) {
				c.mTasks[i].State = 1
				c.mTasks[i].StartedAt = time.Now()
				reply.NReduce = c.nReduce
				reply.Filename = c.files[i]
				reply.TaskIdx = i
				reply.TaskType = 0
				mapChanged = true
				break
			}
		}
		if !mapChanged {
			reply.TaskType = 2
		}
		c.lock.Unlock()
	} else {
		c.lock.Lock()
		for i := 0; i < len(c.rTasks); i++ {
			if c.rTasks[i].State == 0 || (c.rTasks[i].State == 1 && c.rTasks[i].StartedAt.Before(time.Now().Add(-time.Second*10))) {
				c.rTasks[i].State = 1
				c.rTasks[i].StartedAt = time.Now()
				reply.NReduce = c.nReduce
				reply.TaskIdx = i
				reply.TaskType = 1
				reduceChanged = true
				break
			}
		}
		if !reduceChanged {
			reply.TaskType = 2
		}
		c.lock.Unlock()
	}
	return nil
}

func (c *Coordinator) TaskDoneReceive(args *TaskDone, reply *Reply) error {
	taskType := args.TaskType
	taskId := args.TaskIdx
	c.lock.Lock()
	if taskType == 0 && c.mTasks[taskId].State != 2 {
		c.mTasks[taskId].State = 2
	} else if taskType == 1 && c.rTasks[taskId].State != 2 {
		c.rTasks[taskId].State = 2
	}
	if c.isMapDone() {
		c.isReducePhase = true
		if c.isReduceDone() {
			reply.TaskType = 3
		}
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) isMapDone() bool {
	for i := 0; i < len(c.mTasks); i++ {
		if c.mTasks[i].State != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) isReduceDone() bool {
	for i := 0; i < len(c.rTasks); i++ {
		if c.rTasks[i].State != 2 {
			return false
		}
	}
	return true
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
	c.lock.Lock()
	if c.isMapDone() && c.isReduceDone() {
		time.Sleep(time.Second)
		ret = true
	}
	c.lock.Unlock()
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:         files,
		nReduce:       nReduce,
		mTasks:        make([]task, len(files)),
		rTasks:        make([]task, nReduce),
		isReducePhase: false,
	}
	c.server()
	return &c
}
