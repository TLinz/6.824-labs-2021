package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mapTask struct {
	status   int // 0: not assigned; 1: excuting; 2: finished
	fileName string
}

type Coordinator struct {
	status int // 0: mapping; 1: reducing; 2: finished
	slock  sync.Mutex

	nr int // number of reduce tasks

	mapTaskPool []mapTask
	mfCounter   int
	mlock       sync.Mutex

	reduceTaskPool []int
	rfCounter      int
	rlock          sync.Mutex
}

func (c *Coordinator) assignMapTask() (string, int) {
	c.mlock.Lock()
	mtn := -1
	for i := range c.mapTaskPool {
		if (c.mapTaskPool)[i].status == 0 {
			mtn = i
			(c.mapTaskPool)[i].status = 1

			time.AfterFunc(10*time.Second, func() {
				c.mlock.Lock()
				defer c.mlock.Unlock()

				if (c.mapTaskPool)[i].status == 1 {
					(c.mapTaskPool)[i].status = 0
				}
			})

			c.mlock.Unlock()
			return (c.mapTaskPool)[i].fileName, mtn
		}
	}
	c.mlock.Unlock()
	return "", 0
}

func (c *Coordinator) assignReduceTask() int {
	c.rlock.Lock()
	rtn := -1
	for i := range c.reduceTaskPool {
		if (c.reduceTaskPool)[i] == 0 {
			rtn = i
			(c.reduceTaskPool)[i] = 1

			time.AfterFunc(10*time.Second, func() {
				c.rlock.Lock()
				defer c.rlock.Unlock()

				if (c.reduceTaskPool)[i] == 1 {
					(c.reduceTaskPool)[i] = 0
				}
			})

			c.rlock.Unlock()
			return rtn
		}
	}
	c.rlock.Unlock()
	return -1
}

func (c *Coordinator) AssignTask(args *ApplyTaskArgs, reply *AssignTaskReply) error {
	c.slock.Lock()
	defer c.slock.Unlock()

	reply.Status = c.status
	reply.NReduce = c.nr
	reply.NMap = len(c.mapTaskPool)
	switch c.status {
	case 0:
		reply.MapFileName, reply.MapTaskNum = c.assignMapTask()
		reply.IsAvailable = (reply.MapFileName != "")
	case 1:
		reply.ReduceTaskNum = c.assignReduceTask()
		reply.IsAvailable = (reply.ReduceTaskNum != -1)
	case 2:
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	switch args.Type {
	case 0:
		c.slock.Lock()
		if c.status != 0 {
			c.slock.Unlock()
			return errors.New("map phase has been completed")
		}
		c.slock.Unlock()

		mtn := args.Mtn
		c.mlock.Lock()
		if (c.mapTaskPool)[mtn].status == 1 {
			c.mfCounter++
			if c.mfCounter == len(c.mapTaskPool) {
				c.slock.Lock()
				c.status = 1
				c.slock.Unlock()
			}
			(c.mapTaskPool)[mtn].status = 2

			c.mlock.Unlock()
			return nil
		}
		c.mlock.Unlock()
		return errors.New("map phase has been completed")

	case 1:
		c.slock.Lock()
		if c.status != 1 {
			c.slock.Unlock()
			return errors.New("reduce phase has been completed")
		}
		c.slock.Unlock()

		rtn := args.Rtn
		c.rlock.Lock()
		if c.reduceTaskPool[rtn] == 1 {
			c.rfCounter++
			if c.rfCounter == len(c.reduceTaskPool) {
				c.slock.Lock()
				c.status = 2
				c.slock.Unlock()
			}
			(c.reduceTaskPool)[rtn] = 2

			c.rlock.Unlock()
			return nil
		}
		c.rlock.Unlock()
		return errors.New("reduce phase has been completed")
	}
	return errors.New("type not invalid")
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	c.slock.Lock()
	defer c.slock.Unlock()

	time.Sleep(200 * time.Millisecond)
	return c.status == 2
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.status = 0
	c.nr = nReduce
	c.mfCounter = 0

	for _, v := range files {
		c.mapTaskPool = append(c.mapTaskPool, mapTask{0, v})
	}

	c.reduceTaskPool = make([]int, nReduce)
	c.rfCounter = 0

	c.server()
	return &c
}
