package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type taskStatus uint8

const (
	IDLE        taskStatus = 0x01
	IN_PROGRESS taskStatus = 0x02
	COMPLETED   taskStatus = 0x03
)

type task struct {
	mutex     sync.Mutex
	filename  string
	status    taskStatus
	timestamp time.Time
}

type Coordinator struct {
	mutex             sync.Mutex
	mapTasks          []*task
	reduceTasks       []*task
	mapTasksRemain    int
	reduceTasksRemain int
}

func waitForTask(task *task) {
	for {
		time.Sleep(500 * time.Millisecond)

		if task.status == COMPLETED {
			fmt.Fprintf(os.Stdout, "%s Coordinator: task %s completed\n", time.Now().String(), task.filename)
			break
		}
		if time.Since(task.timestamp) > 10*time.Second {
			task.mutex.Lock()
			task.status = IDLE
			fmt.Fprintf(os.Stderr, "%s Coordinator: task %s failed, "+"re-allocate to other workers\n", time.Now().String(), task.filename)
			task.mutex.Unlock()
			break
		}
	}
}

func (c *Coordinator) Req(args *WorkerRequest, reply *WorkerRequestReply) error {
	reply.TaskType = NONE

	if c.mapTasksRemain > 0 {
		for i, mTask := range c.mapTasks {
			mTask.mutex.Lock()
			defer mTask.mutex.Unlock()
			if mTask.status == IDLE {
				mTask.status = IN_PROGRESS
				reply.TaskType = MAP
				reply.File = mTask.filename
				reply.NReduce = len(c.reduceTasks)
				reply.TaskNumber = i
				mTask.timestamp = time.Now()
				go waitForTask(mTask)
				break
			}
		}
	} else {
		for i, rTask := range c.reduceTasks {
			rTask.mutex.Lock()
			defer rTask.mutex.Unlock()
			if rTask.status == IDLE {
				rTask.status = IN_PROGRESS
				reply.TaskType = REDUCE
				reply.Split = len(c.mapTasks)
				reply.TaskNumber = i
				rTask.timestamp = time.Now()
				go waitForTask(rTask)
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) Resp(args *WorkerResponse, reply *WorkerResponseReply) error {
	now := time.Now()
	var task *task
	if args.TaskType == MAP {
		task = c.mapTasks[args.TaskNumber]
	} else {
		task = c.reduceTasks[args.TaskNumber]
	}

	if now.Before(task.timestamp.Add(10 * time.Second)) {
		task.mutex.Lock()
		task.status = COMPLETED
		task.mutex.Unlock()
		c.mutex.Lock()
		if args.TaskType == MAP {
			c.mapTasksRemain--
		} else {
			c.reduceTasksRemain--
		}
		c.mutex.Unlock()
	}

	return nil
}

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

func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.reduceTasksRemain == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mutex = sync.Mutex{}
	c.mapTasksRemain = len(files)
	c.mapTasks = make([]*task, c.mapTasksRemain)
	c.reduceTasksRemain = nReduce
	c.reduceTasks = make([]*task, c.reduceTasksRemain)

	for i, file := range files {
		c.mapTasks[i] = new(task)
		c.mapTasks[i].mutex = sync.Mutex{}
		c.mapTasks[i].filename = file
		c.mapTasks[i].status = IDLE
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = new(task)
		c.reduceTasks[i].mutex = sync.Mutex{}
		c.reduceTasks[i].status = IDLE
	}

	c.server()
	return &c
}
