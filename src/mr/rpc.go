package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType uint8

const (
	MAP    TaskType = 0x01
	REDUCE TaskType = 0x02
	NONE   TaskType = 0x03
)

type WorkerRequest struct {
}

type WorkerRequestReply struct {
	File       string
	TaskNumber int
	NReduce    int
	TaskType   TaskType
	Split      int
}

type WorkerResponse struct {
	TaskType   TaskType
	TaskNumber int
}

type WorkerResponseReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
