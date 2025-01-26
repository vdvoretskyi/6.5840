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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func doMap(mapf func(string, string) []KeyValue, filename string, nReduce int, index int) bool {
	intermediate := make([][]KeyValue, nReduce)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s cannot open %s", time.Now().String(), filename)
		return false
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s cannot read %s", time.Now().String(), filename)
		return false
	}
	defer file.Close()

	kva := mapf(filename, string(content))
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	for i, kva := range intermediate {
		oldname := fmt.Sprintf("temp-mr-%d-%d.json", index, i)
		tempfile, err := os.CreateTemp("", oldname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not open temp file %v\n", time.Now().String(), oldname)
			return false
		}
		defer os.Remove(tempfile.Name())

		enc := json.NewEncoder(tempfile)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "%s Worker: map can not write to temp file %v\n", time.Now().String(), oldname)
				return false
			}
		}

		newname := fmt.Sprintf("mr-%d-%d.json", index, i)
		if err := os.Rename(tempfile.Name(), newname); err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not rename temp file %v\n", time.Now().String(), oldname)
			return false
		}
	}
	return true
}

func doReduce(reducef func(string, []string) string, split int, index int) bool {
	var intermediate []KeyValue

	for i := 0; i < split; i++ {
		filename := fmt.Sprintf("mr-%d-%d.json", i, index)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: can not read intermidiate file %v\n", time.Now().String(), filename)
			return false
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oldname := fmt.Sprintf("temp-mr-out-%d", index)
	tempfile, err := os.CreateTemp("", oldname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: reduce can not open temp file %v\n", time.Now().String(), oldname)
		return false
	}
	defer os.Remove(tempfile.Name())

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	newname := fmt.Sprintf("mr-out-%d", index)
	if err := os.Rename(tempfile.Name(), newname); err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: reduce can not rename temp file %v\n", time.Now().String(), oldname)
		return false
	}

	return true
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := WorkerRequest{}
		reply := WorkerRequestReply{}
		ok := call("Coordinator.Req", &args, &reply)
		if !ok {
			fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
			os.Exit(0)
		}

		if reply.TaskType == NONE {
			continue
		}

		responseArgs := WorkerResponse{}
		responseReply := WorkerResponseReply{}
		if reply.TaskType == MAP {
			if doMap(mapf, reply.File, reply.NReduce, reply.TaskNumber) {
				fmt.Fprintf(os.Stderr, "%s Worker: map task performed successfully\n", time.Now().String())
				responseArgs.TaskType = reply.TaskType
				responseArgs.TaskNumber = reply.TaskNumber
				if !call("Coordinator.Resp", &responseArgs, &responseReply) {
					fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
					os.Exit(0)
				}
			} else {
				fmt.Fprintf(os.Stderr, "%s Worker: map task failed\n", time.Now().String())
			}
		} else {
			if doReduce(reducef, reply.Split, reply.TaskNumber) {
				fmt.Fprintf(os.Stderr, "%s Worker: reduce task performed successfully\n", time.Now().String())
				responseArgs.TaskType = reply.TaskType
				responseArgs.TaskNumber = reply.TaskNumber
				if !call("Coordinator.Resp", &responseArgs, &responseReply) {
					fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
					os.Exit(0)
				}
			} else {
				fmt.Fprintf(os.Stderr, "%s Worker: reduce task failed\n", time.Now().String())
			}
		}

		time.Sleep(time.Second)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
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
