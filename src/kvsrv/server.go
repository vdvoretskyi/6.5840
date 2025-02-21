package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type clientTransaction struct {
	id    int
	value string
}

type KVServer struct {
	mu      sync.Mutex
	store   map[string]string
	history map[int64]*clientTransaction
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Get %s", args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.store[args.Key]

	DPrintf("Get %s -> %s", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Put %s %s", args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, exists := kv.findDuplicate(args.ClientId, args.MessageID); exists {
		reply.Value = value
		return
	}
	reply.Value = kv.store[args.Key]
	kv.store[args.Key] = args.Value
	kv.addTransaction(args.ClientId, args.MessageID, reply.Value)

	DPrintf("Put %s %s -> %s", args.Key, args.Value, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Append %s %s", args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, exists := kv.findDuplicate(args.ClientId, args.MessageID); exists {
		reply.Value = value
		return
	}

	reply.Value = kv.store[args.Key]
	kv.store[args.Key] += args.Value
	kv.addTransaction(args.ClientId, args.MessageID, reply.Value)

	DPrintf("Append %s %s -> %s", args.Key, args.Value, reply.Value)
}

func (kv *KVServer) CompleteOp(args *CompleteArgs, reply *CompleteReply) {
	DPrintf("Complete operation %v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.history, args.ClientId)
}

func (kv *KVServer) findDuplicate(clientId int64, messageId int) (string, bool) {
	if transaction, exists := kv.history[clientId]; exists {
		if transaction.id == messageId {
			return transaction.value, true
		}
	}
	return "", false
}

func (kv *KVServer) addTransaction(clientId int64, messageId int, value string) {
	if transaction, exists := kv.history[clientId]; exists {
		transaction.id = messageId
		transaction.value = value
	} else {
		kv.history[clientId] = &clientTransaction{id: messageId, value: value}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.history = make(map[int64]*clientTransaction)
	return kv
}
