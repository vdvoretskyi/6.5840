package kvsrv

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server    *labrpc.ClientEnd
	clientId  int64
	messageId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = nrand()
	ck.messageId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.messageId++
	defer ck.server.Call("KVServer.CompleteOp", &CompleteArgs{ClientId: ck.clientId, Uid: ck.messageId}, &CompleteReply{})

	args := &GetArgs{Key: key, ClientId: ck.clientId, Uid: ck.messageId}
	for {
		var reply GetReply
		ok := ck.server.Call("KVServer.Get", args, &reply)
		if ok {
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.messageId++
	defer ck.server.Call("KVServer.CompleteOp", &CompleteArgs{ClientId: ck.clientId, Uid: ck.messageId}, &CompleteReply{})

	args := &PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, MessageID: ck.messageId}
	for {
		reply := &PutAppendReply{}
		ok := ck.server.Call("KVServer."+op, args, reply)
		if ok {
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
