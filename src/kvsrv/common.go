package kvsrv

type PutAppendArgs struct {
	Key       string
	Value     string
	ClientId  int64
	MessageID int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key      string
	ClientId int64
	Uid      int
}

type CompleteArgs struct {
	ClientId int64
	Uid      int
}

type CompleteReply struct {
}

type GetReply struct {
	Value string
}
