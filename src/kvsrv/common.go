package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	ClientId  int64
	RequsetId int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequsetId int
}

type GetReply struct {
	Value string
}
