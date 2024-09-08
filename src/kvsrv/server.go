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

type KVServer struct {
	mu       sync.Mutex
	stor     map[string]string
	fallback map[int64]ClientLast
}

type ClientLast struct {
	requsetId int
	result    string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	prevReq := kv.fallback[args.ClientId]
	if args.RequsetId == prevReq.requsetId {
		reply.Value = prevReq.result
		return
	}
	val := kv.stor[args.Key]
	reply.Value = val
	prevReq.requsetId, prevReq.result = args.RequsetId, val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	prevReq := kv.fallback[args.ClientId]
	if args.RequsetId == prevReq.requsetId {
		reply.Value = prevReq.result
		return
	}
	kv.stor[args.Key] = args.Value
	prevReq.requsetId, prevReq.result = args.RequsetId, ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	prevReq := kv.fallback[args.ClientId]
	if args.RequsetId == prevReq.requsetId {
		reply.Value = prevReq.result
		return
	}
	reply.Value = kv.stor[args.Key]
	kv.stor[args.Key] += args.Value
	prevReq.requsetId, prevReq.result = args.RequsetId, reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.stor = make(map[string]string)
	kv.fallback = make(map[int64]ClientLast)
	return kv
}
