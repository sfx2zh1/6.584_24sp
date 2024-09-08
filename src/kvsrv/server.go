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
	mu         sync.Mutex
	stor       map[string]string
	lastReq    map[int64]int
	lastResult map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.RequsetId == kv.lastReq[args.ClientId] {
		reply.Value = kv.lastResult[args.ClientId]
		return
	}
	val := kv.stor[args.Key]
	reply.Value = val
	kv.lastReq[args.ClientId], kv.lastResult[args.ClientId] = args.RequsetId, reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.RequsetId == kv.lastReq[args.ClientId] {
		reply.Value = kv.lastResult[args.ClientId]
		return
	}
	kv.stor[args.Key] = args.Value
	kv.lastReq[args.ClientId], kv.lastResult[args.ClientId] = args.RequsetId, reply.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.RequsetId == kv.lastReq[args.ClientId] {
		reply.Value = kv.lastResult[args.ClientId]
		return
	}
	reply.Value = kv.stor[args.Key]
	kv.stor[args.Key] += args.Value
	kv.lastReq[args.ClientId], kv.lastResult[args.ClientId] = args.RequsetId, reply.Value

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.stor = make(map[string]string)
	kv.lastReq = make(map[int64]int)
	kv.lastResult = make(map[int64]string)
	return kv
}
