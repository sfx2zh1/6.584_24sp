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
	mu   sync.Mutex
	stor map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.stor[args.Key]
	if ok {
		reply.Value = val
		return
	}
	reply.Value = ""
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.stor[args.Key] = args.Value
	return
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.stor[args.Key]
	kv.stor[args.Key] += args.Value
	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.stor = make(map[string]string)

	return kv
}
