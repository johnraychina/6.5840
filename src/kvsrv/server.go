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
	mu sync.Mutex

	//kvmap sync.Map
	kvmap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.kvmap[args.Key]; ok {
		reply.Value = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	old := kv.kvmap[args.Key]
	kv.kvmap[args.Key] = args.Value
	reply.Value = old
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	old := kv.kvmap[args.Key]
	kv.kvmap[args.Key] = old + args.Value
	reply.Value = old
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// init
	kv.kvmap = make(map[string]string)

	return kv
}
