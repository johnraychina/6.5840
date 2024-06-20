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

	reqCache map[int64]*ReqReply // [client id] -> {req id, value}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// hit cache
	if cache, ok := kv.reqCache[args.ClientId]; ok {
		if cache.ReqId == args.ReqId {
			reply.Value = cache.Value
			return
		}
	}

	if val, ok := kv.kvmap[args.Key]; ok {
		reply.Value = val
	}

	// refresh cache
	kv.reqCache[args.ClientId] = &ReqReply{ReqId: args.ReqId, Value: reply.Value}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// hit cache
	if cache, ok := kv.reqCache[args.ClientId]; ok {
		if cache.ReqId == args.ReqId {
			reply.Value = cache.Value
			return
		}
	}

	old := kv.kvmap[args.Key]
	kv.kvmap[args.Key] = args.Value
	reply.Value = old

	// refresh cache
	kv.reqCache[args.ClientId] = &ReqReply{ReqId: args.ReqId, Value: reply.Value}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// hit cache
	if cache, ok := kv.reqCache[args.ClientId]; ok {
		if cache.ReqId == args.ReqId {
			reply.Value = cache.Value
			return
		}
	}

	old := kv.kvmap[args.Key]
	kv.kvmap[args.Key] = old + args.Value
	reply.Value = old

	// refresh cache
	kv.reqCache[args.ClientId] = &ReqReply{ReqId: args.ReqId, Value: reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// init
	kv.kvmap = make(map[string]string)
	kv.reqCache = make(map[int64]*ReqReply)

	return kv
}
