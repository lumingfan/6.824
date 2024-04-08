package kvraft

import (
	"src/labgob"
	"src/labrpc"
	"log"
	"src/raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op_name string
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	key_value map[string]string

	applied_command_idx int
	apply_cv sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()	
	defer kv.mu.Unlock()

	op := Op{
		Op_name: "Get",
		Key:     args.Key,
		Value:   "",
	}

	DPrintf("leader %d received a Get RPC call with key: %s", kv.me, args.Key)

	idx, _, is_leader := kv.rf.Start(op)
	reply.Is_leader = is_leader

	if !is_leader {
		return 
	}

	DPrintf("leader %d start a Get RPC call with key: %s at index %d", kv.me, args.Key, idx)

	// can't use idx != kv.applied_command_idx 
	// since with mesa schedule, this threads may win the lock after kv.applied_command_idx updated again
	for idx > kv.applied_command_idx {
		kv.apply_cv.Wait()
	}

	DPrintf("leader %d commit a Get RPC call with key: %s at index %d", kv.me, args.Key, idx)

	reply.Value = kv.key_value[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Op_name: args.Op,
		Key:     args.Key,
		Value:   args.Value,
	}

	DPrintf("leader %d received a PutAppend RPC call with args: %v", kv.me, args)
	
	idx, _, is_leader := kv.rf.Start(op)
	reply.Is_leader = is_leader

	if !is_leader {
		return 
	}

	DPrintf("leader %d start a PutAppend RPC call with args: %v", kv.me, args)

	// can't use idx != kv.applied_command_idx 
	// since with mesa schedule, this threads may win the lock after kv.applied_command_idx updated again
	for idx > kv.applied_command_idx {
		kv.apply_cv.Wait()
	}

	if args.Op == "Put" {
		kv.key_value[args.Key] = args.Value
	} else if args.Op == "Append" {
		kv.key_value[args.Key] += args.Value
	} else {
		panic("doesn't support operation: " + args.Op)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := &KVServer{
		mu:                  sync.Mutex{},
		me:                  me,
		rf:                  nil,
		applyCh:             make(chan raft.ApplyMsg),
		dead:                0,
		maxraftstate:        maxraftstate,
		key_value:           map[string]string{},
		applied_command_idx: -1,
		apply_cv:            sync.Cond{},
	}

	// You may need initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.apply_cv = sync.Cond{
		L: &kv.mu,
	}

	go kv.waitApplyChannel()
	return kv
}


// helper function

// caller can't hold kv.mu.Lock
func (kv *KVServer) waitApplyChannel() {
	for apply_msg := range kv.applyCh{
		if !apply_msg.CommandValid {

		} else {
			DPrintf("%v committed", apply_msg)
			kv.mu.Lock()
			DPrintf("notify %v committed", apply_msg)
			kv.applied_command_idx = apply_msg.CommandIndex
			kv.apply_cv.Broadcast()
			kv.mu.Unlock()
			DPrintf("notify %v committed finished", apply_msg)
		}

		if kv.killed() {
			break
		}
	}
}


// caller must hold kv.mu.Lock
