package kvraft

import (
	"bytes"
	"log"
	"src/labgob"
	"src/labrpc"
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
	Op_name  string // operation name: Get, Append, Put
	Key      string
	Value    string
	ClientId int // which client sends this Op, used to detect duplication
	SeqNo    int // which sequence number this Op corresponds used to detect duplication
}

/** RPC request information
 */
type RequestInfo struct {
	cv        *sync.Cond
	operation Op

	// reply is a pointer to PutAppendReply/GetReply
	// set by waitApplyChannel
	reply interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// key/value state
	key_value map[string]string

	// client id maps to latest sequence no
	seqnos map[int]int

	// rpc request information at a specific index
	request_infos map[int][]RequestInfo

	// last applied index
	last_applied_idx int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.RPCHandler("Get", args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.RPCHandler(args.Op, args, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := &KVServer{
		mu:               sync.Mutex{},
		me:               me,
		rf:               nil,
		applyCh:          make(chan raft.ApplyMsg),
		dead:             0,
		maxraftstate:     maxraftstate,
		key_value:        map[string]string{},
		seqnos:           map[int]int{},
		request_infos:    map[int][]RequestInfo{},
		last_applied_idx: 0,
	}

	// You may need initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.waitApplyChannel()
	return kv
}

/**---------------- Helper functions ---------------**/

/* ----- caller must hold kv.mu.Lock() ----- */

/** apply given operation to state machine (if not a duplication)
 */
func (kv *KVServer) applyToStateMachine(op Op, snapshot_idx int) {
	seqno, ok := kv.seqnos[op.ClientId]

	if !ok || (ok && op.SeqNo > seqno) {
		DPrintf("server %d submit operation: %v", kv.me, op)
		kv.seqnos[op.ClientId] = op.SeqNo

		if op.Op_name == "Put" {
			kv.key_value[op.Key] = op.Value
		} else if op.Op_name == "Append" {
			kv.key_value[op.Key] += op.Value
		}
	} else {
		DPrintf("server %d reject stale operation: %v", kv.me, op)
	}

	if kv.maxraftstate != -1 && kv.rf.ReadStateSize() >= kv.maxraftstate {
		kv.rf.SaveStateAndSnapshot(kv.key_value, kv.seqnos, snapshot_idx)
	}
}

/** signal rpc handlers wait at command_index
 */
func (kv *KVServer) signalRPCHandler(command_index int, op Op) {
	req_info_list := kv.request_infos[command_index]
	for idx := 0; idx < len(req_info_list); idx++ {
		if req_info_list[idx].operation == op {
			if op.Op_name == "Get" {
				req_info_list[idx].reply.(*GetReply).Is_leader = true
				req_info_list[idx].reply.(*GetReply).Value = kv.key_value[op.Key]
			} else {
				req_info_list[idx].reply.(*PutAppendReply).Is_leader = true
			}
		} else {
			if req_info_list[idx].operation.Op_name == "Get" {
				req_info_list[idx].reply.(*GetReply).Is_leader = false
				req_info_list[idx].reply.(*GetReply).Err = "leader has changed"
			} else {
				req_info_list[idx].reply.(*PutAppendReply).Is_leader = false
				req_info_list[idx].reply.(*PutAppendReply).Err = "leader has changed"
			}
		}
		req_info_list[idx].cv.Signal()
	}

	delete(kv.request_infos, command_index)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return 
	}

	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var key_value map[string]string
	var seqnos map[int]int

	if decoder.Decode(&key_value) != nil ||
	   decoder.Decode(&seqnos) != nil {
		panic("decoder error")
	} else {
		kv.key_value = key_value
		kv.seqnos = seqnos
	}
}


/* ----- caller can't hold kv.mu.Lock() ----- */

/** wait for raft to send apply_msg to applyCh
 *  then apply this operation to state machine(if not a duplication)
 *  and signal rpc handler
 */
func (kv *KVServer) waitApplyChannel() {
	for apply_msg := range kv.applyCh {
		kv.mu.Lock()
		if apply_msg.CommandIndex > kv.last_applied_idx {

			if !apply_msg.CommandValid {
				kv.readSnapshot(apply_msg.Snapshot)
			} else {
				op := apply_msg.Command.(Op)
				kv.applyToStateMachine(op, apply_msg.CommandIndex)
				kv.signalRPCHandler(apply_msg.CommandIndex, op)
			}

			kv.last_applied_idx = apply_msg.CommandIndex
		}
		kv.mu.Unlock()

		if kv.killed() {
			break
		}
	}
}

func (kv *KVServer) RPCHandler(op_name string, args interface{}, reply interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{}
	op.Op_name = op_name

	if op_name == "Get" {
		op.Key = args.(*GetArgs).Key
		op.Value = ""
		op.ClientId = args.(*GetArgs).ClientId
		op.SeqNo = args.(*GetArgs).SeqNo
	} else if op_name == "Put" || op_name == "Append" {
		op.Key = args.(*PutAppendArgs).Key
		op.Value = args.(*PutAppendArgs).Value
		op.ClientId = args.(*PutAppendArgs).ClientId
		op.SeqNo = args.(*PutAppendArgs).SeqNo
	}

	idx, _, is_leader := kv.rf.Start(op)

	if op_name == "Get" {
		reply.(*GetReply).Is_leader = is_leader
	} else {
		reply.(*PutAppendReply).Is_leader = is_leader
	}

	if !is_leader {
		return
	}

	info := RequestInfo{
		cv:        &sync.Cond{L: &kv.mu},
		operation: op,
		reply:     reply,
	}

	kv.request_infos[idx] = append(kv.request_infos[idx], info)
	info.cv.Wait()
}
