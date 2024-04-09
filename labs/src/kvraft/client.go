package kvraft

import (
	"crypto/rand"
	"math/big"
	"src/labrpc"
	"sync"
	"time"
)

/** allocate to a new Clerk every time MakeClerk is called
 *  monotonically increasing
 */
var client_id int = 0

// protect client_id
var mu sync.Mutex = sync.Mutex{}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// cache the leader id
	leader_id int

	// client_id of this server
	me int

	// sequence number of next RPC request
	seq_no int

	// protect seq_no
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	mu.Lock()
	defer mu.Unlock()
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader_id = 0
	ck.seq_no = 0
	ck.me = client_id
	ck.mu = sync.Mutex{}
	client_id++
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{
		Key:      key,
		ClientId: ck.me,
		SeqNo:    ck.seq_no,
	}
	ck.seq_no++
	ck.mu.Unlock()
	reply := GetReply{}

	DPrintf("send seqno: %d to server %d", args.SeqNo, ck.leader_id)
	ok := ck.CallSingleRPC(ck.leader_id, "Get", &args, &reply)

	for !ok {
		for server_id := 0; server_id < len(ck.servers); server_id++ {
			new_reply := GetReply{}
			DPrintf("send seqno: %d to server %d", args.SeqNo, server_id)
			ok = ck.CallSingleRPC(server_id, "Get", &args, &new_reply)
			if ok {
				ck.leader_id = server_id
				return new_reply.Value
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		SeqNo:    ck.seq_no,
	}
	ck.seq_no++
	ck.mu.Unlock()
	reply := PutAppendReply{}

	DPrintf("send seqno: %d to server %d", args.SeqNo, ck.leader_id)
	ok := ck.CallSingleRPC(ck.leader_id, "PutAppend", &args, &reply)
	for !ok {
		for server_id := 0; server_id < len(ck.servers); server_id++ {
			new_reply := PutAppendReply{}
			DPrintf("send seqno: %d to server %d", args.SeqNo, server_id)
			ok = ck.CallSingleRPC(server_id, "PutAppend", &args, &new_reply)
			if ok {
				ck.leader_id = server_id
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

/**---------------- Helper functions ---------------**/

/** send a RPC to a specified server, then wait for response/timeout
 */
func (ck *Clerk) CallSingleRPC(server_id int, rpc_name string, args interface{}, reply interface{}) bool {
	ok_ch := make(chan bool)
	timer := time.NewTimer(300 * time.Millisecond)
	go func() {
		ret := ck.servers[server_id].Call("KVServer."+rpc_name, args, reply)
		ok_ch <- ret
	}()

	select {
	case <-timer.C:
		return false
	case ok := <-ok_ch:
		if !ok {
			return false
		}

		if rpc_name == "Get" && !reply.(*GetReply).Is_leader {
			return false
		} else if rpc_name == "PutAppend" && !reply.(*PutAppendReply).Is_leader {
			return false
		}
		return true
	}
}
