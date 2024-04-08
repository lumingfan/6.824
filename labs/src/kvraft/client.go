package kvraft

import (
	"crypto/rand"
	"math/big"
	"src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	
	// cache the leader id
	leader_id int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader_id = 0
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
	}
	reply := GetReply{}

	ok := ck.CallSingleGet(ck.leader_id, &args, &reply)

	for !ok {
		for server_id := 0; server_id < len(ck.servers); server_id++ {
			reply = GetReply{}
			ok = ck.CallSingleGet(server_id, &args, &reply)
			if ok {
				ck.leader_id = server_id
				return reply.Value
			}
		}
	}
	return reply.Value
}



//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}	
	reply := PutAppendReply{}

	ok := ck.CallSinglePutAppend(ck.leader_id, op, &args, &reply)
	for !ok {
		for server_id := 0; server_id < len(ck.servers); server_id++ {
			reply = PutAppendReply{}
			ok = ck.CallSinglePutAppend(server_id, op, &args, &reply)
			if ok {
				ck.leader_id = server_id
				return
			}
		}
	}
}


func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}


// helper functions
func (ck *Clerk) CallSingleGet(server_id int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server_id].Call("KVServer.Get", args, reply)
	if !ok || !reply.Is_leader {
		return false
	}
	return true
}

func (ck *Clerk) CallSinglePutAppend(server_id int, op string, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server_id].Call("KVServer.PutAppend", args, reply)
	if !ok || !reply.Is_leader {
		return false
	}
	return true
}