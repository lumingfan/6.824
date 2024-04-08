package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	Is_leader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int
	SeqNo int
}

type GetReply struct {
	Err   Err
	Value string
	Is_leader bool
}
