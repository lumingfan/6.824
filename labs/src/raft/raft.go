package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"src/labgob"
	"src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot []byte
}

// 2B, structure for storing the log information
// each entry contains
// 1. command for state machine,
// 2. term when entry was received by leader
type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A

	current_term  int   // the current term of this server
	voted_for     int   // the server votes for which server (-1 for none)
	current_state State // the current state of server

	/**
	 *  default true, set to false
	 *  if a follower receives AppendEntries RPC from current leader or
	 *  grants vote to candidate before the timer expires
	 */
	follower_election_flag bool
	election_timer         *time.Ticker // timer for start election

	/**
	 * store logs, for lab2a, only the term.
	 * initially only one element with term 0
	 */
	// logs []int
	logs []Log

	/**
	 * Index of highest log entry known to be committed
	 */
	commit_index int

	/**
	 * index of highest log entry applied to state machine
	 */
	last_applied int

	/**
	 * for each server,
	 * index of the next log entry to send to that server
	 */
	next_index []int

	/**
	 * for each server,
	 * index of highest log entry known to be replicated on server
	 */
	match_index []int

	// a channel to send ApplyMsg messages
	apply_ch chan ApplyMsg

	// a condition variable to apply the logs to state machine
	apply_cv sync.Cond

	// snapshot length: last snapshot index + 1
	// initially 1, since we have a dummy log entry in the logs
	snapshot_len int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term := rf.current_term
	isleader := rf.current_state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// caller must hold the rf.mu.Lock
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveRaftState(rf.encodeState())
}



// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var current_term int 
	var voted_for int
	var logs []Log
	var snapshot_len int

	if decoder.Decode(&current_term) != nil ||
	   decoder.Decode(&voted_for) != nil ||
	   decoder.Decode(&logs) != nil || 
	   decoder.Decode(&snapshot_len) != nil {
		panic("decoder error")
	} else {
		rf.current_term = current_term
		rf.voted_for = voted_for
		rf.logs = logs
		rf.snapshot_len = snapshot_len
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term          int // candidate's term
	Candidate_id  int // candidate requesting vote
	Last_log_idx  int // index of candidate's last log entry
	Last_log_term int // term of candidate's last log entry
}

type AppendEntriesArgs struct {
	Term      int
	Leader_id int

	// index of log entry immediately preceding new ones
	Prev_log_index int

	Prev_log_term       int   // term of prevLogIndex entry
	Entries             []Log // log entries to store
	Leader_commit_index int   // leader’s commitIndex
}

type InstallSnapshotArgs struct {
	Term int 	
	Leader_id int

	// the snapshot replaces all entries up through 
	// and including this index
	Last_included_idx int 

	Last_included_term int
	Snapshot []byte
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int  // currentTerm, for candidate to update itself
	VotedGranted bool // means candidate received vote
}

type AppendEntriesReply struct {
	Term int

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool

	Conflict_term int
	Conflict_index int
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if !rf.checkUpdateTerm(args.Term) {
		reply.Term = rf.current_term
		reply.VotedGranted = false
		return
	}


	if rf.checkValidCandidate(args.Last_log_term, args.Last_log_idx, args.Candidate_id) {
		DPrintf("server %d vote for candidate %d", rf.me, args.Candidate_id)
		rf.voted_for = args.Candidate_id
		rf.follower_election_flag = false

		reply.Term = rf.current_term
		reply.VotedGranted = true
	} 
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if !rf.checkUpdateTermAppEntVer(args.Term)  {
		// DPrintf("server %d refuse message from leader %d due to the stale term", rf.me, args.Leader_id)
		reply.Term = rf.current_term
		reply.Success = false
		return
	}

	reply.Term = rf.current_term
	reply.Success = false
	
	if !rf.fillConflictReply(args, reply) {
		return 
	}

	if len(args.Entries) != 0 {
		rf.checkUpdateLogs(args)
	}


	if args.Leader_commit_index > rf.commit_index {
		rf.commit_index = min(args.Leader_commit_index, args.Prev_log_index+len(args.Entries))
		rf.apply_cv.Signal()
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if !rf.checkUpdateTermAppEntVer(args.Term)  {
		// DPrintf("server %d refuse message from leader %d due to the stale term", rf.me, args.Leader_id)
		reply.Term = rf.current_term
		return
	}

	reply.Term = rf.current_term


	if !rf.checkValidSnapshot(args.Last_included_idx) {
		return
	}

	last_included_idx_rel := rf.absIdxToRel(args.Last_included_idx)

	if last_included_idx_rel < len(rf.logs) && 
	   rf.logs[last_included_idx_rel].Term == args.Last_included_term {
	   rf.snapshot_len = args.Last_included_idx + 1
	   rf.logs = rf.logs[last_included_idx_rel:] 
	   rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Snapshot)
	   return 
	}
	
	rf.snapshot_len = args.Last_included_idx + 1
	rf.logs = append([]Log{}, Log{
		Command: nil,
		Term:    args.Last_included_term,
	})

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Snapshot)
	rf.apply_cv.Signal()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		rf.checkUpdateTerm(reply.Term)
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		rf.checkUpdateTerm(reply.Term)
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.mu.Lock()
		rf.checkUpdateTerm(reply.Term)
		rf.mu.Unlock()
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.current_state == LEADER
	if isLeader {
		index = rf.relIdxToAbs(len(rf.logs))
		term = rf.current_term
		rf.logs = append(rf.logs, Log{
			Command: command,
			Term:    term,
		})
		rf.persist()
		rf.match_index[rf.me] = index
		DPrintf("leader %d add term: %d, command: %v", rf.me, rf.logs[len(rf.logs) - 1].Term, rf.logs[len(rf.logs) - 1].Command)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:                     sync.Mutex{},
		peers:                  peers,
		persister:              persister,
		me:                     me,
		dead:                   0,
		current_term:           0,
		voted_for:              -1,
		current_state:          FOLLOWER,
		follower_election_flag: true,
		election_timer:         nil,
		logs:                   []Log{{nil, 0}},
		commit_index:           0,
		last_applied:           0,
		next_index:             make([]int, len(peers)),
		match_index:            make([]int, len(peers)),
		apply_ch:               applyCh,
		snapshot_len:  			1,
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.apply_cv = sync.Cond{
		L: &rf.mu,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()
	go rf.startHeartBeatsTimer()
	go rf.startApplyLogsToStateMachine()
	return rf
}

const (
	// random timeout range of election timer is [300, 600)
	RAND_TIMEOUT_LOWER_BOUND int = 300
	RAND_TIMEOUT_RANGE       int = 300

	// timeout of heartbeat timer is 100
	HEARTBEAT_TIMEOUT int = 100
)

// used for debug
var DEBUG_STATE = []string{"follower", "candidate", "leader"}

type State int

const (
	FOLLOWER  State = 0
	CANDIDATE State = 1
	LEADER    State = 2
)

// generate a duration between 300ms to 600ms
func genRandDuration() time.Duration {
	return time.Duration(rand.Intn(RAND_TIMEOUT_RANGE)+RAND_TIMEOUT_LOWER_BOUND) * time.Millisecond
}

/** Start a election timer
 *  only called once in Make as a goroutine
 */
func (rf *Raft) startElectionTimer() {
	rf.election_timer = time.NewTicker(genRandDuration())
	defer rf.election_timer.Stop()
	for {
		<-rf.election_timer.C
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		begin_election := (rf.current_state == FOLLOWER && rf.follower_election_flag) || rf.current_state == CANDIDATE
		rf.follower_election_flag = true

		if begin_election {
			rf.current_state = CANDIDATE
			go rf.beginElection()
		}
		rf.mu.Unlock()
	}
}

/** Reset the election timer
 *  called when start a new election
 */
func (rf *Raft) resetElectionTimer() {
	rf.election_timer.Reset(genRandDuration())
}

/** Start heartbeats timer
 * 	only called once in Make as a goroutine
 */
func (rf *Raft) startHeartBeatsTimer() {
	for {
		time.Sleep(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.current_state == LEADER {
			go rf.beginHeartbeats()
		}
		rf.mu.Unlock()
	}
}

/** Apply committed logs to state machine after woken up by rf.apply_cv.Signal()
 * 	only called once in Make as a goroutine
 */

func (rf *Raft) startApplyLogsToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		rf.apply_cv.Wait()
		if rf.killed() {
			return 
		}

		if rf.last_applied < rf.snapshot_len {
			rf.last_applied = rf.snapshot_len - 1	
			msg := ApplyMsg{
				CommandValid: false,
				Command:      nil,
				CommandIndex: rf.last_applied,
				Snapshot:     rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()
			rf.apply_ch <- msg	
			rf.mu.Lock()
		}

		for rf.last_applied < rf.commit_index {
			rf.last_applied++
			last_applied_rel := rf.absIdxToRel(rf.last_applied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[last_applied_rel].Command,
				CommandIndex: rf.last_applied,
				Snapshot: []byte{},
			}
			rf.mu.Unlock()
			rf.apply_ch <- msg
			rf.mu.Lock()
		}
	}
}

/** start a new election
 */
func (rf *Raft) beginElection() {
	args := rf.initElection()
	rf.sendElection(args)
}

/** start a heartbeats
 */
func (rf *Raft) beginHeartbeats() {
	args, ok := rf.generateAppendEntriesArgsForAllServers()
	// current state is not LEADER
	if !ok {
		return 
	}
	rf.sendHeartbeats(args)
}

/**---------------- Helper functions ---------------**/

/* ----- caller must hold rf.mu.lock() ----- */


/** given absolute index, transform it to relative index 
 *  (used by snapshot)
 */ 
func (rf *Raft) absIdxToRel(idx int) int {
	return idx - rf.snapshot_len + 1
}

/** given relative index, transform it to absolute index 
 *  (used by snapshot)
 */
func (rf *Raft) relIdxToAbs(idx int) int {
	return idx + rf.snapshot_len - 1
}

/** encode state (current_term, voted_for, logs) to []byte
 */ 
func (rf *Raft) encodeState() []byte{
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	encoder.Encode(rf.current_term)
	encoder.Encode(rf.voted_for)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.snapshot_len)
	data := buffer.Bytes()
	return data
}

/** Check the validation of the snapshot 
 */
func  (rf *Raft) checkValidSnapshot(idx int) bool {
	// this snapshot has been applied to state machine
	if idx <= rf.last_applied {
		return false
	}
	return true
}

/** Check the validation of the candidate
 */
func (rf *Raft) checkValidCandidate(candidate_term int, candidate_index int, candidate_id int) bool {
	if rf.current_state != FOLLOWER {
		return false
	}

	if rf.voted_for != -1 && rf.voted_for != candidate_id {
		return false
	}

	if rf.logs[len(rf.logs) - 1].Term > candidate_term {
		return false
	}

	if rf.logs[len(rf.logs) - 1].Term == candidate_term {
		if rf.relIdxToAbs(len(rf.logs)) > candidate_index+1 {
			return false
		}
	}
	return true
}

/** Check if the server needs to update the current_term and transition back to a follower.
 */
func (rf *Raft) checkUpdateTerm(term int) bool {
	if term < rf.current_term {
		return false
	}

	if term > rf.current_term {
		rf.current_term = term
		rf.current_state = FOLLOWER
		rf.voted_for = -1
		rf.persist()
	}
	return true
}

/** extended checkUpdateTerm for AppendEntries RPC handler
 * 	when the state of this server is CANDIDATE,
 *		it must transition back to being a follower
 *  when the state of this server is FOLLOWER
 * 		it must prevent next election
 */
func (rf *Raft) checkUpdateTermAppEntVer(term int) bool {
	if !rf.checkUpdateTerm(term) {
		return false
	}

	if rf.current_state == CANDIDATE {
		rf.current_state = FOLLOWER
	}

	if rf.current_state == FOLLOWER {
		rf.follower_election_flag = false
	}

	return true
}


/** generate AppendEntriesArgs given the server_id
 *  return false if current state is not LEADER
 */
func (rf *Raft) generateAppendEntriesArgs(server_id int) (*AppendEntriesArgs, bool) {
	prev_log_idx := rf.next_index[server_id] - 1
	prev_log_idx_rel := rf.absIdxToRel(prev_log_idx)
	
	// entries needs to be sent to follower has been snapshot
	if prev_log_idx_rel < 0 {
		ins_args := InstallSnapshotArgs{
			Term:               rf.current_term,
			Leader_id:          rf.me,
			Last_included_idx:  rf.relIdxToAbs(0),
			Last_included_term: rf.logs[0].Term,
			Snapshot:           rf.persister.ReadSnapshot(),
		}		
		ins_reply := InstallSnapshotReply{}

		rf.mu.Unlock()
		rf.sendInstallSnapshot(server_id, &ins_args, &ins_reply)

		if rf.isTermOrStateStale(ins_args.Term, LEADER) {
			rf.mu.Lock()
			return &AppendEntriesArgs{}, false
		}

		rf.mu.Lock()
		rf.next_index[server_id] = rf.relIdxToAbs(len(rf.logs))
	}

	prev_log_idx = rf.next_index[server_id] - 1
	prev_log_idx_rel = rf.absIdxToRel(prev_log_idx)

	return &AppendEntriesArgs{
		Term:                rf.current_term,
		Leader_id:           rf.me,
		Prev_log_index:      prev_log_idx,
		Prev_log_term:       rf.logs[prev_log_idx_rel].Term,
		Entries:   append([]Log{}, rf.logs[prev_log_idx_rel + 1:]...),
		Leader_commit_index: rf.commit_index,
	}, true
}


/** update commit index once match_index has been updated
 *  then use rf.apply_cv.Signal() to apply new entries to state machine 
 */
func (rf *Raft) updateCommitIndex() {
	match_idx_temp := make([]int, len(rf.match_index))
	copy(match_idx_temp, rf.match_index)

	sort.Slice(match_idx_temp, func(i, j int) bool {
		return match_idx_temp[i] < match_idx_temp[j]
	})

	var n int
	if len(match_idx_temp) % 2 == 0 {
		n = match_idx_temp[len(match_idx_temp) / 2 - 1]	
	} else {
		n = match_idx_temp[len(match_idx_temp) / 2]
	}

	if n > rf.commit_index && rf.logs[rf.absIdxToRel(n)].Term == rf.current_term {
		rf.commit_index = n
		rf.apply_cv.Signal()
	}
}

/** set Conflict_index, Conflict_term fields in reply
 *  return true if no log inconsistency, false otherwise
 */ 
func (rf *Raft) fillConflictReply(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	logs_abs_len := rf.relIdxToAbs(len(rf.logs))
	prev_idx_rel := rf.absIdxToRel(args.Prev_log_index)
	
	// prev_idx_rel < 0 when this index has been snapshot, 
	// we don't know if this log entry is valid
	if logs_abs_len <= args.Prev_log_index || prev_idx_rel < 0 {
		reply.Conflict_index = logs_abs_len
		reply.Conflict_term = -1
		return false 
	}
	
	if rf.logs[prev_idx_rel].Term != args.Prev_log_term {
		// DPrintf("server %d refuse message from leader %d due to the log inconsistency, current logs: %v", rf.me, args.Leader_id, rf.logs)
		reply.Conflict_term = rf.logs[prev_idx_rel].Term
		reply.Conflict_index = args.Prev_log_index

		for idx := prev_idx_rel - 1; idx > 0; idx-- {
			if rf.logs[idx].Term == reply.Conflict_term {
				reply.Conflict_index = rf.relIdxToAbs(idx)
			}
		}
		return false
	}
	return true
}

/** update the server(follower)'s logs given the leader's logs  
 */
func (rf *Raft) checkUpdateLogs(args *AppendEntriesArgs) {
	logs_idx := rf.absIdxToRel(args.Prev_log_index + 1)
	entries_idx := 0
	for entries_idx < len(args.Entries) && logs_idx < len(rf.logs) {
		if rf.logs[logs_idx] != args.Entries[entries_idx] {
			rf.logs = rf.logs[:logs_idx]	
			break	
		}
		logs_idx++
		entries_idx++
	}
	rf.logs = append(rf.logs, args.Entries[entries_idx:]...)
	rf.persist()
}

/** move backwards next index for server_id according to reply.Conflict_term & reply.Conflict_idx
 */ 
func (rf *Raft) decreaseNextIndex(server_id int, reply *AppendEntriesReply) {
	idx := len(rf.logs) - 1
	for ; idx > 0; idx-- {
		if rf.logs[idx].Term == reply.Conflict_term {
			rf.next_index[server_id] = rf.relIdxToAbs(idx + 1)
			break
		}
	}

	if idx == 0 {
		rf.next_index[server_id] = reply.Conflict_index
	}
}

/* ----- caller can't hold rf.mu.lock() ----- */

/** return the size of current state, called by kvraft's server.go
 */
func (rf *Raft) ReadStateSize() int {
	return rf.persister.RaftStateSize()
}

/** save snapshot and state, called by kvraft's server.go
 */ 
func (rf *Raft) SaveStateAndSnapshot(key_value map[string]string, seqnos map[int]int, last_snapshot_idx int) {
	rf.mu.Lock()	
	defer rf.mu.Unlock()

	// save state and snapshot
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(key_value)
	encoder.Encode(seqnos)

	// discard stale logs, 
	// keep last_snapshot_idx as a dummy entry to make AppendEntries 
	// consistency check more easily
	last_snapshot_idx_rel := rf.absIdxToRel(last_snapshot_idx)
	rf.snapshot_len = last_snapshot_idx + 1	
	rf.logs = rf.logs[last_snapshot_idx_rel:] 

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), buffer.Bytes())
}



/** update states for beginning a new election and then return RequestVoteArgs to beginElection 
 */
func (rf *Raft) initElection() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.current_term++
	rf.voted_for = rf.me
	rf.persist()
	rf.resetElectionTimer()

	DPrintf("server %d begin election with term %d", rf.me, rf.current_term)

	return &RequestVoteArgs{
		Term:          rf.current_term,
		Candidate_id:  rf.me,
		Last_log_idx:  rf.relIdxToAbs(len(rf.logs) - 1),
		Last_log_term: rf.logs[len(rf.logs) - 1].Term,
	}
}

/**  send RequestVote RPC to other servers and wait for the response 
 */ 
func (rf *Raft) sendElection(args *RequestVoteArgs) {
	// check state before sending rpc 
	rf.mu.Lock()
	if rf.current_state != CANDIDATE {
		rf.mu.Unlock()
		return 
	}
	rf.mu.Unlock()

	voted_num := 1
	voted_mu := sync.Mutex{}
	voted_fin_flag := false
	voted_fin_cv := sync.Cond{
		L: &voted_mu,
	}

	for id := 0; id < len(rf.peers); id++ {
		if id != rf.me {
			go func(server_id int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server_id, args, &reply)
				if !ok || rf.isTermOrStateStale(args.Term, CANDIDATE) {
					rf.exitSendElection(server_id, &voted_num, &voted_fin_flag, &voted_mu, &voted_fin_cv)
				}	

				voted_mu.Lock()
				if voted_fin_flag {
					voted_mu.Unlock()
					return 
				}
				
				if reply.VotedGranted {
					voted_num++
				}
				voted_mu.Unlock()

				rf.exitSendElection(server_id, &voted_num, &voted_fin_flag, &voted_mu, &voted_fin_cv)
			}(id)
		}
	}

	voted_mu.Lock()
	for !voted_fin_flag {
		voted_fin_cv.Wait()
	}
	voted_mu.Unlock()

	rf.becomeLeader(voted_num, args.Term)
}

/** determine if this server can become a leader according to voted_num
 *  origin_term is used to detect stale response
 */
func (rf *Raft) becomeLeader(voted_num int, origin_term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voted_num > len(rf.peers) / 2 && rf.current_state == CANDIDATE && rf.current_term == origin_term {
		DPrintf("%d becomes a LEADER with term %d", rf.me, rf.current_term)
		rf.current_state = LEADER
		for idx := range rf.next_index {
			rf.next_index[idx] = rf.relIdxToAbs(len(rf.logs))
			rf.match_index[idx] = 0
			
			// we know our own match index 
			if idx == rf.me {
				rf.match_index[idx] = rf.relIdxToAbs(len(rf.logs) - 1)
			}
		}
		go rf.beginHeartbeats()
	} 
}

/** exit send RequestVote go routine safely
 */
func (rf *Raft) exitSendElection(server_id int, voted_num *int, flag *bool, mu *sync.Mutex, cv *sync.Cond) {
	mu.Lock()
	defer mu.Unlock()
	if !*flag && (server_id == len(rf.peers) - 1 || *voted_num > len(rf.peers) / 2){
		*flag = true
		cv.Signal()
	}
}

/** send AppendEntries RPC to all servers
 */
func (rf *Raft) sendHeartbeats(args []*AppendEntriesArgs) {
	for server_id := 0; server_id < len(rf.peers); server_id++ {
		if server_id == rf.me {
			continue
		}
		go rf.sendSingleHeartbeats(server_id, args[server_id])
	}	
}

/** send AppendEntries RPC to a specified server
 */ 
func (rf *Raft) sendSingleHeartbeats(server_id int, args *AppendEntriesArgs) {
	rf.mu.Lock()
	if rf.current_state != LEADER {
		rf.mu.Unlock()
		return 
	}
	rf.mu.Unlock()

	// DPrintf("leader %d send Heatbeats to server %d with args: %v", rf.me, server_id, args)

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server_id, args, &reply)

	if !ok || rf.isTermOrStateStale(args.Term, LEADER) {
		return 
	}

	for !reply.Success {
		if rf.killed() {
			return
		}

		if rf.isTermOrStateStale(args.Term, LEADER) {
			return 
		}

		if args.Term < reply.Term {
			return 
		}

		var args_ok bool
		rf.mu.Lock()
		rf.decreaseNextIndex(server_id, &reply)
		args, args_ok = rf.generateAppendEntriesArgs(server_id)
		rf.mu.Unlock()

		if !args_ok {
			return 	
		}
		
		reply = AppendEntriesReply{}
		ok = rf.sendAppendEntries(server_id, args, &reply)

		if !ok || rf.isTermOrStateStale(args.Term, LEADER) {
			return
		}
	}

	if len(args.Entries) != 0 {
		rf.handleSingleHeartbeatsReply(server_id, args)
	}
}


/** generate AppendEntriesArgs for all servers (except for leader)
 *  return false if current state is not leader
 */
func (rf *Raft) generateAppendEntriesArgsForAllServers() ([]*AppendEntriesArgs, bool){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := make([]*AppendEntriesArgs, len(rf.peers))
	var ok bool
	for server_id:= 0; server_id < len(rf.peers); server_id++{
		if server_id != rf.me {
			args[server_id], ok = rf.generateAppendEntriesArgs(server_id)
			// current state is not LEADER
			if !ok {
				return args, false
			}
		}
	}
	return args, true
}


/** is this term/state stale? 
 */ 
func (rf *Raft) isTermOrStateStale(origin_term int, origin_state State) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return origin_term != rf.current_term || origin_state != rf.current_state
}


/** update match_index/next_index for a specified server, then update commit index for leader
 */
func (rf *Raft) handleSingleHeartbeatsReply(server_id int, args *AppendEntriesArgs) {
	// success: update next_index and match_idx for follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.match_index[server_id] = args.Prev_log_index + len(args.Entries)
	rf.next_index[server_id] = rf.match_index[server_id] + 1
	rf.updateCommitIndex()
}





