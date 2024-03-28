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
	"math/rand"
	"src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// enum for type of server State
type State int
const (
	follower State = 0
	candidate State = 1
	leader State = 2
)


const randLowerBound int = 500	// lower bound of rand values
const randRange int = 500 		// range of rand values
// helper function for generate election timeout
func generateRandomInterval() time.Duration {
	return time.Duration(rand.Intn(randRange) + randLowerBound)
}

/** Used to reply voted information to SendRequestVote() rpc
 */
type VoteInfo struct {
	votedFor int // the idx of server voted by this server
	voted bool   // whether this server has already voted for a server
}

/** Used to store log information
 */
type Log struct {
	term int 	// when entry was received by leader
}


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
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
	currentTerm int 			  // the current term of this server
	voteInfo VoteInfo			 

	currentState State 			  // the current state of server

	followerCh chan bool  		  // Follower timeout reset channel

	/** Candidate timeout stop channel
	 * 	true become leader
	 * 	false become follower
	 */
	candidateCh chan bool 

	/** leader timeout reset channel
	 * true retain leader
	 * false become follower
	 */
	leaderCh chan bool 			  
	
	
	// log entries, initially with length 1, store the term 0 for election
	logs []Log 					  
}



// enter follower state, return the next state to enter 
func (rf *Raft) enterFollowerState() State {
	rf.mu.Lock()
	rf.currentState = follower
	rf.mu.Unlock()

	timer := time.NewTimer(time.Microsecond * generateRandomInterval());
	for {
		select {
		case <-rf.followerCh :
			timer.Reset(time.Millisecond * generateRandomInterval())
		case <-timer.C:
			return candidate
		}
	}
}

// helper function for sending RequestVote RPCs to peers
func (rf *Raft) sendRequestVoteToPeers() {
	votedNum := 1
	votedNumMu := sync.Mutex{}
	for idx:= 0; idx < len(rf.peers); idx++ {
		if idx != rf.me {
			go func(serverIdx int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.logs) - 1,
					LastLogTerm:  rf.logs[len(rf.logs) - 1].term,
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				voted := rf.sendRequestVote(serverIdx, &args, &reply)

				votedNumMu.Lock()
				defer votedNumMu.Unlock()

				if voted {
					votedNum++	
				} else {
					return 
				}
				
				// become a leader
				if votedNum == len(rf.peers) / 2 + 1 {
					DPrintf("server %d becomes a leader", rf.me)
					rf.candidateCh <- true
				}
			}(idx)
		}
	}

}

// enter candidate state, return next state to enter
func (rf *Raft) enterCandidateState() State {
	rf.mu.Lock()
	rf.currentState = candidate
	rf.currentTerm++
	rf.voteInfo.voted = true
	rf.voteInfo.votedFor = rf.me
	rf.mu.Unlock()

	// send VoteRequest RPC to other servers
	rf.sendRequestVoteToPeers()

	timer := time.NewTimer(time.Millisecond * generateRandomInterval())
	select {
	case isleader := <- rf.candidateCh:
		if isleader {
			return leader
		}
		return follower
	case <- timer.C:
		return candidate
	}
}

// helper function for sending heartbeat RPCs to peers
func (rf *Raft) sendHeartBeatToPeers() {
	for idx := 0; idx < len(rf.peers); idx++ {
		if idx != rf.me {
			go func(serverIdx int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:    rf.currentTerm,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(serverIdx, &args, &reply)
			}(idx)
		}
	}
}

// enter candidate state, return next state to enter
func (rf *Raft) enterLeaderState() State {
	rf.mu.Lock()
	rf.currentState = leader
	rf.mu.Unlock()

	rf.sendHeartBeatToPeers()
	
	// send heartbeat after time firing if leader is idle
	timer := time.NewTimer(time.Millisecond * time.Duration(randLowerBound) / 2)

	select {
	case isleader := <- rf.leaderCh:
		if isleader {
			return leader
		}
		return follower
	case <- timer.C:
		return leader
	}
}

// background go routine, transit between the different states of server
func (rf *Raft) scheduleState() {
	// initial state is follower
	state := follower
	for {
		switch state {
		case follower:
			state = rf.enterFollowerState()
		case candidate:
			state = rf.enterCandidateState()
		case leader:
			state = rf.enterLeaderState()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 				// candidate's term
	CandidateId int 		// candidate requesting vote
	LastLogIndex int 		// index of candidate's last log entry
	LastLogTerm int 		// term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 			// currentTerm, for candidate to upate itself
	VoteGranted bool 	// means candidate received vote
}

// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
func (rf *Raft) termCheck(term int) {
	rf.mu.Lock()
	leaderToFollower := false
	candidateToFollower := false
	followerTimerReset := false
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteInfo.voted = false
		leaderToFollower = rf.currentState == leader
		candidateToFollower = rf.currentState == candidate
		followerTimerReset = rf.currentState == follower
	} 
	rf.mu.Unlock()

	if leaderToFollower {
		rf.leaderCh <- false
	}
	if candidateToFollower {
		rf.candidateCh <- false
	}
	if followerTimerReset {
		rf.followerCh <- false
	}
}

// when receive a valid append entries RPC from leader, candidates should return to followers, followers should reset election timer
func (rf *Raft) heartBeatTermCheck(term int) {
	rf.mu.Lock()
	candidateToFollower := false
	followerReset := false
	if term == rf.currentTerm {
		rf.voteInfo.voted = false
		candidateToFollower = rf.currentState == candidate 
		followerReset = rf.currentState == follower 
	}
	rf.mu.Unlock()

	if candidateToFollower {
		rf.candidateCh <- false
	}

	if followerReset {
		rf.followerCh <- false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.termCheck(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}


	// term >= currentTerm
	// candidate's log is not at least as up-to-date
	if rf.logs[args.LastLogIndex].term != args.LastLogTerm || rf.voteInfo.voted {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return 
	}

	DPrintf("server %d vote for %d", rf.me, args.CandidateId)
	
	// vote it
	rf.voteInfo.voted = true
	rf.voteInfo.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.termCheck(reply.Term)

	return reply.VoteGranted 
}

// AppendEntries arguments struct
type AppendEntriesArgs struct {
	Term int 		// leader's term
	// Entries []Log	// log entries to store (empty for heartbeat)
}

// AppendEntries reply struct
type AppendEntriesReply struct {
	Term int 		// currentTerm, for leader to update itself
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.termCheck(args.Term)
	rf.heartBeatTermCheck(args.Term)
	reply.Term = rf.currentTerm
}

// send AppendEntries RPC to other servers
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}
	rf.termCheck(reply.Term)
	return ok
}





//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:           sync.Mutex{},
		peers:        peers,
		persister:    persister,
		me:           me,
		dead:         0,
		currentTerm:  0,
		voteInfo:     VoteInfo{},
		currentState: 0,
		followerCh:   make(chan bool),
		candidateCh:  make(chan bool),
		leaderCh:     make(chan bool),
		logs:         []Log{{0}},
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go rf.scheduleState()
	return rf
}
