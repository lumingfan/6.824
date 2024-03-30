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

	current_term int 		// the current term of this server 
	voted_for int    		// the server votes for which server (-1 for none)
	current_state State  	// the current state of server

	/** 
	 *  default true, set to false 
	 *  if a follower receives AppendEntries RPC from current leader or 
	 *  grants vote to candidate before the timer expires
	*/ 
	follower_election_flag  bool
	election_timer *time.Ticker  // timer for start election
	
	/**
	 * store logs, for lab2a, only the term, 
	 * initially only one element with term 0
	*/
	logs []int 				
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

	Term int  				// candidate's term
	Candidate_id int		// candidate requesting vote
	Last_log_idx int		// index of candidate's last log entry
	Last_log_term int		// term of candidate's last log entry
}

type AppendEntriesArgs struct {
	Term int 
}


//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int		  // currentTerm, for candidate to update itself
	VotedGranted bool // means candidate received vote
}

type AppendEntriesReply struct {
	Term int
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.checkUpdateTerm(args.Term) {
		reply.Term = rf.current_term	
		reply.VotedGranted = false
		return 
	}

	if rf.current_state == FOLLOWER && (rf.voted_for == -1 || rf.voted_for == args.Candidate_id) && rf.logs[args.Last_log_idx] == args.Last_log_term {
		rf.voted_for = args.Candidate_id
		rf.follower_election_flag = false

		reply.Term = rf.current_term
		reply.VotedGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkUpdateTermAppEntVer(args.Term) 
	reply.Term = rf.current_term	
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkUpdateTerm(reply.Term)
	return ok
}


func (rf *Raft) sendAppendEntries(server int , args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkUpdateTermAppEntVer(reply.Term)
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
		logs:                   []int{0},
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()
	go rf.startHeartBeatsTimer()
	return rf
}

const (
	// random timeout range of election timer is [300, 600)
	RAND_TIMEOUT_LOWER_BOUND int = 300
	RAND_TIMEOUT_RANGE int = 300

	// timeout of heartbeat timer is 100
	HEARTBEAT_TIMEOUT int = 100
)

// used for debug
var DEBUG_STATE = [] string {"follower", "candidate", "leader"}

type State int
const (
	FOLLOWER State = 0
	CANDIDATE State = 1
	LEADER State = 2
)


// generate a duration between 300ms to 600ms
func genRandDuration() time.Duration {
	return time.Duration(rand.Intn(RAND_TIMEOUT_RANGE) + RAND_TIMEOUT_LOWER_BOUND) * time.Millisecond
}

/** Start a election timer 
 *  only called once in Make as a goroutine
 */
func (rf *Raft) startElectionTimer() {
	rf.election_timer = time.NewTicker(genRandDuration()) 
	defer rf.election_timer.Stop()
	for {
		<-rf.election_timer.C
		rf.mu.Lock()	
		begin_election := (rf.current_state == FOLLOWER && rf.follower_election_flag) || rf.current_state == CANDIDATE
		rf.follower_election_flag = true

		if begin_election {
			rf.current_state = CANDIDATE
			rf.mu.Unlock()
			go rf.beginElection()	
		} else {
			rf.mu.Unlock()
		}
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
		rf.mu.Lock()
		begin_heartbeats := rf.current_state == LEADER 
		rf.mu.Unlock()
		if begin_heartbeats {
			go rf.beginHeartbeats()
		}	
	}
}


/** start a new election
 * 	wait for obtaining a majority of votes or 
 * 	receiving replies from all other servers
 */
func (rf *Raft) beginElection() {
	rf.mu.Lock()
	if rf.current_state != CANDIDATE {
		rf.mu.Unlock()
		return 
	}

	rf.current_term++

	rf.voted_for = rf.me
	rf.resetElectionTimer()
	args := RequestVoteArgs{
		Term:          rf.current_term,
		Candidate_id:  rf.me,
		Last_log_idx:  len(rf.logs) - 1,
		Last_log_term: rf.logs[len(rf.logs) - 1],
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
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				voted_mu.Lock()
				defer voted_mu.Unlock()
				
				if voted_fin_flag {
					return 
				}

				if ok && reply.VotedGranted {
					voted_num++
					if voted_num > len(rf.peers) / 2 {
						voted_fin_flag = true	
						voted_fin_cv.Signal()
					}
				} 
				
				// last peers'response, and didn't win this election
				if !voted_fin_flag && server == len(rf.peers) - 1 {
					voted_fin_flag = true
					voted_fin_cv.Signal()
				}
			}(id)
		}
	}			

	voted_mu.Lock()
	for !voted_fin_flag {
		voted_fin_cv.Wait()	
	}
	voted_mu.Unlock()

	rf.mu.Lock()
	if voted_num > len(rf.peers) / 2 && rf.current_state == CANDIDATE {
		// become a leader 
		rf.current_state = LEADER
		rf.mu.Unlock()
		rf.beginHeartbeats()
	} else {
		rf.mu.Unlock()
	}
}

/** start a heartbeats
 * 	doesn't wait for replies
 */
func (rf *Raft) beginHeartbeats() {
	rf.mu.Lock()
	if rf.current_state != LEADER {
		rf.mu.Unlock()
		return 
	}	
	args := AppendEntriesArgs{
		Term: rf.current_term,
	}
	rf.mu.Unlock()

	for id := 0; id < len(rf.peers); id++ {
		if id != rf.me {
			go func(server int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)
			} (id)
		}
	}
}


/** Check if it is necessary to update the current term and transition back to being a follower. 
 *  Called before processing a RPC request or 
 * 	after receiving a RPC reply  
*/
func (rf *Raft) checkUpdateTerm(term int) bool {
	if term < rf.current_term {
		return false
	}

	if term > rf.current_term {
		rf.current_term = term
		rf.current_state = FOLLOWER
		rf.voted_for = -1
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

	if rf.current_state == CANDIDATE  {
		rf.current_state = FOLLOWER	
	} 

	if rf.current_state == FOLLOWER {
		rf.follower_election_flag = false
	}

	return true
}


