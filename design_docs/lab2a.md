# Lab2a: Raft leader election and heartbeats

## Preliminaries

> Fill in your name and email address.

J.K. Xu <2507550027@qq.com>

> preliminary comments on your submission

None

> Please cite any offline or online sources you consulted while preparing your submission, other than the lab handouts, lecture notes/papers

None

## A single Rust peer

### Data Structures

> Copy here the declaration of each new or changed `struct` or `struct member`
> Identify the purpose of each in 25 words or less.

A `state` is used to track the current type of server

```go
type State int 
const (
    FOLLOWER State = 0
    CANDIDATE State = 1
    LEADER State = 2
)
```

The following three constants are related to the timeout of the timer:

```go
const (
    // random timeout range of election timer is [300, 600)
    RAND_TIMEOUT_LOWER_BOUND int = 300
    RAND_TIMEOUT_RANGE int = 300

    // timeout of heartbeat timer is 100
    HEARTBEAT_TIMEOUT int = 100
)
```


Added to struct `Raft`

```go
current_term int       // the current term of this server 
voted_for int          // the server votes for which server (-1 for none)
current_state State    // the current state of server

/** 
 *  default true, set to false 
 *  if a follower receives AppendEntries RPC from current leader or 
 *  grants vote to candidate before the timer expires
*/ 
follower_election_flag  bool 

election_timer *time.Ticker  // timer for start election

/**
 * store logs. only the term and the initial element for lab2a, 
 * initially only one element with term 0
*/
logs [] int 
```

### Algorithms

> Briefly describe your transition logic between different states

A linear state transition after staring up:

---
Upon startup, we initiate two timers (election timer and heartbeat timer) using goroutines in the Make function. As the initial state of a server is a follower, when the election timer expires, the server transitions to a candidate state. 

In this state, it initiates an election process, seeking votes from other servers. If it receives a majority of votes, it becomes a leader (subsequently sending heartbeats to other servers upon the heartbeat timer expiration). However, if the election fails, the server remains in the candidate state. 

If the server receives a heartbeat from a valid leader, it reverts to being a follower.

---

To transition logic between all states:

- A follower transitions to a candidate only when the election timer times out and it does not receive a heartbeat from a valid leader or vote for a candidate.
- A candidate initiates a new election each time the election timer times out, irrespective of other conditions.
- A candidate becomes a leader upon receiving sufficient votes.
- A candidate reverts to being a follower upon receiving a heartbeat from a valid leader.

- Leaders/candidates transition back to followers upon receiving a term greater than their own current term from other servers (via RPC arguments/replies).

**Note: In this context, "valid" refers to the leader's term being greater than or equal to the current term of the server.**

> How do you implement the timer for starting an election ?

I utilize Go's ticker type as the timer for starting an election. It runs in a background goroutine. When the timer expires, it checks whether the one of the following conditions are met for starting an election

1. the current state is FOLLOWER and it has not received a heartbeat from a valid leader or voted for a candidate
2. the current state is CANDIDATE

If the conditions are met, the state is changed to CANDIDATE, and beginElection function is called as a goroutine.

**NOTE: It is essential to execute beginElection function as a goroutine instead of a regular function. Otherwise, if the beginElection function is blocked due to not receiving replies from some offline servers, the timer will also be blocked. This may lead to failed to pass `Test (2A): election after network failure`. The error messages will be like: `expected one leader, got none"` due to the timer being blocked, we can't start a new election**  

> How do you implement the timer for heartbeats ?

I utilize `time.Sleep` function to implement the timer for heartbeats, because this timer needs not to be reset, we can just run it as a background timer, if current state is LEADER, we run beginHeartbeats as a goroutine to send heartbeats to other servers 

**The reason to run it as a goroutine is similar to the election timer, otherwise, you may encounter error message: `expected no leader, but 1 claims to be leader` since the heartbeats can't be sent, the stale leader can't revert back to being a FOLLOWER**


> How do you implement the election ?


> How do you implement the heartbeats ?


### Rationale


## Request Vote RPC

### Data Structures

> Copy here the declaration of each new or changed `struct` or `struct member`
> Identify the purpose of each in 25 words or less.

added to RequestVoteArgs struct:

```go
Term int        // candidate’s term
CandidateId int // candidate requesting vote
LastLogIndex    // index of candidate’s last log entry
LastLogTerm     // term of candidate’s last log entry 
``` 

added to RequestVoteReply struct:

```go
Term int            // currentTerm, for candidate to update itself
VoteGranted bool    // means candidate received vote
```

### Algorithms

> Briefly describe your implementation of RequestVote

RequestVote first get its own state, if it's the candidate or leader and term > currentTerm, set term = currentTerm, return to a follower, if term <= currentTerm, return false.

otherwise, if it's a follower and

- the voteInfo.voted is still false
- term >= currentTerm
- candidate’s log is at least as up-to-date as receiver’s log

return true

> Briefly describe your implementation of SendRequestVote

SendRequestVote send a RequestVote RPC to a specify server, after RPC return, it check voteGranted, if true, then add the voted num by 1, if get the major votes, then send true to candidateCh.

### Rationale


## Heartbeat RPC

### Data Structures
> Copy here the declaration of each new or changed `struct` or `struct member`
> Identify the purpose of each in 25 words or less.

A `AppendEntriesArgs` struct is used as the args of SendAppendEntires RPC.

```go
type AppendEntriesArgs struct {
    term int            // leader’s term
    entries []Log       // log entries to store (empty for heartbeat)
}
```

A `AppendEntriesReply` is used as the reply of AppendEntires RPC

```go
type AppendEntriesReply struct {
    term int            // currentTerm, for leader to update itself
}
```

### Algorithms



### Ratinoale