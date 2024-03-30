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

`beginElection` first check the whether the current state is CANDIDATE (asynchronous RPC handler/sender may alter the state), if not, it returns immediately. Otherwise, it follows the rules for a CANDIDATE:

- Increment currentTerm
- Vote for self
- Reset election timer
- Send RequestVote RPCs to all other servers 

Before sending RequestVote RPCs to all other servers, we must unlock the mutex, because we need to acquire the lock in `sendRequestVote` (**deadlock**).

I use `sync.Cond` to allow `beginElection` to wait util a majority of servers votes for us or all servers reply to us. (In the first version, i use `sync.WaitGroup` to wait for replies from all servers, but it could block for a long time if one server is offline, causing test failures)


After being awakened by `Signal`, we check if we won this election and if the current state is still CANDIDATE (for the same reason as the initial check at the beginning of `beginElection`), if  conditions hold, we transition to the leader state and  send heartbeats to other servers. If not, we return and wait for election timer to expire.

## RPCs

### Data Structures

> Copy here the declaration of each new or changed `struct` or `struct member`
> Identify the purpose of each in 25 words or less.

A `AppendEntriesArgs` is used as the arguments for `AppendEntries` RPC

in this lab, we need a Term to tell other servers the current term of leader

```go
type AppendEntriesArgs struct {
    Term int        // current term of leader
}
```

A `AppendEntriesReply` is used as the reply for `AppendEntries` RPC

In this lab, we require a Term field to reply to the leader, let it to update itself according to this Term.

```go
type AppendEntriesReply struct {
    Term int        // current term of this server
}
```


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

> Briefly describe your implementation of RequestVote/AppendEntriesVote RPC handler

RequestVote RPC handler first calls the helper function `checkUpdateTerm` to verify if the sender's term is valid. If it's not, return false immediately. Otherwise, it checks if it needs to update its current term/state and return true. 

Once `checkUpdateTerm` completes, we check if we need to vote for the sender, if the conditions are met, records the `voted_for` information and set `follower_election_flag` to false to prevent the next election.


AppendEntries RPC handler only needs to call `checkUpdateTermAppEntVer` helper function to perform check/update tasks in this lab.

`checkUpdateTermAppEntVer` first call `checkUpdateTerm` to do basic check/update works, then depending on the current state, perform some additional updates:

- CANDIDATE: revert back to a FOLLOWER
- FOLLOWER: set follower_election_flag to false to prevent next election

