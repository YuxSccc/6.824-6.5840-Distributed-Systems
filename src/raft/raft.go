package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"encoding/json"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PeerRole int

const (
	RoleFollower = iota
	RoleCandidate
	RoleLeader
)

const (
	LeaderElectionTimeout           = time.Second * 3
	HeartbeatTimeout                = time.Millisecond * 200
	RequestVoteSuccessCheckInterval = time.Millisecond * 10
)

type LogEntry struct {
	term        int
	indexInTerm int
	indexInLog  int
	data        []byte
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
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	role                              PeerRole
	lastAppendEntriesRequestReceiveTs int64
	leaderElectionTimeout             time.Duration
	electionVoteCount                 int32
	leaderId                          int
}

func (rf *Raft) getRandomLeaderElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(1000+rand.Int63()%500)
}

func (rf *Raft) isMajority(count int) bool {
	return count >= (len(rf.peers)+1)/2
}

// getLastLogIndexAndTerm return (-1, -1) means there is no log
func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return -1, -1
	}
	lastEntry := rf.log[len(rf.log)-1]
	return lastEntry.indexInLog, lastEntry.term
}

func (rf *Raft) doLeaderElection() {
	var wg sync.WaitGroup
	replyList := make([]RequestVoteReply, len(rf.peers))

	requestVoteCallback := func(reply *RequestVoteReply, index int) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// maybe got another leader appendEntry, role change to follower
		if reply.Term <= rf.currentTerm && rf.role == RoleCandidate && reply.VoteGranted {
			atomic.AddInt32(&rf.electionVoteCount, 1)
			Info("peer[%d] receive access vote from [%d], current vote count [%d]",
				rf.me, index, atomic.LoadInt32(&rf.electionVoteCount))
		}
	}

	// for scoped lock
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != RoleFollower {
			Warning("Role change when enter the election function")
			return
		}

		rf.handleNextTermWithMutexLocked(rf.currentTerm+1, -1, true)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			requestArgs := RequestVoteArgs{}
			requestArgs.Term = rf.currentTerm
			requestArgs.CandidateId = rf.me
			lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
			requestArgs.LastLogIndex = lastLogIndex
			requestArgs.LastLogTerm = lastLogTerm

			wg.Add(1)
			go func(index int) {
				if rf.sendRequestVote(index, &requestArgs, &replyList[index]) {
					requestVoteCallback(&replyList[index], index)
				}
				defer wg.Done()
			}(i)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ec := 0
	startElectionTs := time.Now()
loop:
	for {
		select {
		case <-done:
			break loop
		case <-time.Tick(RequestVoteSuccessCheckInterval):
			rf.mu.Lock()
			if rf.role != RoleCandidate {
				ec = 2
				Warning(" peer[%d] Role has been changed when leader election", rf.me)
				rf.mu.Unlock()
				break loop
			}
			rf.mu.Unlock()

			if time.Now().Sub(startElectionTs) > LeaderElectionTimeout {
				ec = 1
				Warning("peer[%d] Timeout in leader election", rf.me)
				break loop
			}

			if rf.isMajority(int(atomic.LoadInt32(&rf.electionVoteCount))) {
				break loop
			}
		}
	}
	func() {
		if ec != 2 {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.isMajority(int(atomic.LoadInt32(&rf.electionVoteCount))) {
				rf.promoteToLeaderWithMutexLocked()
			} else {
				rf.degradeToFollowerWithMutexLocked()
			}
		}
	}()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.role == RoleLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [][]byte
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.handleNextTermWithMutexLocked(args.Term, -1, false)
	}

	if rf.votedFor != -1 {
		Warning("peer[%d] has vote for [%d] in term [%d], denied RequestVote from [%d]", rf.me, rf.votedFor,
			rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		Warning("peer[%d] get delayed log from [%d], denied", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	// get valid vote from candidate can also refresh timeout
	rf.updateLastLeaderHeartbeatTimestamp()
	rf.votedFor = args.CandidateId
	Info("peer[%d] access RequestVote from [%d]", rf.me, args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateLastLeaderHeartbeatTimestamp()

	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.handleNextTermWithMutexLocked(args.Term, args.LeaderId, false)
	}

	reply.Term = rf.currentTerm

	if rf.role == RoleCandidate {
		Info("Another leader send append entry when leader election, terminated")
		rf.degradeToFollowerWithMutexLocked()
	}

	if len(args.Entries) == 0 {
		// heartbeat rpc
		return
	}

	// process Entries
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		Warning("AppendEntries first log does not match PrevLogIndex")
	}

	oldLength := len(rf.log)

	for idx, entry := range args.Entries {
		newEntry := LogEntry{}
		err := json.Unmarshal(entry, &newEntry)
		if err != nil {
			Error("A entry caught an error when unmarshal [%s]", err.Error())
			reply.Success = false
			rf.log = rf.log[:oldLength] // revert the status
			return
		}

		indexInServerLog := idx + args.PrevLogIndex + 1

		if indexInServerLog < len(rf.log) {
			if newEntry.term != rf.log[indexInServerLog].term {
				rf.log = rf.log[:indexInServerLog]
				Warning("There is a mismatch in LogEntry[%d], replace with leader's log", indexInServerLog)
			} else {
				continue
			}
		}
		rf.log = append(rf.log, newEntry)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = func() int {
			if args.LeaderCommit < len(rf.log)-1 {
				return args.LeaderCommit
			} else {
				return len(rf.log) - 1
			}
		}()
	}
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) updateLastLeaderHeartbeatTimestamp() {
	atomic.StoreInt64(&rf.lastAppendEntriesRequestReceiveTs, time.Now().UnixMicro())
}

func (rf *Raft) getLastLeaderHeartbeatTimestampInMicro() int64 {
	return atomic.LoadInt64(&rf.lastAppendEntriesRequestReceiveTs)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		lastRPCTs := rf.getLastLeaderHeartbeatTimestampInMicro()
		if rf.role == RoleFollower &&
			time.Now().Sub(time.UnixMicro(lastRPCTs)) > rf.leaderElectionTimeout {
			rf.doLeaderElection()
			rf.updateLastLeaderHeartbeatTimestamp()
			rf.leaderElectionTimeout = rf.getRandomLeaderElectionTimeout()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLogEntryToStatusMachine(index int) {
	return
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.commitIndex > rf.lastApplied {
		rf.applyLogEntryToStatusMachine(rf.lastApplied + 1)
		rf.lastApplied += 1
	}
}

func (rf *Raft) degradeToFollowerWithMutexLocked() {
	rf.role = RoleFollower
}

func (rf *Raft) promoteToLeaderWithMutexLocked() {
	rf.role = RoleLeader
	rf.leaderId = rf.me
	Info("EVENT: [%d] promote to leader in term[%d]", rf.me, rf.currentTerm)
}

func (rf *Raft) handleNextTermWithMutexLocked(newTerm int, leaderId int, isElection bool) {
	rf.currentTerm = newTerm
	if isElection {
		rf.role = RoleCandidate
		rf.votedFor = rf.me
		atomic.StoreInt32(&rf.electionVoteCount, 1)
		Info("EVENT: [%d] promote to candidate in term[%d]", rf.me, rf.currentTerm)
	} else {
		rf.degradeToFollowerWithMutexLocked()
		rf.votedFor = -1
		atomic.StoreInt32(&rf.electionVoteCount, 0)
		Info("EVENT: [%d] degrade to follower in term[%d], leaderId[%d]", rf.me, rf.currentTerm, leaderId)
	}
	rf.leaderId = leaderId
}

func (rf *Raft) broadcastHeartBeat() {

	var wg sync.WaitGroup
	replyList := make([]AppendEntriesReply, len(rf.peers))

	appendEntriesCallback := func(reply *AppendEntriesReply) {
	}

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != RoleLeader {
			// Warning("Role is not leader, can not broadcast")
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			requestArgs := AppendEntriesArgs{}
			requestArgs.Term = rf.currentTerm
			requestArgs.LeaderCommit = rf.commitIndex
			requestArgs.LeaderId = rf.me
			requestArgs.PrevLogTerm, requestArgs.PrevLogIndex = func() (int, int) {
				if len(rf.log) != 0 {
					return rf.log[len(rf.log)-1].term, rf.log[len(rf.log)-1].indexInLog
				} else {
					return -1, -1
				}
			}()

			wg.Add(1)
			go func(index int) {
				if rf.sendAppendEntries(index, &requestArgs, &replyList[index]) {
					appendEntriesCallback(&replyList[index])
				}
				defer wg.Done()
			}(i)
		}

	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	startElectionTs := time.Now()
loop:
	for {
		select {
		case <-done:
			break loop
		case <-time.Tick(RequestVoteSuccessCheckInterval):
			if time.Now().Sub(startElectionTs) > HeartbeatTimeout {
				break loop
			}
		}
	}
}

func (rf *Raft) statusUpdateLoop() {
	for rf.killed() == false {
		rf.updateCommitIndex()
		rf.broadcastHeartBeat()
		time.Sleep(time.Millisecond * 50)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = RoleFollower
	rf.updateLastLeaderHeartbeatTimestamp()
	rf.leaderElectionTimeout = rf.getRandomLeaderElectionTimeout()
	rf.commitIndex = -1
	rf.lastApplied = -1
	atomic.StoreInt32(&rf.electionVoteCount, 0)
	rf.leaderId = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.statusUpdateLoop()

	return rf
}
