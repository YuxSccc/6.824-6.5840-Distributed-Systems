package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"context"
	"sort"

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

type PeerRole int32

const (
	RoleFollower = iota
	RoleCandidate
	RoleLeader
)

const (
	LeaderElectionTimeout         = time.Millisecond * 500
	HeartbeatTimeout              = time.Millisecond * 150
	AppendEntriesBroadcastTimeout = time.Millisecond * 150
	InstallSnapshotTimeout        = time.Millisecond * 150
	ClientRequestTimeout          = time.Millisecond * 6000

	RequestVoteSuccessCheckInterval = time.Millisecond * 10
	HeartbeatInterval               = time.Millisecond * 60
	LeaderLoopInterval              = time.Millisecond * 60

	NextIndexMismatchDecreaseCount = 100
	AppendEntriesMethodName        = "Raft.AppendEntries"
	InstallSnapshotMethodName      = "Raft.InstallSnapshot"
)

type LogEntry struct {
	Term       int
	IndexInLog int
	Data       interface{}
}

type LoggingMutex struct {
	m       sync.Mutex
	me      int
	locklog []string
}

func (lm *LoggingMutex) Lock() {
	//_, file, line, _ := runtime.Caller(1)
	//lm.locklog = append(lm.locklog, fmt.Sprintf("[%s] [%d] Locking from %s:%d", time.Now().GoString(), lm.me, file, line))
	//Info("Lockpeer[%d]:lock %s", lm.me, lm.locklog[len(lm.locklog)-1])
	lm.m.Lock()
}

func (lm *LoggingMutex) Unlock() {
	//_, file, line, _ := runtime.Caller(1)
	//lm.locklog = append(lm.locklog, fmt.Sprintf("[%s] [%d] Unlocking from %s:%d", time.Now().GoString(), lm.me, file, line))
	//Info("Lockpeer[%d]:unlock %s", lm.me, lm.locklog[len(lm.locklog)-1])
	lm.m.Unlock()
}

// A Go object implementing a single Raft peer.
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        LoggingMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your Data here (2A, 2B, 2C).
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
	applyCh                           chan ApplyMsg

	nextIndex  []int
	matchIndex []int

	snapshot *SnapshotPackage

	newSnapshotTerm int
	newSnapshotData []byte
}

type SnapshotPackage struct {
	Term              int
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

func NewSnapshotPackage() *SnapshotPackage {
	ret := &SnapshotPackage{}
	ret.reset()
	return ret
}

func (s *SnapshotPackage) hasData() bool {
	return s.Term != -1
}

func (s *SnapshotPackage) toBytes() (error, []byte) {
	if !s.hasData() {
		return nil, nil
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(s.Data)
	if err != nil {
		return err, nil
	}
	err = e.Encode(s.LastIncludedIndex)
	if err != nil {
		return err, nil
	}
	err = e.Encode(s.LastIncludedTerm)
	if err != nil {
		return err, nil
	}
	return nil, w.Bytes()
}

func (s *SnapshotPackage) reset() {
	s.Term = -1
	s.Data = make([]byte, 0)
	s.LastIncludedIndex = -1
	s.LastIncludedTerm = -1
}

func (s *SnapshotPackage) fromLogEntryAndData(entry LogEntry, data []byte) {
	s.Data = data
	s.LastIncludedIndex = entry.IndexInLog
	s.LastIncludedTerm = entry.Term
	s.Term = entry.Term // arbitrary value except -1
}

func (s *SnapshotPackage) fromBytes(data []byte) error {
	if data == nil || len(data) == 0 {
		s.reset()
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err := d.Decode(&s.Data)
	if err != nil {
		goto errorHandle
	}
	err = d.Decode(&s.LastIncludedIndex)
	if err != nil {
		goto errorHandle
	}
	err = d.Decode(&s.LastIncludedTerm)
	if err != nil {
		goto errorHandle
	}
	return nil

errorHandle:
	s.reset()
	return err
}

func (rf *Raft) getRandomLeaderElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(500+rand.Int63()%500)
}

func (rf *Raft) isMajority(count int) bool {
	return count >= len(rf.peers)/2+1
}

func (rf *Raft) getRole() PeerRole {
	return PeerRole(atomic.LoadInt32((*int32)(&rf.role)))
}

func (rf *Raft) setRole(role PeerRole) {
	atomic.StoreInt32((*int32)(&rf.role), int32(role))
}

// getLastLogIndexAndTermWithMutexLocked return (-1, -1) means there is no log
func (rf *Raft) getLastLogIndexAndTermWithMutexLocked() (int, int) {
	if len(rf.log) == 0 {
		return -1, -1
	}
	lastEntry := rf.log[len(rf.log)-1]
	return lastEntry.IndexInLog, lastEntry.Term
}

func (rf *Raft) doLeaderElection() {
	var wg sync.WaitGroup
	replyList := make([]RequestVoteReply, len(rf.peers))

	requestVoteCallback := func(reply *RequestVoteReply, index int) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// maybe got another leader appendEntry, role change to follower
		if reply.Term <= rf.currentTerm && rf.getRole() == RoleCandidate && reply.VoteGranted {
			atomic.AddInt32(&rf.electionVoteCount, 1)
			Info("peer[%d] receive access vote from [%d], current vote count [%d]",
				rf.me, index, atomic.LoadInt32(&rf.electionVoteCount))
		}
	}

	// for scoped lock
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.getRole() != RoleFollower {
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
			lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTermWithMutexLocked()
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
			if rf.getRole() != RoleCandidate {
				ec = 2
				Warning("peer[%d] Role has been changed when leader election", rf.me)
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
	return int(rf.currentTerm), rf.getRole() == RoleLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

// With mutex locked
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		FATAL("peer[%d] persist currentTerm caught an error[%s]", rf.me, err)
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		FATAL("peer[%d] persist votedFor caught an error[%s]", rf.me, err)
		return
	}
	err = e.Encode(rf.log)
	if err != nil {
		FATAL("peer[%d] persist log caught an error[%s]", rf.me, err)
		return
	}
	raftstate := w.Bytes()
	err, snapshotstate := rf.snapshot.toBytes()
	if err != nil {
		FATAL("peer[%d] persist snapshot caught an error[%s]", rf.me, err)
		return
	}
	rf.persister.Save(raftstate, snapshotstate)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		FATAL("peer[%d] readPersist caught an error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
	Info("LoadFromDisk: peer[%d] restart Term=[%d] votedFor=[%d], logLen=[%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

func (rf *Raft) getLogCount() int {
	return len(rf.log) + rf.snapshot.LastIncludedIndex + 1
}

func (rf *Raft) getLogWithMutexLocked(index int) LogEntry {
	return rf.log[rf.getRealLogIndexWithMutexLocked(index)]
}

func (rf *Raft) getRealLogIndexWithMutexLocked(virtualIndex int) int {
	snapshotPrefix := 0
	if rf.snapshot.hasData() {
		snapshotPrefix = rf.snapshot.LastIncludedIndex + 1
	}
	if virtualIndex < snapshotPrefix {
		FATAL("peer[%d] visit a non-exist logEntry index=[%d], snapPrefix=[%d]", rf.me, virtualIndex, snapshotPrefix)
	}
	return virtualIndex - snapshotPrefix
}

func (rf *Raft) setSnapshotWithMutexLocked(index int, snapshot []byte) {
	rf.log = rf.log[rf.getRealLogIndexWithMutexLocked(index)+1:]
	rf.snapshot.fromLogEntryAndData(rf.getLogWithMutexLocked(index), snapshot)
}

func (rf *Raft) reportFollowerNeedMoreLogWithMutexLocked(followerIndex int, currentNextIndex int, followerPrevLogTerm int) {
	if rf.nextIndex[followerIndex] > currentNextIndex {
		// report a stale value, ignore
		return
	}

	getNextCheckLogIndex := func(currentLog int) int {
		currentLog -= NextIndexMismatchDecreaseCount
		if currentLog < 0 {
			currentLog = 0
		}
		return currentLog
	}

	currentNextIndex = getNextCheckLogIndex(currentNextIndex)
	for currentNextIndex != 0 && rf.getLogWithMutexLocked(currentNextIndex).Term > followerPrevLogTerm {
		currentNextIndex = getNextCheckLogIndex(currentNextIndex)
	}
	rf.nextIndex[followerIndex] = currentNextIndex
}

func (rf *Raft) reportFollowerLogSynchronizeWithMutexLocked(followerIndex int, lastSyncLogIndex int) {
	if rf.nextIndex[followerIndex] < lastSyncLogIndex+1 {
		rf.nextIndex[followerIndex] = lastSyncLogIndex + 1
	}
	if rf.matchIndex[followerIndex] < lastSyncLogIndex {
		rf.matchIndex[followerIndex] = lastSyncLogIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setSnapshotWithMutexLocked(index, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC Reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your Data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// for optimize unsuccessful request
	PrevLogTerm int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	offset            int
	data              []byte
	done              bool
}

type InstallSnapshotReply struct {
	Term int
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
		// speed up election process maybe some peers has high Term but delayed log
		// they will never access a low Term election, so update Term before check valid
		rf.handleNextTermWithMutexLocked(args.Term, -1, false)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		Warning("peer[%d] has vote for [%d] in Term [%d], denied RequestVote from [%d]", rf.me, rf.votedFor,
			rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTermWithMutexLocked()

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		Warning("peer[%d] get delayed log from [%d], denied", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	// get valid vote from candidate can also refresh timeout
	rf.updateLastLeaderHeartbeatTimestamp()
	rf.votedFor = args.CandidateId
	rf.persist()
	Info("peer[%d] access RequestVote from [%d]", rf.me, args.CandidateId)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateLastLeaderHeartbeatTimestamp()

	reply.Success = true
	_, reply.PrevLogTerm = rf.getLastLogIndexAndTermWithMutexLocked()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.handleNextTermWithMutexLocked(args.Term, args.LeaderId, false)
	}
	reply.Term = rf.currentTerm

	if rf.getRole() == RoleCandidate {
		Info("Another leader send append entry when leader election, terminated")
		rf.degradeToFollowerWithMutexLocked()
	}

	updateFollowerCommitFunc := func() {
		// LeaderCommit may greater than known logEntries because of heartbeat request or other stale request
		// it may lead to commit unchecked/mismatched log (case:TestRejoin2B)
		canCommitIndex := args.LeaderCommit
		staleCheckedIndex := args.PrevLogIndex + len(args.Entries)
		if canCommitIndex > staleCheckedIndex {
			canCommitIndex = staleCheckedIndex
		}

		if canCommitIndex > rf.commitIndex {
			bef := rf.commitIndex
			rf.commitIndex = func() int {
				if canCommitIndex < len(rf.log)-1 {
					return canCommitIndex
				} else {
					return len(rf.log) - 1
				}
			}()
			rf.updateAppliedLogWithMutexLocked()
			if bef != rf.commitIndex {
				//Info("CommitUpdate: peer[%d] commitIndex update to [%d]", rf.me, rf.commitIndex)
			}
		}
	}

	// process Entries
	if len(rf.log) <= args.PrevLogIndex || (args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		Warning("peer[%d] AppendEntries first log does not match PrevLogIndex, len=[%d], prev=[%d], need more entries", rf.me, len(rf.log), args.PrevLogIndex)
		return
	}

	for idx, newEntry := range args.Entries {
		indexInServerLog := idx + args.PrevLogIndex + 1
		if indexInServerLog < len(rf.log) {
			if newEntry.Term != rf.log[indexInServerLog].Term {
				rf.log = rf.log[:indexInServerLog]
				Warning("There is a mismatch in LogEntry[%d], will be replaced with leader's log", indexInServerLog)
			} else {
				continue
			}
		}
		rf.log = append(rf.log, newEntry)
	}

	updateFollowerCommitFunc()
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// there is a scenario that exists a partitioned leader (term x) (can access this peer) and a real leader (term x+1),
	// but current peer is not either of majority, must receive x + 1 term snapshot, so rpc must update new term if term > currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.handleNextTermWithMutexLocked(args.Term, args.LeaderId, false)
	}

	reply.Term = rf.currentTerm
	if rf.getRole() == RoleLeader {
		FATAL("That is impossible")
	}

	// new snapshot from leader
	// args.Term == currentTerm && currentTerm >= newSnapshotTerm => args.Term >= newSnapshotTerm
	if args.offset == 0 {
		rf.newSnapshotTerm = args.Term
		rf.newSnapshotData = make([]byte, 0)
	}
	if args.Term != rf.newSnapshotTerm {
		FATAL("peer[%d] not support inordered snapshot package with term[%d] offset[%d]", rf.me, args.Term, args.offset)
	}
	if args.offset != len(rf.newSnapshotData) {
		FATAL("peer[%d] not support inordered snapshot package with offset[%d/%d]", rf.me, args.offset, len(rf.newSnapshotData))
	}
	rf.newSnapshotData = append(rf.newSnapshotData, args.data...)
	if args.done {
		if args.LastIncludedIndex+1 >= rf.getLogCount() {
			lastLog := rf.getLogWithMutexLocked(args.LastIncludedIndex)
			rf.snapshot.fromLogEntryAndData(lastLog, rf.newSnapshotData)
			rf.persist()
		}
		rf.newSnapshotData = make([]byte, 0)
		rf.newSnapshotTerm = -1
	}
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *Reply with RPC Reply, so caller should
// pass &Reply.
// the types of the args and Reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a Reply. If a Reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost Reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the Reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendRPC(server int, methodName string, args interface{}, reply interface{}) bool {
	// call interface{} as args will cause gob error: type not registered for interface
	if methodName == "Raft.RequestVote" {
		return rf.sendRequestVote(server, args.(*RequestVoteArgs), reply.(*RequestVoteReply))
	} else if methodName == "Raft.AppendEntries" {
		return rf.sendAppendEntries(server, args.(*AppendEntriesArgs), reply.(*AppendEntriesReply))
	} else if methodName == "Raft.InstallSnapshot" {
		return rf.sendInstallSnapshot(server, args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply))
	} else {
		FATAL("Unsupported method")
		return false
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := rf.getRole() == RoleLeader

	if !isLeader {
		return 0, 0, false
	}

	rf.mu.Lock()
	newEntry := LogEntry{}
	newEntry.Data = command
	newEntry.Term = rf.currentTerm
	newEntry.IndexInLog = len(rf.log)
	rf.log = append(rf.log, newEntry)
	rf.reportFollowerLogSynchronizeWithMutexLocked(rf.me, len(rf.log)-1)
	rf.persist()
	rf.mu.Unlock()

	//Warning("ClientReq: peer[%d] Client request success, index = [%d]", rf.me, newEntry.IndexInLog)
	return newEntry.IndexInLog + 1, newEntry.Term, isLeader
}

// broadcast all log to followers, update [nextIndex[i],nextIndex)
// return until majority followers accepted or Term changed
func (rf *Raft) updateLogToMajorityFollowers(nextIndex int, currentTerm int) bool {
	replicatedPeers := make(map[int]bool)
	replicatedPeers[rf.me] = true

	for !rf.isMajority(len(replicatedPeers)) {
		rf.mu.Lock()
		// log maybe refresh by other leader, nextIndex may not exist
		if rf.currentTerm != currentTerm {
			Warning("peer[%d] Terminate broadcast log process, because current Term [%d] is newer than [%d]",
				rf.me, rf.currentTerm, currentTerm)
			rf.mu.Unlock()
			return false
		}
		requestList := make([]interface{}, 0, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			if _, ok := replicatedPeers[i]; ok == true {
				requestList = append(requestList, nil)
				continue
			}
			nextIndexForPeer := rf.nextIndex[i]
			appendEntriesReq := AppendEntriesArgs{}
			appendEntriesReq.Term = rf.currentTerm
			appendEntriesReq.PrevLogIndex = nextIndexForPeer - 1
			appendEntriesReq.LeaderId = rf.me
			appendEntriesReq.LeaderCommit = rf.commitIndex
			if nextIndexForPeer != 0 {
				appendEntriesReq.PrevLogTerm = rf.log[appendEntriesReq.PrevLogIndex].Term
			} else {
				appendEntriesReq.PrevLogTerm = -1
			}
			if nextIndexForPeer >= nextIndex {
				//appendEntriesReq.Entries = make([]LogEntry, 0)
				requestList = append(requestList, nil)
				replicatedPeers[i] = true
				continue
			} else {
				sendEntries := make([]LogEntry, 0, nextIndex-nextIndexForPeer)
				for copyLogIndex := nextIndexForPeer; copyLogIndex < nextIndex; copyLogIndex++ {
					sendEntries = append(sendEntries, rf.log[copyLogIndex])
				}
				appendEntriesReq.Entries = sendEntries
			}
			requestList = append(requestList, &appendEntriesReq)
		}
		rf.mu.Unlock()

		replyChan := rf.asyncBroadcastToAllFollowers(requestList, AppendEntriesBroadcastTimeout,
			AppendEntriesMethodName)
		for reply := range replyChan {
			if reply.Status == RPCSuccessful {
				rf.mu.Lock()
				if rf.currentTerm != currentTerm {
					Warning("peer[%d] Terminate broadcast log process, because current Term [%d] is newer than [%d]",
						rf.me, rf.currentTerm, currentTerm)
					rf.mu.Unlock()
					return false
				}
				if rf.getRole() != RoleLeader {
					rf.mu.Unlock()
					return false
				}

				rawReply := reply.Reply.(AppendEntriesReply)
				req := requestList[reply.Index].(*AppendEntriesArgs)
				if rawReply.Success == false {
					if rawReply.Term > rf.currentTerm {
						// don't know who is the leader, wait RPC from leader to update Status
						Warning("peer[%d] There has a new Term, wait background AppendEntries from leader to update Term", rf.me)
						rf.degradeToFollowerWithMutexLocked()
						rf.mu.Unlock()
						return false
					}
					rf.reportFollowerNeedMoreLogWithMutexLocked(reply.Index, req.PrevLogIndex+1, rawReply.PrevLogTerm)
				} else {
					replicatedPeers[reply.Index] = true
					lastSyncIndex := req.PrevLogIndex + len(req.Entries)
					rf.reportFollowerLogSynchronizeWithMutexLocked(reply.Index, lastSyncIndex)
					//Info("leader[%d] update peer[%d] nextIndex = [%d], matchIndex = [%d]", rf.me, reply.Index, newNextIndex, newNextIndex-1)
				}
				rf.mu.Unlock()
			}
		}
	}
	return true
}

type RPCRequestStatus int

const (
	RPCSuccessful = iota
	RPCFailed
	RPCTimeout
)

type RpcReply struct {
	Index  int
	Status RPCRequestStatus
	Reply  interface{}
}

// asyncBroadcastToAllFollowers return RpcReply chan
func (rf *Raft) asyncBroadcastToAllFollowers(requestList []interface{},
	timeout time.Duration, methodName string) chan RpcReply {
	var wg sync.WaitGroup
	replyList := make([]AppendEntriesReply, len(rf.peers))
	replyChan := make(chan RpcReply)

	indexArray := make([]int, 0)
	for i := 0; i < len(rf.peers); i++ {
		indexArray = append(indexArray, i)
	}
	// simulate real network latency and avoid get same majority follower in some special case
	rand.Shuffle(len(indexArray), func(i, j int) {
		indexArray[i], indexArray[j] = indexArray[j], indexArray[i]
	})

	func() {
		for i := 0; i < len(indexArray); i++ {
			node := indexArray[i]
			if node == rf.me {
				continue
			}
			if requestList[node] == nil {
				continue
			}
			wg.Add(1)
			go func(index int) {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				done := make(chan bool, 1)
				go func() {
					done <- rf.sendRPC(index, methodName, requestList[index], &replyList[index])
				}()

				reply := RpcReply{}
				reply.Index = index

				select {
				case success := <-done:
					if success {
						reply.Reply = replyList[index]
						reply.Status = RPCSuccessful
					} else {
						reply.Reply = nil
						reply.Status = RPCFailed
					}
				case <-ctx.Done():
					reply.Reply = nil
					reply.Status = RPCTimeout
				}
				replyChan <- reply
				wg.Done()
			}(node)
		}
	}()
	go func() {
		wg.Wait()
		close(replyChan)
	}()
	return replyChan
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
		if rf.getRole() == RoleFollower &&
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
	msg := ApplyMsg{}
	msg.Command = rf.log[index].Data
	msg.CommandValid = true
	msg.CommandIndex = index + 1
	// TODO: snapshot
	rf.applyCh <- msg
}

func (rf *Raft) updateAppliedLogWithMutexLocked() {
	for rf.commitIndex > rf.lastApplied {
		rf.applyLogEntryToStatusMachine(rf.lastApplied + 1)
		rf.lastApplied += 1
	}
}

func (rf *Raft) degradeToFollowerWithMutexLocked() {
	rf.setRole(RoleFollower)
	Info("EVENT: [%d] degrade to follower in Term[%d], leaderId[%d]", rf.me, rf.currentTerm, rf.leaderId)
}

func (rf *Raft) promoteToLeaderWithMutexLocked() {
	rf.setRole(RoleLeader)
	rf.leaderId = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	Info("EVENT: [%d] promote to leader in Term[%d]", rf.me, rf.currentTerm)
}

func (rf *Raft) handleNextTermWithMutexLocked(newTerm int, leaderId int, isElection bool) {
	rf.currentTerm = newTerm
	rf.leaderId = leaderId
	if isElection {
		rf.setRole(RoleCandidate)
		rf.votedFor = rf.me
		atomic.StoreInt32(&rf.electionVoteCount, 1)
		Info("EVENT: [%d] promote to candidate in Term[%d]", rf.me, rf.currentTerm)
	} else {
		rf.degradeToFollowerWithMutexLocked()
		rf.votedFor = -1
		atomic.StoreInt32(&rf.electionVoteCount, 0)
	}
	rf.persist()
}

func (rf *Raft) broadcastHeartBeat() {
	requestList := make([]interface{}, 0, len(rf.peers))

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				requestList = append(requestList, nil)
				continue
			}
			requestArgs := AppendEntriesArgs{}
			requestArgs.Term = rf.currentTerm
			requestArgs.LeaderCommit = rf.commitIndex
			requestArgs.LeaderId = rf.me
			nextIndexForPeer := rf.nextIndex[i]
			requestArgs.PrevLogIndex = nextIndexForPeer - 1
			if nextIndexForPeer != 0 {
				requestArgs.PrevLogTerm = rf.log[requestArgs.PrevLogIndex].Term
			} else {
				requestArgs.PrevLogTerm = -1
			}
			requestList = append(requestList, &requestArgs)
		}
	}()

	if rf.getRole() != RoleLeader || len(requestList) != len(rf.peers) {
		//Error("E1")
		return
	}
	//Error("ES")
	replyChan := rf.asyncBroadcastToAllFollowers(requestList, AppendEntriesBroadcastTimeout,
		AppendEntriesMethodName)

	// wait all received
	for reply := range replyChan {
		if reply.Status == RPCSuccessful {
			rawReply := reply.Reply.(AppendEntriesReply)
			if rawReply.Success == false {
				rf.mu.Lock()
				//Error("E2")
				if rawReply.Term > rf.currentTerm {
					rf.degradeToFollowerWithMutexLocked()
					rf.mu.Unlock()
					//Error("E3")
					return
				}
				if rf.getRole() != RoleLeader {
					rf.mu.Unlock()
					//Error("E4")
					return
				}
				reqNextIndex := requestList[reply.Index].(*AppendEntriesArgs).PrevLogIndex + 1
				rf.reportFollowerNeedMoreLogWithMutexLocked(reply.Index, reqNextIndex, rawReply.PrevLogTerm)
				rf.mu.Unlock()
			} else {
				//Error("E5")
			}
		}
	}
	//Error("EE")
}

func (rf *Raft) leaderToFollowerHeartbeatLoop() {
	for rf.killed() == false {
		if rf.getRole() == RoleLeader {
			//Info("SN: peer[%d] role[%d] heartbeat", rf.me, rf.getRole())
			rf.broadcastHeartBeat()
			//Info("EN: peer[%d] role[%d] heartbeat", rf.me, rf.getRole())
		}
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) updateFollowersStatus() {
	var logSize, term int
	needUpdateNextIndex := false
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.getRole() != RoleLeader {
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if len(rf.log) > rf.nextIndex[i] || rf.matchIndex[i] < len(rf.log)-1 {
				needUpdateNextIndex = true
				break
			}
		}
		logSize, term = len(rf.log), rf.currentTerm
	}()

	if needUpdateNextIndex {
		if !rf.updateLogToMajorityFollowers(logSize, term) {
			Warning("peer[%d] leader background routine replicated failed", rf.me)
		}
	}

	rf.updateSnapshotToFollowers()
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.getRole() != RoleLeader {
		return
	}

	tempNextIndex := make([]int, len(rf.peers))
	copy(tempNextIndex, rf.nextIndex)

	targetPos := (len(rf.peers) - 1) / 2
	sort.Ints(tempNextIndex)
	threshold := tempNextIndex[targetPos] - 1
	flag := false
	for i := threshold; i >= rf.commitIndex+1; i-- {
		if rf.log[i].Term == rf.currentTerm {
			threshold = i
			flag = true
			break
		}
	}
	if flag && rf.commitIndex < threshold {
		rf.commitIndex = threshold
		rf.updateAppliedLogWithMutexLocked()
		Info("CommitUpdate: leader[%d] update commit index to [%d]", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) needInstallSnapshotWithMutex(index int) bool {
	// a follower could be installed snapshot should meet two conditions
	// 1. leader's matchIndex is up-to-date
	// 2. follower's matchIndex is earlier than first logEntry (must exist snapshot)
	return rf.matchIndex[index]+1 == rf.nextIndex[index] &&
		index == rf.me &&
		rf.snapshot.hasData() &&
		rf.matchIndex[index] < rf.snapshot.LastIncludedIndex
}

func (rf *Raft) updateSnapshotToFollowers() {
	if rf.getRole() != RoleLeader {
		return
	}

	requestList := make([]interface{}, 0, len(rf.peers))

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if !rf.needInstallSnapshotWithMutex(i) {
				requestList = append(requestList, nil)
				continue
			}
			var err error
			requestArgs := InstallSnapshotArgs{}
			requestArgs.Term = rf.currentTerm
			requestArgs.LastIncludedIndex = rf.snapshot.LastIncludedIndex
			requestArgs.LastIncludedTerm = rf.snapshot.LastIncludedTerm
			requestArgs.LeaderId = rf.me
			requestArgs.offset = 0
			requestArgs.done = true
			err, requestArgs.data = rf.snapshot.toBytes()
			requestList = append(requestList, &requestArgs)
			if err != nil {
				FATAL("peer[%d], there is an error when snapshot to bytes [%s]", rf.me, err)
			}
		}
	}()

	if rf.getRole() != RoleLeader || len(requestList) != len(rf.peers) {
		return
	}

	replyChan := rf.asyncBroadcastToAllFollowers(requestList, InstallSnapshotTimeout,
		AppendEntriesMethodName)

	for reply := range replyChan {
		rf.mu.Lock()
		rawReply := reply.Reply.(InstallSnapshotReply)
		if rawReply.Term > rf.currentTerm {
			Warning("peer[%d] There has a new Term, wait background AppendEntries from leader to update Term", rf.me)
			rf.degradeToFollowerWithMutexLocked()
			return
		}
		if rf.getRole() != RoleLeader {
			rf.mu.Unlock()
			return
		}
		rawReq := requestList[reply.Index].(InstallSnapshotArgs)
		rf.reportFollowerLogSynchronizeWithMutexLocked(reply.Index, rawReq.LastIncludedIndex)
	}
}

// update nextIndex, matchIndex, commitIndex, snapshot
func (rf *Raft) leaderToFollowerUpdateStatusLoop() {
	for rf.killed() == false {
		if rf.getRole() == RoleLeader {
			rf.updateFollowersStatus()
			rf.updateLeaderCommitIndex()
		}
		time.Sleep(LeaderLoopInterval)
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
	rf.applyCh = applyCh
	rf.setRole(RoleFollower)
	rf.updateLastLeaderHeartbeatTimestamp()
	rf.leaderElectionTimeout = rf.getRandomLeaderElectionTimeout()
	rf.commitIndex = -1
	rf.lastApplied = -1
	atomic.StoreInt32(&rf.electionVoteCount, 0)
	rf.leaderId = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.snapshot = NewSnapshotPackage()
	rf.newSnapshotTerm = -1
	rf.newSnapshotData = make([]byte, 0)

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
	}
	rf.mu.me = rf.me
	rf.mu.locklog = make([]string, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	err := rf.snapshot.fromBytes(persister.ReadSnapshot())
	if err != nil {
		FATAL("peer[%d] read snapshot from persist caught error [%s]", rf.me, err)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderToFollowerHeartbeatLoop()
	go rf.leaderToFollowerUpdateStatusLoop()
	return rf
}
