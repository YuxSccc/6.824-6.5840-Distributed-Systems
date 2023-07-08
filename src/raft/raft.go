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

// must require
// 1. LeaderNotReceiveMajorityTimeout < LeaderElectionTimeout (avoid multi leader)
// 2. LeaderNotReceiveMajorityTimeout < RequestTimeout in application server (avoid stale leader)

// TODO: maybe notification for ApplyLoop & LeaderLoop is needed (Performance).

const (
	LeaderElectionTimeout           = time.Millisecond * 700
	HeartbeatTimeout                = time.Millisecond * 50
	AppendEntriesBroadcastTimeout   = time.Millisecond * 150
	LeaderNotReceiveMajorityTimeout = time.Millisecond * 500
	InstallSnapshotTimeout          = time.Millisecond * 150

	HeartbeatInterval  = time.Millisecond * 10
	LeaderLoopInterval = time.Millisecond * 10
	ApplyLoopInterval  = time.Millisecond * 10

	NextIndexMismatchDecreaseCount = 100
	RequestVoteMethodName          = "Raft.RequestVote"
	AppendEntriesMethodName        = "Raft.AppendEntries"
	InstallSnapshotMethodName      = "Raft.InstallSnapshot"
)

type LogEntry struct {
	Term       int
	IndexInLog int
	Data       interface{}
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
	lastMajorityHeartbeatTs           int64
	leaderElectionTimeout             time.Duration
	electionVoteCount                 int32
	leaderId                          int32
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

func (s *SnapshotPackage) reset() {
	s.Term = -1
	s.Data = make([]byte, 0)
	s.LastIncludedIndex = -1
	s.LastIncludedTerm = -1
}

func (s *SnapshotPackage) fromIndexTermAndData(lastIndex int, lastTerm int, data []byte) {
	s.Data = data
	s.LastIncludedIndex = lastIndex
	s.LastIncludedTerm = lastTerm
	s.Term = lastTerm // arbitrary value except -1
}

func (s *SnapshotPackage) setData(data []byte) {
	if data == nil || len(data) == 0 {
		s.reset()
		return
	}
	s.Term = 0
	s.Data = clone(data)
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
	if rf.getLogCount() == 0 {
		return -1, -1
	}
	if rf.snapshot.hasData() && len(rf.log) == 0 {
		return rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm
	}
	lastEntry := rf.getLog(rf.getLogCount() - 1)
	return lastEntry.IndexInLog, lastEntry.Term
}

func (rf *Raft) doLeaderElection() {
	requestList := make([]interface{}, 0, len(rf.peers))
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
				requestList = append(requestList, nil)
				continue
			}
			requestArgs := RequestVoteArgs{}
			requestArgs.Term = rf.currentTerm
			requestArgs.CandidateId = rf.me
			lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTermWithMutexLocked()
			requestArgs.LastLogIndex = lastLogIndex
			requestArgs.LastLogTerm = lastLogTerm
			requestList = append(requestList, &requestArgs)
		}
	}()

	if rf.getRole() != RoleCandidate || len(requestList) != len(rf.peers) {
		return
	}

	replyChan := rf.asyncBroadcastToAllFollowers(requestList, LeaderElectionTimeout, RequestVoteMethodName)

	// wait all received
	for reply := range replyChan {
		if reply.Status == RPCSuccessful {
			rawReply := reply.Reply.(*RequestVoteReply)
			rf.mu.Lock()
			if rf.getRole() != RoleCandidate {
				Warning("peer[%d] Role has been changed when leader election", rf.me)
				rf.mu.Unlock()
				break
			}
			// maybe got another leader appendEntry, role change to follower
			if rawReply.Term <= rf.currentTerm && rf.getRole() == RoleCandidate && rawReply.VoteGranted {
				atomic.AddInt32(&rf.electionVoteCount, 1)
				Info("peer[%d] receive access vote from [%d], current vote count [%d]",
					rf.me, reply.Index, atomic.LoadInt32(&rf.electionVoteCount))
			}
			if rf.isMajority(int(atomic.LoadInt32(&rf.electionVoteCount))) {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		} else if reply.Status == RPCFailed {
			//Error("peer[%d] Heartbeat RequestVote for peer[%d] failed, status=[%d]", rf.me, reply.Index, reply.Status)
		}
	}

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.getRole() == RoleCandidate {
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
	return rf.currentTerm, rf.getRole() == RoleLeader
}

func (rf *Raft) GetLastMajorityHeartbeatTsInMilli() int64 {
	return atomic.LoadInt64(&rf.lastMajorityHeartbeatTs)
}

func (rf *Raft) IsCommitIdUpToDate() bool {
	if rf.getRole() != RoleLeader {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, _ := rf.getLastLogIndexAndTermWithMutexLocked()
	return term == rf.currentTerm
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
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		FATAL("peer[%d] persist votedFor caught an error[%s]", rf.me, err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		FATAL("peer[%d] persist log caught an error[%s]", rf.me, err)
	}
	err = e.Encode(rf.snapshot.LastIncludedTerm)
	if err != nil {
		FATAL("peer[%d] persist lastIncludedTerm an error[%s]", rf.me, err)
	}
	err = e.Encode(rf.snapshot.LastIncludedIndex)
	if err != nil {
		FATAL("peer[%d] persist lastIncludedIndex an error[%s]", rf.me, err)
	}
	raftstate := w.Bytes()
	snapshotstate := rf.snapshot.Data
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
	var currentTerm, votedFor, snapshotLastTerm, snapshotLastIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotLastTerm) != nil ||
		d.Decode(&snapshotLastIndex) != nil {
		FATAL("peer[%d] readPersist caught an error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshot.LastIncludedTerm = snapshotLastTerm
		rf.snapshot.LastIncludedIndex = snapshotLastIndex
	}
	Info("LoadFromDisk: peer[%d] restart Term=[%d] votedFor=[%d], logLen=[%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

func (rf *Raft) getLogCount() int {
	return len(rf.log) + rf.snapshot.LastIncludedIndex + 1
}

func (rf *Raft) getLog(index int) *LogEntry {
	return &rf.log[rf.getRealLogIndex(index)]
}

func (rf *Raft) getRealLogIndex(virtualIndex int) int {
	snapshotPrefix := 0
	if rf.snapshot.hasData() {
		snapshotPrefix = rf.snapshot.LastIncludedIndex + 1
	}
	if virtualIndex < snapshotPrefix {
		FATAL("peer[%d] visit a non-exist logEntry index=[%d], snapPrefix=[%d]", rf.me, virtualIndex, snapshotPrefix)
	}
	return virtualIndex - snapshotPrefix
}

func (rf *Raft) isLogInSnapshot(index int) bool {
	return rf.snapshot.hasData() && index <= rf.snapshot.LastIncludedIndex
}

func (rf *Raft) setSnapshotWithMutexLocked(lastIndex int, lastTerm int, snapshot []byte) bool {
	if rf.snapshot.hasData() && rf.snapshot.LastIncludedIndex >= lastIndex {
		return false
	}
	if lastIndex+1 >= rf.getLogCount() {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = rf.log[rf.getRealLogIndex(lastIndex)+1:]
	}
	rf.snapshot.fromIndexTermAndData(lastIndex, lastTerm, snapshot)
	rf.persist()
	return true
}

func (rf *Raft) setSnapshotByLogWithMutexLocked(logIndex int, snapshot []byte) bool {
	if rf.isLogInSnapshot(logIndex) {
		return false
	}
	lastLog := rf.getLog(logIndex)
	return rf.setSnapshotWithMutexLocked(lastLog.IndexInLog, lastLog.Term, snapshot)
}

func (rf *Raft) reportFollowerNeedMoreLogWithMutexLocked(followerIndex int, currentNextIndex int, followerPrevLogTerm int) {
	if rf.nextIndex[followerIndex] > currentNextIndex {
		// report a stale value, ignore
		return
	}

	inSnapshot := rf.isLogInSnapshot(currentNextIndex)

	// if nextIndex > lastIncludeIndex + 1, try to send more logs but don't intersect with snapshots to avoid making too much InstallSnapshot RPC
	// or just make nextIndex equal to matchIndex + 1 to emit InstallSnapshot soon
	if inSnapshot || (rf.snapshot.hasData() && currentNextIndex == rf.snapshot.LastIncludedIndex+1) {
		rf.nextIndex[followerIndex] = rf.matchIndex[followerIndex] + 1
		return
	}

	minTarget := rf.matchIndex[followerIndex] + 1
	if rf.snapshot.hasData() && minTarget < rf.snapshot.LastIncludedIndex+1 {
		minTarget = rf.snapshot.LastIncludedIndex + 1
	}

	getNextCheckLogIndex := func(currentLog int) int {
		currentLog -= NextIndexMismatchDecreaseCount
		if currentLog < minTarget {
			currentLog = minTarget
		}
		return currentLog
	}

	currentNextIndex = getNextCheckLogIndex(currentNextIndex)
	for currentNextIndex != minTarget && rf.getLog(currentNextIndex).Term > followerPrevLogTerm {
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
	index -= 1
	if rf.setSnapshotByLogWithMutexLocked(index, snapshot) {
		Debug("peer[%d] snapshot from log is set, lastIndex=[%d]", rf.me, index)
	}
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
	LeaderId     int32
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
	LeaderId          int32
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
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
		Debug("peer[%d] get delayed log from [%d], denied", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	// get valid vote from candidate can also refresh timeout
	rf.updateLastLeaderHeartbeatTimestamp()
	rf.votedFor = args.CandidateId
	rf.persist()
	Debug("peer[%d] access RequestVote from [%d]", rf.me, args.CandidateId)
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
				if canCommitIndex < rf.getLogCount()-1 {
					return canCommitIndex
				} else {
					return rf.getLogCount() - 1
				}
			}()
			// rf.updateAppliedLogWithMutexLocked()
			if bef != rf.commitIndex {
				//Debug("CommitUpdate: peer[%d] commitIndex update to [%d]", rf.me, rf.commitIndex)
			}
		}
	}

	// process Entries
	if rf.getLogCount() <= args.PrevLogIndex || (args.PrevLogIndex != -1 && !rf.isLogInSnapshot(args.PrevLogIndex) &&
		rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm) {
		reply.Success = false
		Debug("peer[%d] AppendEntries first log does not match PrevLogIndex, len=[%d], prev=[%d], "+
			"need more entries", rf.me, rf.getLogCount(), args.PrevLogIndex)
		return
	}

	//if len(args.Entries) != 0 {
	//	Debug("peer[%d] entris[%v] [%d]", rf.me, args.Entries, args.PrevLogIndex)
	//}

	for idx, newEntry := range args.Entries {
		indexInServerLog := idx + args.PrevLogIndex + 1
		if rf.isLogInSnapshot(indexInServerLog) {
			continue
		}
		if indexInServerLog < rf.getLogCount() {
			if newEntry.Term != rf.getLog(indexInServerLog).Term {
				rf.log = rf.log[:rf.getRealLogIndex(indexInServerLog)]
				Debug("There is a mismatch in LogEntry[%d], will be replaced with leader's log", indexInServerLog)
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
	if args.Offset == 0 {
		rf.newSnapshotTerm = args.Term
		rf.newSnapshotData = make([]byte, 0)
	}
	if args.Term != rf.newSnapshotTerm {
		FATAL("peer[%d] not support inordered snapshot package with term[%d] offset[%d]", rf.me, args.Term, args.Offset)
	}
	if args.Offset != len(rf.newSnapshotData) {
		FATAL("peer[%d] not support inordered snapshot package with offset[%d/%d]", rf.me, args.Offset, len(rf.newSnapshotData))
	}
	rf.newSnapshotData = append(rf.newSnapshotData, args.Data...)
	if args.Done {
		if rf.setSnapshotWithMutexLocked(args.LastIncludedIndex, args.LastIncludedTerm, args.Data) {
			rf.persist()
			rf.applySnapshotWithMutexLocked()
			Info("peer[%d] snapshot from leader is set, lastIndex=[%d]", rf.me, rf.snapshot.LastIncludedIndex)
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
	ok := rf.peers[server].Call(RequestVoteMethodName, args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call(AppendEntriesMethodName, args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call(InstallSnapshotMethodName, args, reply)
	return ok
}

func (rf *Raft) sendRPC(server int, methodName string, args interface{}, reply interface{}) bool {
	// call interface{} as args will cause gob error: type not registered for interface
	if methodName == RequestVoteMethodName {
		return rf.sendRequestVote(server, args.(*RequestVoteArgs), reply.(*RequestVoteReply))
	} else if methodName == AppendEntriesMethodName {
		return rf.sendAppendEntries(server, args.(*AppendEntriesArgs), reply.(*AppendEntriesReply))
	} else if methodName == InstallSnapshotMethodName {
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
	newEntry.IndexInLog = rf.getLogCount()
	rf.log = append(rf.log, newEntry)
	rf.reportFollowerLogSynchronizeWithMutexLocked(rf.me, rf.getLogCount()-1)
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
			if rf.isLogInSnapshot(nextIndexForPeer) {
				// wait for InstallSnapshot
				requestList = append(requestList, nil)
				continue
			}
			appendEntriesReq := AppendEntriesArgs{}
			appendEntriesReq.Term = rf.currentTerm
			appendEntriesReq.PrevLogIndex = nextIndexForPeer - 1
			appendEntriesReq.LeaderId = int32(rf.me)
			appendEntriesReq.LeaderCommit = rf.commitIndex
			if appendEntriesReq.PrevLogIndex == -1 {
				appendEntriesReq.PrevLogTerm = -1
			} else if rf.snapshot.hasData() && appendEntriesReq.PrevLogIndex == rf.snapshot.LastIncludedIndex {
				appendEntriesReq.PrevLogTerm = rf.snapshot.LastIncludedTerm
			} else {
				appendEntriesReq.PrevLogTerm = rf.getLog(appendEntriesReq.PrevLogIndex).Term
			}
			if nextIndexForPeer >= nextIndex {
				// nextIndex for this peer has reached this target
				requestList = append(requestList, nil)
				replicatedPeers[i] = true
				continue
			} else {
				sendEntries := make([]LogEntry, 0, nextIndex-nextIndexForPeer)
				for copyLogIndex := nextIndexForPeer; copyLogIndex < nextIndex; copyLogIndex++ {
					sendEntries = append(sendEntries, *rf.getLog(copyLogIndex))
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

				rawReply := reply.Reply.(*AppendEntriesReply)
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
					Debug("leader[%d] update peer[%d] nextIndex = [%d], matchIndex = [%d]", rf.me, reply.Index, lastSyncIndex+1, lastSyncIndex)
				}
				rf.mu.Unlock()
			} else if reply.Status == RPCFailed {
				//Error("peer[%d] AppendEntries for peer[%d] failed, status=[%d]", rf.me, reply.Index, reply.Status)
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

	type replyCreator func() interface{}
	replyCreators := map[string]replyCreator{
		RequestVoteMethodName:     func() interface{} { return &RequestVoteReply{} },
		AppendEntriesMethodName:   func() interface{} { return &AppendEntriesReply{} },
		InstallSnapshotMethodName: func() interface{} { return &InstallSnapshotReply{} },
	}
	creator, ok := replyCreators[methodName]
	if !ok {
		FATAL("not support method name")
	}
	replyList := make([]interface{}, len(rf.peers))
	for i := range replyList {
		replyList[i] = creator()
	}

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
					done <- rf.sendRPC(index, methodName, requestList[index], replyList[index])
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

func (rf *Raft) applyLogEntryToStateMachine(log *LogEntry) {
	msg := ApplyMsg{}
	msg.Command = log.Data
	msg.CommandValid = true
	msg.CommandIndex = log.IndexInLog + 1
	//Debug("peer[%d] apply log[%v]", rf.me, log)
	rf.applyCh <- msg
}

func (rf *Raft) applySnapshotToStateMachine(snapshotPackage *SnapshotPackage) {
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.SnapshotValid = true
	msg.Snapshot = snapshotPackage.Data
	msg.SnapshotTerm = snapshotPackage.LastIncludedTerm
	msg.SnapshotIndex = snapshotPackage.LastIncludedIndex + 1
	//Debug("peer[%d] apply ss[%d]", rf.me, msg.SnapshotIndex)
	rf.applyCh <- msg
}

func (rf *Raft) applySnapshotWithMutexLocked() {
	if rf.snapshot.LastIncludedIndex <= rf.lastApplied {
		return
	}
	snapshot := *rf.snapshot
	// deep copy & avoid deadlock with server channel
	rf.mu.Unlock()
	rf.applySnapshotToStateMachine(&snapshot)
	rf.mu.Lock()
	if rf.lastApplied < snapshot.LastIncludedIndex {
		rf.lastApplied = snapshot.LastIncludedIndex
	}
}

func (rf *Raft) updateAppliedLogWithMutexLocked() {
	if rf.snapshot.LastIncludedIndex >= rf.lastApplied+1 {
		rf.applySnapshotWithMutexLocked()
	}
	for rf.commitIndex > rf.lastApplied {
		// deep copy & avoid deadlock with server channel
		applyLog := *rf.getLog(rf.lastApplied + 1)
		rf.lastApplied += 1
		rf.mu.Unlock()
		rf.applyLogEntryToStateMachine(&applyLog)
		rf.mu.Lock()
	}
}

func (rf *Raft) degradeToFollowerWithMutexLocked() {
	if rf.getRole() == RoleLeader {
		Info("EVENT: peer[%d] degrade to follower in Term[%d], leaderId[%d]", rf.me, rf.currentTerm, rf.leaderId)
	} else {
		Debug("EVENT: peer[%d] degrade to follower in Term[%d], leaderId[%d]", rf.me, rf.currentTerm, rf.leaderId)
	}
	rf.setRole(RoleFollower)
}

func (rf *Raft) promoteToLeaderWithMutexLocked() {
	rf.setRole(RoleLeader)
	rf.leaderId = int32(rf.me)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLogCount()
		rf.matchIndex[i] = -1
	}
	Info("EVENT: peer[%d] promote to leader in Term[%d]", rf.me, rf.currentTerm)
}

func (rf *Raft) handleNextTermWithMutexLocked(newTerm int, leaderId int32, isElection bool) {
	rf.currentTerm = newTerm
	rf.leaderId = leaderId
	if isElection {
		rf.setRole(RoleCandidate)
		rf.votedFor = rf.me
		atomic.StoreInt32(&rf.electionVoteCount, 1)
		Debug("EVENT: peer[%d] promote to candidate in Term[%d]", rf.me, rf.currentTerm)
	} else {
		rf.degradeToFollowerWithMutexLocked()
		rf.votedFor = -1
		atomic.StoreInt32(&rf.electionVoteCount, 0)
	}
	rf.persist()
}

func (rf *Raft) broadcastHeartBeat(majority bool) {
	requestList := make([]interface{}, 0, len(rf.peers))

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				requestList = append(requestList, nil)
				continue
			}
			nextIndexForPeer := rf.nextIndex[i]
			if rf.isLogInSnapshot(nextIndexForPeer) {
				// wait for InstallSnapshot
				requestList = append(requestList, nil)
				continue
			}
			appendEntriesReq := AppendEntriesArgs{}
			appendEntriesReq.Term = rf.currentTerm
			appendEntriesReq.PrevLogIndex = nextIndexForPeer - 1
			appendEntriesReq.LeaderId = int32(rf.me)
			appendEntriesReq.LeaderCommit = rf.commitIndex
			if appendEntriesReq.PrevLogIndex == -1 {
				appendEntriesReq.PrevLogTerm = -1
			} else if rf.snapshot.hasData() && appendEntriesReq.PrevLogIndex == rf.snapshot.LastIncludedIndex {
				appendEntriesReq.PrevLogTerm = rf.snapshot.LastIncludedTerm
			} else {
				appendEntriesReq.PrevLogTerm = rf.getLog(appendEntriesReq.PrevLogIndex).Term
			}
			requestList = append(requestList, &appendEntriesReq)
		}
	}()

	if rf.getRole() != RoleLeader || len(requestList) != len(rf.peers) {
		return
	}
	replyChan := rf.asyncBroadcastToAllFollowers(requestList, HeartbeatTimeout,
		AppendEntriesMethodName)

	// wait all received
	// make snapshot node is unsuccessful because if these node can get successful heartbeat it will not lead to a timeout
	receiveHeartbeatCnt := 1
	for reply := range replyChan {
		if reply.Status == RPCSuccessful {
			rawReply := reply.Reply.(*AppendEntriesReply)
			if rawReply.Success == false {
				rf.mu.Lock()
				if rawReply.Term > rf.currentTerm {
					rf.degradeToFollowerWithMutexLocked()
					rf.mu.Unlock()
					return
				}
				if rf.getRole() != RoleLeader {
					rf.mu.Unlock()
					return
				}
				reqNextIndex := requestList[reply.Index].(*AppendEntriesArgs).PrevLogIndex + 1
				rf.reportFollowerNeedMoreLogWithMutexLocked(reply.Index, reqNextIndex, rawReply.PrevLogTerm)
				rf.mu.Unlock()
			}
			receiveHeartbeatCnt += 1
			if rf.isMajority(receiveHeartbeatCnt) {
				atomic.StoreInt64(&rf.lastMajorityHeartbeatTs, time.Now().UnixMicro())
				if majority {
					return
				}
			}
		} else if reply.Status == RPCFailed {
			//Error("peer[%d] Heartbeat AppendEntries for peer[%d] failed, status=[%d]", rf.me, reply.Index, reply.Status)
		}
	}
}

func (rf *Raft) ActivelyTriggerHeartbeat() {
	if rf.getRole() == RoleLeader {
		rf.broadcastHeartBeat(true)
	}
}

func (rf *Raft) leaderToFollowerHeartbeatLoop() {
	for rf.killed() == false {
		if rf.getRole() == RoleLeader {
			rf.broadcastHeartBeat(false)
		}
		if rf.getRole() == RoleLeader &&
			time.Now().Sub(time.UnixMicro(atomic.LoadInt64(&rf.lastMajorityHeartbeatTs))) > LeaderNotReceiveMajorityTimeout {
			rf.mu.Lock()
			rf.degradeToFollowerWithMutexLocked()
			rf.mu.Unlock()
			Info("peer[%d] leader not receive heartbeat from majority, degrade", rf.me)
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
			if rf.nextIndex[i] < rf.getLogCount() || rf.matchIndex[i] < rf.getLogCount()-1 {
				needUpdateNextIndex = true
				break
			}
		}
		logSize, term = rf.getLogCount(), rf.currentTerm
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
		if rf.isLogInSnapshot(i) || rf.getLog(i).Term == rf.currentTerm {
			threshold = i
			flag = true
			break
		}
	}
	if flag && rf.commitIndex < threshold {
		rf.commitIndex = threshold
		//rf.updateAppliedLogWithMutexLocked()
		Debug("CommitUpdate: leader[%d] update commit index to [%d]", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) needInstallSnapshotWithMutex(index int) bool {
	// a follower could be installed snapshot should meet two conditions
	// 1. leader's matchIndex is up-to-date
	// 2. follower's matchIndex is earlier than first logEntry (must exist snapshot)
	return rf.matchIndex[index]+1 == rf.nextIndex[index] &&
		index != rf.me &&
		rf.snapshot.hasData() &&
		rf.matchIndex[index] < rf.snapshot.LastIncludedIndex
}

func (rf *Raft) updateSnapshotToFollowers() {
	if rf.getRole() != RoleLeader {
		return
	}

	requestList := make([]interface{}, 0, len(rf.peers))

	needSendRPC := false

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if !rf.needInstallSnapshotWithMutex(i) {
				requestList = append(requestList, nil)
				continue
			}
			requestArgs := InstallSnapshotArgs{}
			requestArgs.Term = rf.currentTerm
			requestArgs.LastIncludedIndex = rf.snapshot.LastIncludedIndex
			requestArgs.LastIncludedTerm = rf.snapshot.LastIncludedTerm
			requestArgs.LeaderId = int32(rf.me)
			requestArgs.Offset = 0
			requestArgs.Done = true
			requestArgs.Data = rf.snapshot.Data
			requestList = append(requestList, &requestArgs)
			needSendRPC = true
		}
	}()

	if needSendRPC == false || rf.getRole() != RoleLeader || len(requestList) != len(rf.peers) {
		return
	}

	replyChan := rf.asyncBroadcastToAllFollowers(requestList, InstallSnapshotTimeout, InstallSnapshotMethodName)

	for reply := range replyChan {
		if reply.Status == RPCSuccessful {
			rf.mu.Lock()
			rawReply := reply.Reply.(*InstallSnapshotReply)
			if rawReply.Term > rf.currentTerm {
				Warning("peer[%d] There has a new Term, wait background AppendEntries from leader to update Term", rf.me)
				rf.degradeToFollowerWithMutexLocked()
				rf.mu.Unlock()
				return
			}
			if rf.getRole() != RoleLeader {
				rf.mu.Unlock()
				return
			}
			rawReq := requestList[reply.Index].(*InstallSnapshotArgs)
			rf.reportFollowerLogSynchronizeWithMutexLocked(reply.Index, rawReq.LastIncludedIndex)
			rf.mu.Unlock()
		} else if reply.Status == RPCFailed {
			//Error("peer[%d] InstallSnapshot for peer[%d] failed, status=[%d]", rf.me, reply.Index, reply.Status)
		}
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

func (rf *Raft) applyLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.updateAppliedLogWithMutexLocked()
		rf.mu.Unlock()
		time.Sleep(ApplyLoopInterval)
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
	rf.mu.Me = rf.me
	rf.mu.Locklog = make([]string, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// STUPID 6.824 INTERFACE DESIGN: I don't understand why framework limit snapshot=[LastIndex, log], it doesn't contain
	// LastTerm, although I could make all AppendEntries RPC which PrevLog equals to LastLog in snapshot must be accepted by
	// mismatch check (because it has been committed), it will break AppendEntries args. so I store LastTerm in Persist instead of Snapshot
	// Another stupid thing is server index is start from 1... , I don't want to change any raft library code, so decode snapshots
	// or receive request must inc or dec by 1
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot.setData(persister.ReadSnapshot())
	if rf.snapshot.hasData() {
		Info("peer[%d] LoadFromDisk snapshot index=[%d], term=[%d]", rf.me, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderToFollowerHeartbeatLoop()
	go rf.leaderToFollowerUpdateStatusLoop()
	go rf.applyLoop()
	return rf
}
