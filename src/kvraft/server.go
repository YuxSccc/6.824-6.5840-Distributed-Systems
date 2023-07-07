package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RequestCheckFinalizedInterval = time.Millisecond * 10
)

type Op struct {
	Key           string
	Value         string
	Type          string
	ClientId      int64
	Serial        int64
	EmitTsInMilli int64
}

type KVServer struct {
	mu        LoggingMutex
	syncMutex sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxRaftState     int // snapshot if log grows this big
	currentRaftState int

	// Your definitions here.
	kvMap             map[string]string
	clientLastRequest map[int64]Op
}

func (kv *KVServer) isLeader() bool {
	_, leader := kv.rf.GetState()
	return leader
}

func (kv *KVServer) waitRaftSync() Err {
	// even if server is not leader, it will return err when wait finalized
	kv.syncMutex.Lock()
	defer kv.syncMutex.Unlock()
	if !kv.rf.IsCommitIdUpToDate() {
		noop := Op{}
		noop.Serial = nrand()
		noop.ClientId = -1
		noop.EmitTsInMilli = time.Now().UnixMilli()
		noop.Type = OpNil
		return kv.sendAndWaitFinalized(noop)
	}
	return OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if !kv.isLeader() {
		reply.Err = ErrNotLeader
		return
	}
	if err := kv.waitRaftSync(); err != OK {
		reply.Err = err
		return
	}

	kv.mu.Lock()
	requestTsInMilli := time.Now().UnixMilli()
	value, ok := kv.kvMap[args.Key]
	kv.mu.Unlock()

	Debug("server[%d] Get(%s)[%d] wait finalized", kv.me, args.Key, args.Serial)
	for {
		if !kv.isLeader() {
			reply.Err = ErrNotLeader
			break
		}
		if kv.rf.GetLastMajorityHeartbeatTsInMilli() > requestTsInMilli {
			if !ok {
				//reply.Err = ErrNoKey
				reply.Err = OK
				reply.Value = ""
				break
			}
			reply.Err = OK
			reply.Value = value
			break
		}
		time.Sleep(RequestCheckFinalizedInterval)
	}
	Debug("server[%d] Get(%s)[%d] finalized", kv.me, args.Key, args.Serial)
}

func (kv *KVServer) sendAndWaitFinalized(op Op) Err {
	// lock just use to guarantee that EmitTs increments with Raft entry index
	// use ts instead of index for the ability to check timeout (not now)
	kv.mu.Lock()
	op.EmitTsInMilli = time.Now().UnixMilli()
	_, reqTerm, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		return ErrNotLeader
	}
	Debug("server[%d] %s(%s=%s)[%d] wait finalized", kv.me, op.Type, op.Key, op.Value, op.Serial)
	for {
		if term, isLeader := kv.rf.GetState(); term != reqTerm || !isLeader {
			//Debug("server[%d] %s(%s=%s)[%d] not leader anymore", kv.me, op.Type, op.Key, op.Value, op.Serial)
			return ErrNotLeader
		}
		kv.mu.Lock()
		finalizedOp, ok := kv.clientLastRequest[op.ClientId]
		kv.mu.Unlock()
		if ok && finalizedOp.Serial == op.Serial {
			Debug("server[%d] %s(%s=%s)[%d] finalized", kv.me, op.Type, op.Key, op.Value, op.Serial)
			return OK
		}
		if ok && finalizedOp.EmitTsInMilli > op.EmitTsInMilli {
			FATAL("That should be impossible, client send request concurrently. [%v] [%v]", finalizedOp, op)
			return ErrUnknown
		}
		time.Sleep(RequestCheckFinalizedInterval)
	}
}

func (kv *KVServer) generateSnapshotWithLock() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.kvMap)
	if err != nil {
		FATAL("server[%d] encode kvMap caught an error[%s] when generate snapshot", kv.me, err)
	}
	err = e.Encode(kv.clientLastRequest)
	if err != nil {
		FATAL("server[%d] encode clientLastRequest caught an error[%s] when generate snapshot", kv.me, err)
	}
	return w.Bytes()
}

func (kv *KVServer) handleCommandApplyMsg(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	snapshot := make([]byte, 0)
	op, ok := msg.Command.(Op)
	if !ok {
		FATAL("server[%d] That's impossible", kv.me)
	}

	if clientLastMsg, exist := kv.clientLastRequest[op.ClientId]; exist && clientLastMsg.Serial == op.Serial {
		Debug("server[%d] get a duplicated [%s] message, skip", kv.me, op.Type)
		kv.mu.Unlock()
		return
	}

	if op.Type == OpPut {
		kv.kvMap[op.Key] = op.Value
	} else if op.Type == OpAppend {
		oldValue, _ := kv.kvMap[op.Key]
		kv.kvMap[op.Key] = oldValue + op.Value
	} else if op.Type == OpNil {
	} else {
		FATAL("server[%d] Unknown op [%s]", kv.me, op.Type)
	}

	kv.clientLastRequest[op.ClientId] = op
	kv.currentRaftState += 1
	if kv.currentRaftState >= kv.maxRaftState {
		kv.currentRaftState = 0
		snapshot = kv.generateSnapshotWithLock()
	}
	kv.mu.Unlock()

	if len(snapshot) > 0 {
		kv.rf.Snapshot(msg.CommandIndex, snapshot)
		Info("server[%d] snapshot with index[%d] successful", kv.me, msg.CommandIndex)
	}
}

func (kv *KVServer) handleSnapshotApplyMsg(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.kvMap) != nil ||
		d.Decode(&kv.clientLastRequest) != nil {
		FATAL("server[%d] handle snapshot caught an error", kv.me)
	}
}

func (kv *KVServer) applierLoop() {
	for entry := range kv.applyCh {
		if entry.SnapshotValid {
			kv.handleSnapshotApplyMsg(&entry)
		} else if entry.CommandValid {
			kv.handleCommandApplyMsg(&entry)
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{}
	op.Type = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Serial = args.Serial
	op.ClientId = args.ClientId
	reply.Err = kv.sendAndWaitFinalized(op)
}

func (kv *KVServer) IsLeader(args *GetLeaderArgs, reply *GetLeaderReply) {
	reply.IsLeader = kv.isLeader()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	if maxraftstate == -1 {
		maxraftstate = math.MaxInt
	}
	kv.maxRaftState = maxraftstate
	kv.currentRaftState = 0
	kv.clientLastRequest = make(map[int64]Op)
	kv.kvMap = make(map[string]string)
	kv.mu.me = me
	kv.mu.locklog = make([]string, 0)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applierLoop()

	return kv
}
