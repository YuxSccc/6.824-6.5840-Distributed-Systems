package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	WaitRaftElectDuration = time.Millisecond * 100
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader             int
	clientId           int64
	mu                 sync.Mutex
	writeRequestLock   sync.Mutex
	updateLeaderLock   sync.Mutex
	lastUpdateLeaderTs time.Time
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.clientId = nrand()
	ck.lastUpdateLeaderTs = time.Unix(0, 0)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	Debug("client receive get(%s)", key)
	args := GetArgs{}
	args.Key = key
	args.Serial = nrand()
	// You will have to modify this function.
	return ck.sendRPCUntilSuccess(GetRPC, &args)
}

func (ck *Clerk) updateLeaderWithLock() {
	enterTs := time.Now()
	ck.updateLeaderLock.Lock()
	defer ck.updateLeaderLock.Unlock()
	if ck.lastUpdateLeaderTs.After(enterTs) {
		return
	}
	for {
		oriLeader := ck.leader
		for i := 0; i < len(ck.servers); i++ {
			args := GetLeaderArgs{}
			reply := GetLeaderReply{}
			ok := ck.servers[ck.leader].Call(IsLeaderRPC, &args, &reply)
			// TODO: broadcast GetLeader, rpc return term & leader
			if ok && reply.IsLeader {
				ck.lastUpdateLeaderTs = time.Now()
				//Debug("client update leader to [%d]", ck.leader)
				return
			} else {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
		}
		if ck.leader == oriLeader {
			// not one is a leader
			time.Sleep(WaitRaftElectDuration)
			//Debug("There is no one leader, sleep %s", WaitRaftElectDuration.String())
		}
	}
}

func (ck *Clerk) sendRPCUntilSuccess(method string, args interface{}) string {
	for {
		ck.mu.Lock()
		var ok bool
		var err Err
		var ret string
		Debug("client try to send rpc[%s] to server[%d]", method, ck.leader)
		if method == PutAppendRPC {
			reply := PutAppendReply{}
			ok = ck.servers[ck.leader].Call(PutAppendRPC, args.(*PutAppendArgs), &reply)
			err = reply.Err
			ret = ""
		} else if method == GetRPC {
			reply := GetReply{}
			ok = ck.servers[ck.leader].Call(GetRPC, args.(*GetArgs), &reply)
			err = reply.Err
			ret = reply.Value
		} else {
			FATAL("That's impossible")
		}
		if ok && err == OK {
			ck.mu.Unlock()
			return ret
		}
		if !ok || err == ErrNotLeader {
			ck.updateLeaderWithLock()
		} else {
			FATAL("unknown error [%s] %v", err, args)
		}
		// TODO: maybe need a timeout, depends on the testcase
		ck.mu.Unlock()
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Serial = nrand()
	args.ClientId = ck.clientId
	ck.writeRequestLock.Lock()
	defer ck.writeRequestLock.Unlock()
	ck.sendRPCUntilSuccess(PutAppendRPC, &args)
}

func (ck *Clerk) Put(key string, value string) {
	Debug("client receive put(%s)=%s", key, value)
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	Debug("client receive append(%s)=%s", key, value)
	ck.PutAppend(key, value, OpAppend)
}
