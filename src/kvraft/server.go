package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cmap map[int]int       // client id to max applied command id
	kv   map[string]string // database
}

type Command struct {
	ClientId  int
	CommandId int
	Type      string // "Put" or "Append" or "Get"
	Key       string
	Value     string
}

func (kv *KVServer) applyCmd(cmd Command) {
	switch cmd.Type {
	case "Get":
	case "Put":
		kv.kv[cmd.Key] = cmd.Value
	case "Append":
		kv.kv[cmd.Key] = kv.kv[cmd.Key] + cmd.Value
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Command{args.ClientId, args.CommandId, "Get", args.Key, ""}
	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// loop to read applyCh until commandIndex == idx
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			cmd := msg.Command.(Command)
			// check if the cmd is dulplicated
			kv.mu.Lock()
			if cmd.CommandId <= kv.cmap[cmd.ClientId] {
				continue
			}
			// apply the command to state machine and update cmap
			kv.applyCmd(cmd)
			kv.cmap[cmd.ClientId] = cmd.CommandId

			if msg.CommandIndex == idx { //wrong !!!
				if cmd == command {
					v, ok := kv.kv[cmd.Key]
					if ok {
						reply.Err = OK
						reply.Value = v
						kv.mu.Unlock()
						return
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
						kv.mu.Unlock()
						return
					}
				} else {
					reply.Err = ErrWrongLeader
					return
				}
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Command{args.ClientId, args.CommandId, args.Op, args.Key, ""}
	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// loop to read applyCh until commandIndex == idx
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			cmd := msg.Command.(Command)
			// check if the cmd is dulplicated
			kv.mu.Lock()
			if cmd.CommandId <= kv.cmap[cmd.ClientId] {
				continue
			}
			// apply the command to state machine and update cmap
			kv.applyCmd(cmd)
			kv.cmap[cmd.ClientId] = cmd.CommandId

			if msg.CommandIndex == idx { // wrong !!!
				if cmd == command {
					reply.Err = OK
					return
				} else {
					reply.Err = ErrWrongLeader
					return
				}
			}
		}
	}
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
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.cmap = make(map[int]int)
	kv.kv = make(map[string]string)

	return kv
}
