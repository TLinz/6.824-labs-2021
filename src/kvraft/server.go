package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	CommandId int
	Type      string // "Put" or "Append" or "Get"
	Key       string
	Value     string
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

	lastCmdIndex int
	lastCmd      Op

	bigmu     sync.Mutex
	persister *raft.Persister
}

func (kv *KVServer) applyCmd(cmd Op) {
	switch cmd.Type {
	case "Get":
	case "Put":
		kv.kv[cmd.Key] = cmd.Value
	case "Append":
		kv.kv[cmd.Key] = kv.kv[cmd.Key] + cmd.Value
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.bigmu.Lock()
	defer kv.bigmu.Unlock()

	kv.mu.Lock()
	command := Op{args.ClientId, args.CommandId, "Get", args.Key, ""}
	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintln("[k%d] cli-cmd-id:%d Get", kv.me, args.CommandId)
	kv.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)

	for !kv.killed() {
		select {
		case <-timeout:
			reply.Err = ErrWrongLeader
			return
		default:
			kv.mu.Lock()
			DPrintln("[k%d] idx:%d lastCmdIndex:%d", kv.me, idx, kv.lastCmdIndex)
			if idx == kv.lastCmdIndex {
				if command == kv.lastCmd {
					v, ok := kv.kv[command.Key]
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
				}
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.maxraftstate)
	e.Encode(kv.cmap)
	e.Encode(kv.kv)
	e.Encode(kv.lastCmdIndex)
	e.Encode(kv.lastCmd)
	snapshot := w.Bytes()
	DPrintln("[k%d] take snapshot lastCmdIndex:%d", kv.me, kv.lastCmdIndex)
	kv.rf.Snapshot(kv.lastCmdIndex, snapshot)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.bigmu.Lock()
	defer kv.bigmu.Unlock()

	kv.mu.Lock()
	// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate/2 {
	// 	kv.takeSnapshot()
	// }
	command := Op{args.ClientId, args.CommandId, args.Op, args.Key, args.Value}
	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintln("[k%d] cli-cmd-id:%d %s", kv.me, args.CommandId, args.Op)
	kv.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)

	for !kv.killed() {
		select {
		case <-timeout:
			reply.Err = ErrWrongLeader
			return
		default:
			kv.mu.Lock()
			DPrintln("[k%d] idx:%d lastCmdIndex:%d", kv.me, idx, kv.lastCmdIndex)
			if idx == kv.lastCmdIndex {
				if command == kv.lastCmd {
					reply.Err = OK
					kv.mu.Unlock()
					return
				}
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			DPrintln("[k%d] applier cmd:%d", kv.me, msg.CommandIndex)
			kv.mu.Lock()
			if cmd.CommandId <= kv.cmap[cmd.ClientId] {
				kv.lastCmdIndex = msg.CommandIndex
				kv.lastCmd = msg.Command.(Op)
				kv.mu.Unlock()
				continue
			}
			// apply the command to state machine and update cmap
			kv.applyCmd(cmd)
			kv.cmap[cmd.ClientId] = cmd.CommandId
			kv.lastCmdIndex = msg.CommandIndex
			kv.lastCmd = msg.Command.(Op)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readPersist(msg.Snapshot)
			kv.mu.Unlock()
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.cmap = make(map[int]int)
	kv.kv = make(map[string]string)

	kv.persister = persister

	go kv.applier()

	if maxraftstate != -1 {
		kv.mu.Lock()
		kv.readPersist(persister.ReadSnapshot())
		kv.mu.Unlock()

		go func() {
			for !kv.killed() {
				kv.mu.Lock()
				statesz := persister.RaftStateSize()
				if statesz > kv.maxraftstate {
					DPrintln("[k%d] raft state size overflow", kv.me)
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.maxraftstate)
					e.Encode(kv.cmap)
					e.Encode(kv.kv)
					e.Encode(kv.lastCmdIndex)
					e.Encode(kv.lastCmd)
					snapshot := w.Bytes()

					DPrintln("[k%d] take snapshot lastCmdIndex:%d", kv.me, kv.lastCmdIndex)

					kv.rf.Snapshot(kv.lastCmdIndex, snapshot)
					kv.mu.Unlock()
				} else {
					kv.mu.Unlock()
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()
	}

	return kv
}

// restore previously persisted state.
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var maxraftstate int
	var cmap map[int]int
	var kvmap map[string]string
	var lastCmdIndex int
	var lastCmd Op
	if d.Decode(&maxraftstate) != nil ||
		d.Decode(&cmap) != nil ||
		d.Decode(&kvmap) != nil ||
		d.Decode(&lastCmdIndex) != nil ||
		d.Decode(&lastCmd) != nil {
	} else {
		kv.maxraftstate = maxraftstate
		kv.cmap = cmap
		kv.kv = kvmap
		kv.lastCmdIndex = lastCmdIndex
		kv.lastCmd = lastCmd
	}
}
