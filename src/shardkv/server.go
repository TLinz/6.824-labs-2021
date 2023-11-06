package shardkv

import (
	"bytes"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead   int32 // set by Kill()
	scck   *shardctrler.Clerk
	config shardctrler.Config

	cmap         map[int]int       // client id to max applied command id
	kv           map[string]string // database
	lastCmdIndex int
	lastCmd      Op

	bmu       sync.Mutex
	persister *raft.Persister
}

func (kv *ShardKV) applyCmd(cmd Op) {
	switch cmd.Type {
	case "Get":
	case "Put":
		kv.kv[cmd.Key] = cmd.Value
	case "Append":
		kv.kv[cmd.Key] = kv.kv[cmd.Key] + cmd.Value
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.bmu.Lock()
	defer kv.bmu.Unlock()

	kv.mu.Lock()
	command := Op{args.ClientId, args.CommandId, "Get", args.Key, ""}

	// check if the shard is in the current config
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)
	Debug(dClient, "K%d Get k:%s", kv.me, args.Key)

	for !kv.killed() {
		select {
		case <-timeout:
			reply.Err = ErrWrongLeader
			Debug(dClient, "K%d Get k:%s timeout idx:%d", kv.me, args.Key, idx)
			return
		default:
			kv.mu.Lock()
			if idx == kv.lastCmdIndex {
				if command == kv.lastCmd {
					v, ok := kv.kv[command.Key]
					if ok {
						reply.Err = OK
						reply.Value = v
						Debug(dClient, "K%d Get k:%s success v:%s LCI:%d", kv.me, args.Key, reply.Value, kv.lastCmdIndex)
						kv.mu.Unlock()
						return
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
						Debug(dClient, "K%d Get k:%s success v:nil LCI:%d", kv.me, args.Key, kv.lastCmdIndex)
						kv.mu.Unlock()
						return
					}
				}
				reply.Err = ErrWrongLeader
				Debug(dClient, "K%d Get k:%s wrong LCI:%d", kv.me, args.Key, kv.lastCmdIndex)
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.bmu.Lock()
	defer kv.bmu.Unlock()

	kv.mu.Lock()
	command := Op{args.ClientId, args.CommandId, args.Op, args.Key, args.Value}

	// check if the shard is in the current config
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)
	Debug(dClient, "K%d %d k:%s v:%s idx:%d", kv.me, args.Op, args.Key, args.Value, idx)

	for !kv.killed() {
		select {
		case <-timeout:
			reply.Err = ErrWrongLeader
			Debug(dClient, "K%d %d k:%s v:%s timeout", kv.me, args.Op, args.Key, args.Value)
			return
		default:
			kv.mu.Lock()
			if idx == kv.lastCmdIndex {
				if command == kv.lastCmd {
					reply.Err = OK
					Debug(dClient, "K%d %d k:%s v:%s success, LCI:%d", kv.me, args.Op, args.Key, args.Value, kv.lastCmdIndex)
					kv.mu.Unlock()
					return
				}
				reply.Err = ErrWrongLeader
				Debug(dClient, "K%d %d k:%s v:%s wrong, LCI:%d", kv.me, args.Op, args.Key, args.Value, kv.lastCmdIndex)
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.cmap = make(map[int]int)
	kv.kv = make(map[string]string)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.scck = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()

	go kv.pollShardCtrler()

	if maxraftstate != -1 {
		kv.mu.Lock()
		kv.readPersist(persister.ReadSnapshot())
		kv.mu.Unlock()

		Debug(dInfo, "K%d start... LCI:%d", kv.me, kv.lastCmdIndex)

		go func() {
			for !kv.killed() {
				kv.mu.Lock()
				statesz := persister.RaftStateSize()
				if statesz > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.maxraftstate)
					e.Encode(kv.cmap)
					e.Encode(kv.kv)
					e.Encode(kv.lastCmdIndex)
					e.Encode(kv.lastCmd)
					e.Encode(kv.config)
					snapshot := w.Bytes()

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

func (kv *ShardKV) pollShardCtrler() {
	for !kv.killed() {
		config := kv.scck.Query(-1)
		kv.mu.Lock()
		if !reflect.DeepEqual(config, kv.config) {
			kv.config = config
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastCmdIndex {
				Debug(dError, "K%d tries to apply an old cmd, CI:%d LCI:%d", kv.me, msg.CommandIndex, kv.lastCmdIndex)
				kv.mu.Unlock()
				continue
			}
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
			Debug(dClient, "K%d %s k:%d v:%d applied LCI:%d", kv.me, cmd.Type, cmd.Key, cmd.Value, kv.lastCmdIndex)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.lastCmdIndex {
				Debug(dError, "K%d tries to apply an old snapshot, SI:%d LCI:%d", kv.me, msg.SnapshotIndex, kv.lastCmdIndex)
				kv.mu.Unlock()
				continue
			}
			kv.readPersist(msg.Snapshot)
			kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			kv.lastCmdIndex = msg.SnapshotIndex
			Debug(dClient, "K%d LCI:%d after cond install snapshot", kv.me, kv.lastCmdIndex)

			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// restore previously persisted state.
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var maxraftstate int
	var cmap map[int]int
	var kvmap map[string]string
	var lastCmdIndex int
	var lastCmd Op
	var config shardctrler.Config
	if d.Decode(&maxraftstate) != nil ||
		d.Decode(&cmap) != nil ||
		d.Decode(&kvmap) != nil ||
		d.Decode(&lastCmdIndex) != nil ||
		d.Decode(&lastCmd) != nil ||
		d.Decode(&config) != nil {
	} else {
		kv.maxraftstate = maxraftstate
		kv.cmap = cmap
		kv.kv = kvmap
		kv.lastCmdIndex = lastCmdIndex
		kv.lastCmd = lastCmd
		kv.config = config
	}
}
