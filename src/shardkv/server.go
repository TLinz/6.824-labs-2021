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

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	Migration
)

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int
	CommandId   int
	CommandType CommandType // Operation, Configuration, Migration

	// Operation
	OpType string // "Put" or "Append" or "Get"
	Key    string
	Value  string

	// Configuration
	Config shardctrler.Config

	// Migration(Push model)
	ConfigNum int // only push when the config number is the same
	Shards    []Shard
}

type ShardState uint8

const (
	Serving ShardState = iota
	Pushing
	Pulling
)

type Shard struct {
	ShardId int
	State   ShardState
	Kv      map[string]string
	Cmap    map[int]int
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
	persister    *raft.Persister

	// Your definitions here.
	dead       int32 // set by Kill()
	scck       *shardctrler.Clerk
	prevConfig shardctrler.Config
	curConfig  shardctrler.Config

	shards map[int]Shard // shardId -> Shard

	lastCmdIndex int
	lastCmd      Command

	bmu sync.Mutex
}

func (kv *ShardKV) updateShardsConfig(config shardctrler.Config) {
	// Get shards corresponding to kv.gid in config
	var shards []Shard
	for shardId, gid := range config.Shards {
		if gid == kv.gid {
			shards = append(shards, kv.shards[shardId])
		}
	}

	// Update shard state to pushing
	for shardId, shard := range kv.shards {
		// Check if the shard is in the shards slice
		found := false
		for _, shard := range shards {
			if shard.ShardId == shardId {
				found = true
				break
			}
		}

		// If the shard is not found in shards, update its state to Pushing
		if !found {
			shard.State = Pushing
		}
	}

	// Update shard state to pulling
	for shardId := range shards {
		// Check if the shard is in the shards slice
		found := false
		for _, shard := range kv.shards {
			if shard.ShardId == shardId {
				found = true
				break
			}
		}

		// make empty shard
		if !found {
			shard := Shard{}
			shard.ShardId = shardId
			shard.State = Pulling
			shard.Kv = make(map[string]string)
			shard.Cmap = make(map[int]int)
			kv.shards[shardId] = shard
		}
	}
}

func (kv *ShardKV) applyCmd(cmd Command) {
	switch cmd.CommandType {
	case Operation:
		switch cmd.OpType {
		case "Get":
		case "Put":
			shard := kv.shards[key2shard(cmd.Key)]
			shard.Kv[cmd.Key] = cmd.Value
		case "Append":
			shard := kv.shards[key2shard(cmd.Key)]
			shard.Kv[cmd.Key] += cmd.Value
		}
	case Configuration:
		if cmd.Config.Num != kv.curConfig.Num+1 {
			Debug(dClient, "K%d applyCmd wrong config num:%d", kv.me, cmd.Config.Num)
			return
		}
		kv.updateShardsConfig(cmd.Config)
		kv.prevConfig = kv.curConfig
		kv.curConfig = cmd.Config
	case Migration:
		// TODO: ...
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.bmu.Lock()
	defer kv.bmu.Unlock()

	kv.mu.Lock()
	// command := Op{args.ClientId, args.CommandId, "Get", args.Key, ""}
	command := Command{}
	command.ClientId = args.ClientId
	command.CommandId = args.CommandId
	command.CommandType = Operation
	command.OpType = "Get"
	command.Key = args.Key

	// check if the shard is in the current config
	if kv.curConfig.Shards[key2shard(args.Key)] != kv.gid {
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
				if reflect.DeepEqual(command, kv.lastCmd) {
					shard := kv.shards[key2shard(command.Key)]
					v, ok := shard.Kv[command.Key]
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
	// command := Op{args.ClientId, args.CommandId, args.OpType, args.Key, args.Value}
	command := Command{}
	command.ClientId = args.ClientId
	command.CommandId = args.CommandId
	command.CommandType = Operation
	command.OpType = args.OpType
	command.Key = args.Key
	command.Value = args.Value

	// check if the shard is in the current config
	if kv.curConfig.Shards[key2shard(args.Key)] != kv.gid {
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
	Debug(dClient, "K%d %d k:%s v:%s idx:%d", kv.me, args.OpType, args.Key, args.Value, idx)

	for !kv.killed() {
		select {
		case <-timeout:
			reply.Err = ErrWrongLeader
			Debug(dClient, "K%d %d k:%s v:%s timeout", kv.me, args.OpType, args.Key, args.Value)
			return
		default:
			kv.mu.Lock()
			if idx == kv.lastCmdIndex {
				if reflect.DeepEqual(command, kv.lastCmd) {
					reply.Err = OK
					Debug(dClient, "K%d %d k:%s v:%s success, LCI:%d", kv.me, args.OpType, args.Key, args.Value, kv.lastCmdIndex)
					kv.mu.Unlock()
					return
				}
				reply.Err = ErrWrongLeader
				Debug(dClient, "K%d %d k:%s v:%s wrong, LCI:%d", kv.me, args.OpType, args.Key, args.Value, kv.lastCmdIndex)
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) executeConfig(command Command) {
	kv.bmu.Lock()
	defer kv.bmu.Unlock()

	kv.mu.Lock()
	idx, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)
	Debug(dClient, "K%d executeConfig idx:%d", kv.me, idx)

	for !kv.killed() {
		select {
		case <-timeout:
			Debug(dClient, "K%d executeConfig timeout", kv.me)
			return
		default:
			kv.mu.Lock()
			if idx == kv.lastCmdIndex {
				if reflect.DeepEqual(command, kv.lastCmd) {
					Debug(dClient, "K%d executeConfig success, LCI:%d", kv.me, kv.lastCmdIndex)
					kv.mu.Unlock()
					return
				}
				Debug(dClient, "K%d executeConfig wrong, LCI:%d", kv.me, kv.lastCmdIndex)
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
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.shards = make(map[int]Shard)

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

		go func() {
			for !kv.killed() {
				kv.mu.Lock()
				statesz := persister.RaftStateSize()
				if statesz > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.maxraftstate)
					e.Encode(kv.shards)
					e.Encode(kv.lastCmdIndex)
					e.Encode(kv.lastCmd)
					e.Encode(kv.prevConfig)
					e.Encode(kv.curConfig)
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

func (kv *ShardKV) checkAllServing() bool {
	for _, shard := range kv.shards {
		if shard.State != Serving {
			return false
		}
	}
	return true
}

// TODO: add locks...
func (kv *ShardKV) pollShardCtrler() {
	for !kv.killed() {
		// If all shards are in 'Serving' state, then pull the next config from shardctrler
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if kv.checkAllServing() && isLeader {
			kv.mu.Unlock()
			nextConfig := kv.scck.Query(kv.curConfig.Num + 1)
			kv.mu.Lock()
			if nextConfig.Num != kv.curConfig.Num+1 {
				time.Sleep(100 * time.Millisecond)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			command := Command{}
			command.CommandType = Configuration
			command.Config = nextConfig

			kv.executeConfig(command)
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) migrateShard(shardId int) {
	// for !kv.killed() {
	// }
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Command)
			switch cmd.CommandType {
			case Operation:
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastCmdIndex {
					kv.mu.Unlock()
					continue
				}
				shard := kv.shards[key2shard(cmd.Key)]
				if cmd.CommandId <= shard.Cmap[cmd.ClientId] {
					kv.lastCmdIndex = msg.CommandIndex
					kv.lastCmd = msg.Command.(Command)
					kv.mu.Unlock()
					continue
				}
				// apply the command to state machine and update cmap
				kv.applyCmd(cmd)
				shard.Cmap[cmd.ClientId] = cmd.CommandId
				kv.lastCmdIndex = msg.CommandIndex
				kv.lastCmd = msg.Command.(Command)
				kv.mu.Unlock()

			case Configuration:
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastCmdIndex {
					kv.mu.Unlock()
					continue
				}
				if cmd.Config.Num != kv.curConfig.Num+1 {
					kv.mu.Unlock()
					continue
				}
				kv.applyCmd(cmd)
				kv.lastCmdIndex = msg.CommandIndex
				kv.lastCmd = msg.Command.(Command)
				kv.mu.Unlock()

			case Migration:
			}
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.lastCmdIndex {
				kv.mu.Unlock()
				continue
			}
			kv.readPersist(msg.Snapshot)
			kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
			kv.lastCmdIndex = msg.SnapshotIndex

			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// restore previously persisted state.
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var maxraftstate int
	var shards map[int]Shard
	var lastCmdIndex int
	var lastCmd Command
	var prevConfig shardctrler.Config
	var curConfig shardctrler.Config
	if d.Decode(&maxraftstate) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&lastCmdIndex) != nil ||
		d.Decode(&lastCmd) != nil ||
		d.Decode(&prevConfig) != nil ||
		d.Decode(&curConfig) != nil {
	} else {
		kv.maxraftstate = maxraftstate
		kv.shards = shards
		kv.lastCmdIndex = lastCmdIndex
		kv.lastCmd = lastCmd
		kv.prevConfig = prevConfig
		kv.curConfig = curConfig
	}
}
