package shardctrler

import (
	"math"
	"reflect"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killed       bool
	lastCmdIndex int
	lastCmd      Op
	cmap         map[int]int // client id to max applied command id
	bigmu        sync.Mutex

	configs []Config // indexed by config num
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	CommandId int

	Type string // "Join", "Leave", "Move", "Query"

	// Join args
	Servers map[int][]string

	// Leave args
	GIDs []int

	// Move args
	Shard int
	GID   int

	// Query args
	Num int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.bigmu.Lock()
	defer sc.bigmu.Unlock()

	sc.mu.Lock()
	command := Op{}
	command.ClientId = args.ClientId
	command.CommandId = args.CommandId
	command.Type = "Join"
	command.Servers = args.Servers

	idx, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)

	for {
		select {
		case <-timeout:
			reply.WrongLeader = true
			return
		default:
			sc.mu.Lock()
			if idx == sc.lastCmdIndex {
				if reflect.DeepEqual(command, sc.lastCmd) {
					reply.WrongLeader = false
					sc.mu.Unlock()
					return
				}
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.bigmu.Lock()
	defer sc.bigmu.Unlock()

	sc.mu.Lock()
	command := Op{}
	command.ClientId = args.ClientId
	command.CommandId = args.CommandId
	command.Type = "Leave"
	command.GIDs = args.GIDs

	idx, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)

	// for !sc.killed {
	for {
		select {
		case <-timeout:
			reply.WrongLeader = true
			return
		default:
			sc.mu.Lock()
			if idx == sc.lastCmdIndex {
				if reflect.DeepEqual(command, sc.lastCmd) {
					reply.WrongLeader = false
					sc.mu.Unlock()
					return
				}
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
	// }
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.bigmu.Lock()
	defer sc.bigmu.Unlock()

	sc.mu.Lock()
	command := Op{}
	command.ClientId = args.ClientId
	command.CommandId = args.CommandId
	command.Type = "Move"
	command.Shard = args.Shard
	command.GID = args.GID

	idx, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)

	// for !sc.killed {
	for {
		select {
		case <-timeout:
			reply.WrongLeader = true
			return
		default:
			sc.mu.Lock()
			if idx == sc.lastCmdIndex {
				if reflect.DeepEqual(command, sc.lastCmd) {
					reply.WrongLeader = false
					sc.mu.Unlock()
					return
				}
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
	// }
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.bigmu.Lock()
	defer sc.bigmu.Unlock()

	sc.mu.Lock()
	command := Op{}
	command.ClientId = args.ClientId
	command.CommandId = args.CommandId
	command.Type = "Query"
	command.Num = args.Num

	idx, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)

	// for !sc.killed {
	for {
		select {
		case <-timeout:
			reply.WrongLeader = true
			return
		default:
			sc.mu.Lock()
			if idx == sc.lastCmdIndex {
				if reflect.DeepEqual(command, sc.lastCmd) {
					reply.WrongLeader = false
					reply.Config = sc.doQuery(command.Num)
					sc.mu.Unlock()
					return
				}
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
			sc.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
	// }
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	// TODO: May introduce race condition...
	// sc.mu.Lock()
	// sc.killed = true
	// sc.mu.Unlock()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.cmap = make(map[int]int)

	// Your code here.
	go sc.applier()

	return sc
}

func (sc *ShardCtrler) applyCmd(cmd Op) {
	switch cmd.Type {
	case "Join":
		sc.doJoin(cmd.Servers)
	case "Leave":
		sc.doLeave(cmd.GIDs)
	case "Move":
		sc.doMove(cmd.Shard, cmd.GID)
	case "Query":
		// sc.doQuery(cmd.Num)
	}
}

func (sc *ShardCtrler) applier() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastCmdIndex {
				sc.mu.Unlock()
				continue
			}
			if cmd.CommandId <= sc.cmap[cmd.ClientId] {
				sc.lastCmdIndex = msg.CommandIndex
				sc.lastCmd = msg.Command.(Op)
				sc.mu.Unlock()
				continue
			}
			// apply the command to state machine and update cmap
			sc.applyCmd(cmd)
			sc.cmap[cmd.ClientId] = cmd.CommandId
			sc.lastCmdIndex = msg.CommandIndex
			sc.lastCmd = msg.Command.(Op)
			sc.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func deepCopyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for key, servers := range groups {
		newSlice := make([]string, len(servers))
		copy(newSlice, servers)
		newGroups[key] = newSlice
	}
	return newGroups
}

func (sc *ShardCtrler) doQuery(num int) Config {
	length := len(sc.configs)
	config := Config{}
	if num == -1 || num >= length {
		config = sc.configs[length-1]
	} else {
		config = sc.configs[num]
	}

	newGroups := deepCopyGroups(config.Groups)
	newConfig := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: newGroups,
	}

	return newConfig
}

func firstNonZeroKey(inputMap map[int][]int) int {
	for key := range inputMap {
		if key != 0 {
			return key
		}
	}
	return -1
}

func (sc *ShardCtrler) doJoin(gsmap map[int][]string) {
	curConfig := sc.configs[len(sc.configs)-1]
	newGroups := deepCopyGroups(curConfig.Groups)

	// 'servers' represents the addition of new groups or nodes to existing groups.
	for gid, servers := range gsmap {
		newGroups[gid] = servers
		// TODO: What if servers is less than current newGroups[gid]? error?
	}

	newConfig := Config{
		Num:    len(sc.configs),
		Shards: curConfig.Shards,
		Groups: newGroups,
	}

	group2Shards := make(map[int][]int)
	for gid := range newGroups {
		group2Shards[gid] = []int{}
		if gid == 0 {
			panic("Join gids cannot contain 0")
		}
	}
	for shard, gid := range newConfig.Shards {
		finalGid := gid
		if gid == 0 {
			finalGid = firstNonZeroKey(group2Shards)
			if finalGid == -1 {
				panic("No gid available")
			}
			newConfig.Shards[shard] = finalGid
		}
		group2Shards[finalGid] = append(group2Shards[finalGid], shard)
	}

	// Rebalance
	for {
		src, dst := getGIDMaxShards(group2Shards), getGIDMinShards(group2Shards)
		if src != 0 && len(group2Shards[src])-len(group2Shards[dst]) <= 1 {
			break
		}
		shard := group2Shards[src][0]
		group2Shards[src] = group2Shards[src][1:]
		group2Shards[dst] = append(group2Shards[dst], shard)
	}

	// Build newConfig.Shards
	for gid, shards := range group2Shards {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) doLeave(GIDs []int) {
	curConfig := sc.configs[len(sc.configs)-1]
	newGroups := deepCopyGroups(curConfig.Groups)
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: curConfig.Shards,
		Groups: newGroups,
	}

	group2Shards := make(map[int][]int)
	for gid := range newGroups {
		group2Shards[gid] = []int{}
		if gid == 0 {
			panic("Join gids cannot contain 0")
		}
	}
	for shard, gid := range newConfig.Shards {
		finalGid := gid
		if gid == 0 {
			finalGid = firstNonZeroKey(group2Shards)
			if finalGid == -1 {
				panic("No gid available")
			}
			newConfig.Shards[shard] = finalGid
		}
		group2Shards[finalGid] = append(group2Shards[finalGid], shard)
	}

	orphanShards := make([]int, 0)
	for _, gid := range GIDs {
		delete(newConfig.Groups, gid)
		if shards, ok := group2Shards[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(group2Shards, gid)
		}
	}

	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			dst := getGIDMinShards(group2Shards)
			group2Shards[dst] = append(group2Shards[dst], shard)
		}
		for gid, shards := range group2Shards {
			for _, shard := range shards {
				newConfig.Shards[shard] = gid
			}
		}
	} else {
		newConfig.Shards = [NShards]int{}
	}

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) doMove(Shard, GID int) {
	curConfig := sc.configs[len(sc.configs)-1]
	newGroups := deepCopyGroups(curConfig.Groups)
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: curConfig.Shards,
		Groups: newGroups,
	}

	newConfig.Shards[Shard] = GID
	sc.configs = append(sc.configs, newConfig)
}

func getGIDMaxShards(group2Shards map[int][]int) int {
	maxGID := 0
	maxShards := -1
	for gid, shards := range group2Shards {
		if gid == 0 {
			continue
		}
		if len(shards) > maxShards {
			maxGID = gid
			maxShards = len(shards)
		}
	}
	return maxGID
}

func getGIDMinShards(group2Shards map[int][]int) int {
	minGID := 0
	minShards := math.MaxInt
	for gid, shards := range group2Shards {
		if gid == 0 {
			continue
		}
		if len(shards) < minShards {
			minGID = gid
			minShards = len(shards)
		}
	}
	return minGID
}
