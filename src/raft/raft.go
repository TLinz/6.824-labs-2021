package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
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

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type logEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state   State
	rtCh    chan int // Reset timer channel
	applyCh chan ApplyMsg

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []logEntry // Index starts at 1

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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
	Term        int
	CandidateId int

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		DPrintf("N%d's cur term %d > C%d's term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

		rf.mu.Unlock()
		return
	}

	if len(rf.log) != 0 { // ❌❌❌ 如果leader收到了一个任期更大但日志更旧的requestvote请求，leader此时不会退位成candidate，这时严重错误！！！
		lastLogEntry := rf.log[len(rf.log)-1]
		if lastLogEntry.Term > args.LastLogTerm ||
			(lastLogEntry.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex) {
			DPrintf("[C%d]'s last log not up to date\n", args.CandidateId)

			// 忘了更新term，草泥马戈壁❌❌❌
			// 在退位的同时要清空votedFor，虽然后面VoteGranted为false
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				prevState := rf.state
				rf.state = Follower
				if prevState == Leader || prevState == Candidate {
					rf.mu.Unlock()
					rf.rtCh <- 1 //rf.rtCh在输入变量时一定要解锁，具体可以看ticker中的解释。
					rf.mu.Lock()
				}
				rf.votedFor = -1
			}

			reply.Term = rf.currentTerm
			reply.VoteGranted = false

			DPrintf("N%d election safety: ct:%d t:%d cli:%d li:%d\n", rf.me, args.LastLogTerm, lastLogEntry.Term, args.LastLogIndex, len(rf.log)-1)

			rf.mu.Unlock()
			return
		}
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.mu.Unlock()

			// Reset timer
			rf.rtCh <- 1
			return
		}

		DPrintf("N%d's cur term %d = C%d's term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.state = Follower

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.mu.Unlock()

	// Reset timer
	rf.rtCh <- 1
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []logEntry // 发送leader日志列表中nextIndex[i]（i为对应follower的id）索引位置及之后的所有日志项
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower

	rf.mu.Unlock()
	// Reset timer
	rf.rtCh <- 1 // 一定不能持有锁，否则就可能无限循环
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex != -1 && (args.PrevLogIndex >= len(rf.log) || (rf.log[args.PrevLogIndex]).Term != args.PrevLogTerm) {
		reply.Success = false
		rf.mu.Unlock()
	} else {
		reply.Success = true
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)
		idx := args.PrevLogIndex + 1
		isConflict := false
		endIdx := -1
		if len(rf.log) > args.PrevLogIndex+1 {
			for i := range args.Entries {
				if idx == len(rf.log) {
					break
				}

				if args.Entries[i] == rf.log[idx] {
					idx++
				} else {
					isConflict = true
					endIdx = i
					break
				}
			}
		}

		if isConflict {
			rf.log = rf.log[:idx]
			// Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[endIdx:]...)
		} else {
			if idx == len(rf.log) {
				// Append any new entries not already in the log
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
			}
		}

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			idx := rf.commitIndex + 1

			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)

			// Send ApplyMsg
			// logCommited := rf.log[rf.commitIndex]
			// command := logCommited.Command
			// index := rf.commitIndex
			// DPrintf("[N%d]: commit log[%d]\n", rf.me, rf.commitIndex)
			// rf.mu.Unlock()

			for i := idx; i <= rf.commitIndex; i++ {
				logCommited := rf.log[i]
				command := logCommited.Command
				index := i
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1}
				DPrintf("[N%d]: commit log[%d]\n", rf.me, i)
			}
			rf.mu.Unlock()

			//rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1} //可能有跳过的情况
		} else {
			rf.mu.Unlock()
		}
	}

	// // Reset timer
	// rf.rtCh <- 1
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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock() // Maybe I can use only this instead of using later two.

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index := len(rf.log) + 1
	term := rf.currentTerm

	// Your code here (2B).
	log := logEntry{command, rf.currentTerm}
	rf.log = append(rf.log, log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

	rf.mu.Unlock()
	return index, term, true
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

func (rf *Raft) election(ch chan int) {
	// Send RequestVote RPCs to all other servers.
	voteSum := 0
	isFinished := false
	DPrintf("[N%d] starts election...\n", rf.me)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{}
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log) - 1
		if args.LastLogIndex == -1 {
			args.LastLogTerm = -1
		} else {
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
		}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}

		idx := i

		// Send RequestVote RPCs concurrently.
		go func(int) {
			rf.sendRequestVote(idx, args, reply) //还没有像心跳那样处理丢包情况！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！

			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower

				if !isFinished {
					isFinished = true
					rf.mu.Unlock()
					ch <- 1 //也可能导致后续ticker的卡死
					rf.mu.Lock()
				}
			}

			// if reply.VoteGranted {
			// 选举超时的情况下candidate的任期自增后有可能收到过期的reply，也就是说收到的reply不是本次选举的reply，此时应当忽略。
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				voteSum++
				if (voteSum+1)*2 > len(rf.peers) {
					if rf.state == Candidate {
						rf.state = Leader
						DPrintf("New leader [L%d]!\n", rf.me)

						for i := range rf.matchIndex {
							rf.matchIndex[i] = -1
						}

						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}

						if !isFinished {
							isFinished = true
							rf.mu.Unlock()
							ch <- 1
							rf.mu.Lock()
						}
					}
				}
			}
			rf.mu.Unlock()
		}(idx)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		interval := 200 + rand.Intn(150)

		rf.mu.Lock() // 若rtch输入时持有加锁那绑定ticker很有可能永远卡死在这里从而永久进入僵死状态。

		if rf.state == Follower {
			rf.mu.Unlock()
			select {
			case <-rf.rtCh:
			case <-time.After(time.Duration(interval) * time.Millisecond):
				// Start election...
				rf.mu.Lock()
				rf.currentTerm++
				rf.state = Candidate
				rf.votedFor = rf.me
				rf.mu.Unlock()
			}
		} else if rf.state == Candidate {
			rf.mu.Unlock()
			finishCh := make(chan int)

			go rf.election(finishCh)

			select {
			case <-time.After(time.Duration(interval) * time.Millisecond):
				rf.mu.Lock()
				if rf.state == Candidate {
					rf.currentTerm++
				}
				rf.mu.Unlock()
			case <-finishCh:
			case <-rf.rtCh:
			}
		} else {
			rf.mu.Unlock()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				// 为其余followers分别开辟独立goroutine处理心跳或重发
				idx := i
				go func(int) {
					for {
						rf.mu.Lock()

						// 及时获取该节点是否被杀掉的命令，从而能够退出，否则将永不退出
						// if rf.killed() && rf.state == Leader {
						// 	//DPrintf("[N%d]: dead...\n", rf.me)
						// 	rf.state = Follower
						// 	rf.rtCh <- 1
						// 	break
						// }

						if rf.killed() {
							//DPrintf("[N%d]: dead...\n", rf.me)
							if rf.state == Leader {
								rf.state = Follower
								rf.rtCh <- 1
							}
							break
						}

						// 若该follower的日志没有完全和当前leader一致，则需向该follower发送日志项
						if rf.nextIndex[idx] != len(rf.log) {
							if rf.state != Leader {
								//DPrintf("[N%d] is no longer the leader...\n", rf.me)
								rf.mu.Unlock()
								break
							}

							args := &AppendEntriesArgs{}
							reply := &AppendEntriesReply{}

							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.PrevLogIndex = rf.nextIndex[idx] - 1 // 注意-1的边界情况
							if args.PrevLogIndex == -1 {
								args.PrevLogTerm = -1
							} else {
								args.PrevLogTerm = (rf.log[rf.nextIndex[idx]-1]).Term
							}
							args.Entries = rf.log[rf.nextIndex[idx]:len(rf.log)] // rf.log此时可能会更新和初始值不一样，因为过程中可能有多个Start()
							args.LeaderCommit = rf.commitIndex
							rf.mu.Unlock()

							DPrintf("[L%d] is sending log entries to [N%d]...\n", rf.me, idx)
							ok := rf.sendAppendEntries(idx, args, reply) // 如果丢包的话会不会一直阻塞在这里导致整个当前goroutine卡死呢？
							//DPrintf("[L%d]: recv N%d's reply...\n", rf.me, idx)
							if ok {
								// ok返回的时间会不会对系统有影响？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？多思考！！！！！！！！！！！！！！！！！！！！
								// 实际上如果ok为false，那相较于普通的heartbeat往返流程，时间会变的非常长，因此一旦发生丢包，
								// 那么重发会经过这段非常长的时间，因此这一过程中就会有其他节点开始选举当选leader。
								// 这是很不合理的，因为一旦丢包leader就要变更这绝对是个非常愚蠢的逻辑。
								rf.mu.Lock() // 一定要放在sendAppendEntries之后，否则死锁

								if rf.state != Leader {
									//DPrintf("[N%d] is no longer the leader...\n", rf.me)
									rf.mu.Unlock()
									break
								}

								if reply.Term > rf.currentTerm && rf.state == Leader {
									DPrintf("?\n")
									rf.currentTerm = reply.Term
									rf.state = Follower
									rf.mu.Unlock()
									rf.rtCh <- 1
									break
								}

								// 根据响应更新follower的nextIndex和matchIndex以及leader的commitIndex
								if reply.Success {
									DPrintf("[L%d]: recv N%d's success reply.\n", rf.me, idx)
									rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
									rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1

									// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
									// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
									cp := make([]int, len(rf.matchIndex))
									copy(cp, rf.matchIndex)
									sort.Ints(cp)
									//DPrintf("[L%d]: matchIndex: %v, nextIndex: %v\n", rf.me, rf.matchIndex, rf.nextIndex)

									N := cp[len(cp)/2] //当总数为偶数的时候❌

									// TODO: 没有任期的判断逻辑...
									if N > rf.commitIndex && (rf.log[N]).Term == rf.currentTerm {
										idx := rf.commitIndex + 1

										rf.commitIndex = N

										for i := idx; i <= rf.commitIndex; i++ {
											logCommited := rf.log[i]
											command := logCommited.Command
											index := i
											rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1}
											DPrintf("[L%d]: commit log[%d]\n", rf.me, i)
										}
										rf.mu.Unlock()

										// TODO: 发送ApplyMsg...
										// logCommited := rf.log[rf.commitIndex]
										// command := logCommited.Command
										// index := rf.commitIndex
										// DPrintf("[L%d]: commit log[%d]\n", rf.me, rf.commitIndex)
										// rf.mu.Unlock()

										// rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1}
									} else {
										//DPrintf("WARNING: N < [N%d]'s commitIndex.\n", rf.me)
										rf.mu.Unlock()
									}
								} else {
									//DPrintf("[L%d]: recv N%d's fail reply.\n", rf.me, idx)
									rf.nextIndex[idx]--
									rf.mu.Unlock()
								}
							} else {
								rf.mu.Lock()
								DPrintf("AppendEntries RPC from L%d to N%d lost...\n", rf.me, idx)
								rf.mu.Unlock()
							}

							// TODO: 丢包重发机制待实现...跟心跳差不多用ok判断？

						} else {
							// 应该有很大问题，应该将心跳视为普通AppendEntries，处理上不应该有差别
							if rf.state != Leader {
								//DPrintf("[N%d] is no longer the leader...\n", rf.me)
								rf.mu.Unlock()
								break
							}
							// 周期性发送心跳包
							args := &AppendEntriesArgs{}
							reply := &AppendEntriesReply{}

							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.PrevLogIndex = rf.nextIndex[idx] - 1 // 注意-1的边界情况
							if args.PrevLogIndex == -1 {
								args.PrevLogTerm = -1
							} else {
								args.PrevLogTerm = (rf.log[rf.nextIndex[idx]-1]).Term
							}
							// args.Entries = rf.log[rf.nextIndex[idx] : len(rf.log)-1] // rf.log此时可能会更新和初始值不一样，因为过程中可能有多个Start()
							args.LeaderCommit = rf.commitIndex

							DPrintf("[L%d] is sending heartbeat to [N%d]...\n", rf.me, idx)
							rf.mu.Unlock()

							go func(int) {
								ok := rf.sendAppendEntries(idx, args, reply)
								if ok {
									rf.mu.Lock() // 加锁一定要在收到响应之后，否则如果丢包将卡死
									if rf.state != Leader {
										//DPrintf("[N%d] is no longer the leader...\n", rf.me)
										rf.mu.Unlock()
										return
									}

									// TODO: 处理响应
									if reply.Term > rf.currentTerm {
										rf.currentTerm = reply.Term
										rf.state = Follower
										rf.rtCh <- 1
										rf.mu.Unlock()
										return
									}

									//rf.mu.Unlock()
									// 根据响应更新follower的nextIndex和matchIndex以及leader的commitIndex
									if reply.Success {
										DPrintf("[L%d]: recv N%d's success reply.\n", rf.me, idx)
										rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
										rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1

										// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
										// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
										cp := make([]int, len(rf.matchIndex))
										copy(cp, rf.matchIndex)
										sort.Ints(cp)
										//DPrintf("[L%d]: matchIndex: %v, nextIndex: %v\n", rf.me, rf.matchIndex, rf.nextIndex)

										N := cp[len(cp)/2]

										// TODO: 没有任期的判断逻辑...
										if N > rf.commitIndex && (rf.log[N]).Term == rf.currentTerm {
											idx := rf.commitIndex + 1

											rf.commitIndex = N

											for i := idx; i <= rf.commitIndex; i++ {
												logCommited := rf.log[i]
												command := logCommited.Command
												index := i
												rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1}
												DPrintf("[L%d]: commit log[%d]\n", rf.me, i)
											}
											rf.mu.Unlock()

											// TODO: 发送ApplyMsg...
											// logCommited := rf.log[rf.commitIndex]
											// command := logCommited.Command
											// index := rf.commitIndex
											// DPrintf("[L%d]: commit log[%d]\n", rf.me, rf.commitIndex)
											// rf.mu.Unlock()

											// rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1}
										} else {
											//DPrintf("WARNING: N < [N%d]'s commitIndex.\n", rf.me)
											rf.mu.Unlock()
										}
									} else {
										//DPrintf("[L%d]: recv N%d's fail hb reply.\n", rf.me, idx)
										rf.nextIndex[idx]-- //没有考虑丢包问题，可能是reply的默认值，可以用ok进行进一步判断！
										rf.mu.Unlock()
									}
								} else {
									rf.mu.Lock()
									DPrintf("[L%d]: hb package to [N%d] loss...\n", rf.me, idx)
									rf.mu.Unlock()
								}
							}(idx)

							time.Sleep(150 * time.Millisecond)
						}
					}
				}(idx)
			}

			<-rf.rtCh // 可能会一直卡死在这里永远无法获取killed()的状态

		}
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

	rf.state = Follower
	rf.rtCh = make(chan int) // Remember initialization of channel.
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// TODO:
// 1. 节点数量为偶数时commit的错误逻辑没有修复

// 2. 重发只依赖ok的返回是不是太长了

// 3. "If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate: convert to candidate"
// 中current leader需要判断吗

// 4. 之前运行的过程中检测到有race condition需要修复
