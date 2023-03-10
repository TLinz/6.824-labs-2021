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

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

var role = []string{"F", "C", "L"}

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

	rtFlag bool
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("read persist error\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) { // TODO????: refactor the code...
	// Your code here (2A, 2B).
	rf.mu.Lock()

	// 1. Reply false if term < currentTerm (??5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("RV [%s%d] refuses to vote for [C%d]. (T1:%d \\ T2:%d)\n", role[rf.state], rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	stepAside := false
	if args.Term > rf.currentTerm {
		DPrintf("RV [%s%d] T:%d->%d.\n", role[rf.state], rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.persist()
		rf.votedFor = -1
		rf.persist()
		if rf.state == Leader || rf.state == Candidate {
			stepAside = true
			DPrintf("RV [%s%d] becomes follower before voting(or not) for [C%d]. (T1:%d \\ T2:%d)\n", role[rf.state], rf.me, args.CandidateId, rf.currentTerm, args.Term)
		}
		rf.state = Follower
	}
	reply.Term = rf.currentTerm

	isUpToDate := true
	if len(rf.log) != 0 {
		lastLog := rf.log[len(rf.log)-1]
		isUpToDate = args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= len(rf.log)-1)
	}

	// 2. If votedFor is null or candidateId, and candidate???s log is at
	// least as up-to-date as receiver???s log, grant vote (??5.2, ??5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		DPrintf("RV [%s%d] votes for [C%d]. (T1:%d T2:%d)\n", role[rf.state], rf.me, args.CandidateId, rf.currentTerm, args.Term)

		rf.rtFlag = true
		rf.mu.Unlock()
		if len(rf.rtCh) == 0 {
			rf.rtCh <- 1
		}
	} else {
		reply.VoteGranted = false
		DPrintf("RV [%s%d] refuses to vote for [C%d]. (T1:%d VF:%d UTD:%v \\ T2:%d)\n", role[rf.state], rf.me, args.CandidateId, rf.currentTerm, rf.votedFor, isUpToDate, args.Term)

		if stepAside {
			rf.rtFlag = true
			rf.mu.Unlock()
			if len(rf.rtCh) == 0 {
				rf.rtCh <- 1
			}
		} else {
			rf.mu.Unlock()
		}
	}
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

// ?????? When calling sendRequestVote, MUST NOT hold the lock.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// For fast log backtracking.
	ConflictIndex int
	ConflictTerm  int
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Return the index of first log whose entry has corresponding term.
func firstLog(logs []logEntry, term int) int {
	for i := range logs {
		if logs[i].Term == term {
			return i
		}
	}
	return -1
}

// Return the index of last log whose entry has corresponding term.
func lastLog(logs []logEntry, term int) int {
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i].Term == term {
			return i
		}
	}
	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// Reply false if term < currentTerm (??5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("AE [%s%d] replies false to [L%d]. (T1:%d \\ T2:%d)\n", role[rf.state], rf.me, args.LeaderId, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("AE [%s%d] T:%d->%d.\n", role[rf.state], rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	reply.Term = rf.currentTerm

	// Reply false if log doesn???t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (??5.3)
	if args.PrevLogIndex != -1 && (args.PrevLogIndex >= len(rf.log) || (rf.log[args.PrevLogIndex]).Term != args.PrevLogTerm) {
		reply.Success = false
		DPrintf("AE [%s%d] replies false to [L%d] (Consistency check failure). (T1:%d \\ T2:%d)\n", role[rf.state], rf.me, args.LeaderId, rf.currentTerm, args.Term)

		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = (rf.log[args.PrevLogIndex]).Term
			reply.ConflictIndex = firstLog(rf.log, reply.ConflictTerm)
		}
	} else {
		reply.Success = true
		DPrintf("AE [%s%d] replies true to [L%d]. (T1:%d \\ T2:%d)\n", role[rf.state], rf.me, args.LeaderId, rf.currentTerm, args.Term)
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (??5.3)
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
			rf.persist()
		} else {
			if idx == len(rf.log) {
				// Append any new entries not already in the log
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				rf.persist()
			}
		}

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			DPrintf("AE [%s%d] commitIndex -> %d.\n", role[rf.state], rf.me, rf.commitIndex)
		}
	}

	// Must not hold the lock when sending data to rf.rtCh,
	// otherwise it may cause the current AppendEntries handler and ticker to get stuck.
	rf.rtFlag = true
	rf.mu.Unlock()
	if len(rf.rtCh) == 0 {
		rf.rtCh <- 1
	}
}

// ?????? When calling sendAppendEntries, MUST NOT hold the lock.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
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

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index := len(rf.log) + 1
	term := rf.currentTerm

	// Your code here (2B).
	log := logEntry{command, rf.currentTerm}
	rf.log = append(rf.log, log)
	rf.persist()
	DPrintf("[%s%d] appends new log at %d.\n", role[rf.state], rf.me, index-1)

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

func (rf *Raft) election(args *RequestVoteArgs) {
	// Send RequestVote RPCs to all other servers.
	// When election() being called, lock is being held.
	rf.mu.Lock()
	voteSum := 0
	isFinished := false
	DPrintf("[%s%d] starts election...\n", role[rf.state], rf.me)
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		reply := &RequestVoteReply{}

		idx := i
		// Send RequestVote RPCs concurrently.
		go func(int) {
			rf.mu.Lock()
			DPrintf("[%s%d] sends RequestVote to [N%d]...", role[rf.state], rf.me, idx)
			rf.mu.Unlock()

			ok := rf.sendRequestVote(idx, args, reply)
			rf.mu.Lock()
			if ok && rf.state == Candidate && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					DPrintf("[C%d]'s election failed, becomming follower. (T1:%d \\ RNO: %d T2:%d)\n", rf.me, rf.currentTerm, idx, reply.Term)
					rf.currentTerm = reply.Term
					rf.persist()
					rf.state = Follower

					if !isFinished {
						isFinished = true
						rf.mu.Unlock()
						if len(rf.rtCh) == 0 {
							rf.rtCh <- 1
						}
						rf.mu.Lock()
					}
				}

				if reply.VoteGranted {
					voteSum++
					if (voteSum+1)*2 > len(rf.peers) {
						rf.state = Leader
						DPrintf("[C%d] becomes the new leader!\n", rf.me)

						for i := range rf.matchIndex {
							rf.matchIndex[i] = -1
						}

						for i := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
						}

						if !isFinished {
							isFinished = true
							rf.mu.Unlock()
							if len(rf.rtCh) == 0 {
								rf.rtCh <- 1
							}
							rf.mu.Lock()
						}
					}
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
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

		interval := 500 + rand.Intn(250) // Need to be big enough to pass 2c's unreliable test.

		rf.mu.Lock() // May stuck here if send data to rf.rtCh or ch(election) while holding the lock.
		// DPrintf("[%s%d] reset timer with interval %dms.\n", role[rf.state], rf.me, interval)

		if rf.rtFlag {
			DPrintf("Warning: atomic 0!\n")
			<-rf.rtCh
			rf.rtFlag = false
		}

		if rf.state == Follower {
			rf.mu.Unlock()
			select {
			case <-rf.rtCh:
				rf.mu.Lock()
				rf.rtFlag = false
				rf.mu.Unlock()
			case <-time.After(time.Duration(interval) * time.Millisecond):
				// Timeout, start election...
				rf.mu.Lock()
				if rf.rtFlag {
					DPrintf("Warning: atomic 1!\n")
					<-rf.rtCh
					rf.rtFlag = false
				} else {
					rf.currentTerm++
					rf.state = Candidate
					rf.votedFor = rf.me
					rf.persist()
				}
				rf.mu.Unlock()
			}

		} else if rf.state == Candidate {
			args := &RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) - 1
			if args.LastLogIndex == -1 {
				args.LastLogTerm = -1
			} else {
				args.LastLogTerm = rf.log[args.LastLogIndex].Term
			}
			rf.mu.Unlock()

			go rf.election(args)

			select {
			case <-time.After(time.Duration(interval) * time.Millisecond):
				rf.mu.Lock()
				if rf.rtFlag {
					DPrintf("Warning: atomic 2!\n")
					<-rf.rtCh
					rf.rtFlag = false
				} else if rf.state == Candidate {
					rf.currentTerm++
				}
				rf.mu.Unlock()
			case <-rf.rtCh:
				rf.mu.Lock()
				rf.rtFlag = false
				rf.mu.Unlock()
			}

		} else {
			rf.mu.Unlock()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				// Set up goroutines for each follower to deal with heartbeat or log replication.
				idx := i
				go func(int) {
					for {
						rf.mu.Lock()

						if rf.killed() {
							if rf.state == Leader {
								rf.state = Follower // TOFIX: should not become follower when being killed...
								rf.rtFlag = true
								rf.mu.Unlock()
								if len(rf.rtCh) == 0 {
									rf.rtCh <- 1
								}
							} else {
								rf.mu.Unlock()
							}
							break
						}

						// Sending heartbeat periodically.
						if rf.state != Leader {
							rf.mu.Unlock()
							break
						}

						args := &AppendEntriesArgs{}
						reply := &AppendEntriesReply{}

						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.PrevLogIndex = rf.nextIndex[idx] - 1
						if args.PrevLogIndex == -1 {
							args.PrevLogTerm = -1
						} else {
							args.PrevLogTerm = (rf.log[rf.nextIndex[idx]-1]).Term
						}
						args.LeaderCommit = rf.commitIndex

						originalLogs := make([]logEntry, len(rf.log))
						copy(originalLogs, rf.log)

						if rf.nextIndex[idx] != len(rf.log) {
							// args.Entries = rf.log[rf.nextIndex[idx]:len(rf.log)] // ??? There may be a subtle error in the assignment here, which may cause a race condition later.
							args.Entries = make([]logEntry, len(rf.log[rf.nextIndex[idx]:len(rf.log)]))
							copy(args.Entries, rf.log[rf.nextIndex[idx]:len(rf.log)])
						}

						DPrintf("[%s%d] sends AppendEntries to [N%d]...\n", role[rf.state], rf.me, idx)
						rf.mu.Unlock()

						go func(int) {
							ok := rf.sendAppendEntries(idx, args, reply)
							rf.mu.Lock()
							if ok && rf.state == Leader && rf.currentTerm == args.Term {

								if reply.Term > rf.currentTerm {
									DPrintf("[L%d] steps aside. (T1:%d \\ RNO: %d T2:%d)\n", rf.me, rf.currentTerm, idx, reply.Term)
									rf.currentTerm = reply.Term
									rf.persist()
									rf.state = Follower
									rf.rtFlag = true
									rf.mu.Unlock()
									if len(rf.rtCh) == 0 {
										rf.rtCh <- 1
									}
									return
								}

								if reply.Success {
									rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
									rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1

									// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ??? N,
									// and log[N].term == currentTerm: set commitIndex = N (??5.3, ??5.4).
									cp := make([]int, len(rf.matchIndex))
									copy(cp, rf.matchIndex)
									sort.Ints(cp)

									N := cp[(len(cp)-1)/2]

									if N > rf.commitIndex && (rf.log[N]).Term == rf.currentTerm {
										DPrintf("[L%d] commitIndex -> %d.\n", rf.me, rf.commitIndex)
										rf.commitIndex = N
										rf.mu.Unlock()
									} else {
										rf.mu.Unlock()
									}
								} else {
									// rf.nextIndex[idx]-- // ?????????
									// rf.nextIndex[idx] = args.PrevLogIndex
									if reply.ConflictTerm != -1 && lastLog(originalLogs, reply.ConflictTerm) != -1 {
										rf.nextIndex[idx] = lastLog(originalLogs, reply.ConflictTerm) + 1
									} else {
										rf.nextIndex[idx] = reply.ConflictIndex
									}
									rf.mu.Unlock()
								}
							} else {
								rf.mu.Unlock()
							}
						}(idx)

						time.Sleep(105 * time.Millisecond) // Need to be small enough to pass 2c's unreliable test.
					}
				}(idx)
			}

			<-rf.rtCh
			rf.mu.Lock()
			rf.rtFlag = false
			rf.mu.Unlock()

		}
	}
}

func (rf *Raft) applier() {
	prevCommitIdx := -1
	for !rf.killed() {
		rf.mu.Lock()
		cIdx := rf.commitIndex
		rf.mu.Unlock()
		if prevCommitIdx != cIdx {
			for i := prevCommitIdx + 1; i <= cIdx; i++ {
				rf.mu.Lock()
				logCommited := rf.log[i]
				rf.mu.Unlock()
				command := logCommited.Command
				index := i
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index + 1}
				rf.mu.Lock()
				DPrintf("[%s%d] commits log[%d]\n", role[rf.state], rf.me, i)
				rf.mu.Unlock()
			}
			prevCommitIdx = cIdx
		}

		time.Sleep(100 * time.Millisecond)
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
	go rf.applier()

	return rf
}

// TOTHINK????:

// 1. Leader should not change its state to follower after being killed, but if so, it also seems ok.

// 2. Maybe I can use condition viarable in applier rather than commiting periodically.
