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
	"bytes"
	"math/rand"
	"reflect"
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

type logEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state   State
	rtCh    chan int // Reset timer channel
	applyCh chan ApplyMsg

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []logEntry // Index starts at 1

	// Volatile state on all servers.
	commitIndex      int
	lastAppliedIndex int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	rtFlag bool

	// Persistent state on all servers (2D)
	lastIncludedIndex int
	lastIncludedTerm  int

	hasReq   bool
	hasReqCh chan int
}

func (rf *Raft) getLogEntry(index int) *logEntry {
	for i := range rf.log {
		if rf.log[i].Index == index {
			return &rf.log[i]
		}
	}

	// little trick, pretending to have a dummy log entry.
	if index == rf.lastIncludedIndex {
		return &logEntry{Term: rf.lastIncludedTerm, Index: rf.lastIncludedIndex}
	}
	Debug(dError, "S%d tries to get log entry at %d, LII:%d, LLI:%d",
		rf.me, index, rf.lastIncludedIndex, rf.getLastLogIndex())
	return nil
}

func (rf *Raft) getLogRealIndex(index int) int {
	for i := range rf.log {
		if rf.log[i].Index == index {
			return i
		}
	}
	Debug(dError, "S%d tries to get real index at %d, LII:%d, LLI:%d",
		rf.me, index, rf.lastIncludedIndex, rf.getLastLogIndex())
	return -1
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIncludedIndex = lastIncludedIndex - 1
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	Debug(dSnap, "S%d installed SN, LII:%d LIT:%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	if lastIncludedIndex >= rf.getLastLogIndex() {
		rf.log = []logEntry{}
	} else {
		rf.log = rf.log[rf.getLogRealIndex(lastIncludedIndex)+1:]
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	index = index - 1
	rf.mu.Lock()

	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		Debug(dError, "S%d SN err index:%d, LII:%d, CI:%d", rf.me, index, rf.lastIncludedIndex, rf.commitIndex)
		rf.mu.Unlock()
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	Debug(dSnap, "S%d installed SN at %d, LII:%d, LIT:%d", rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm)
	rf.log = rf.log[rf.getLogRealIndex(index)+1:]
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
	rf.mu.Unlock()
}

// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	stepAside := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persister.SaveRaftState(rf.getPersistState())
		if rf.state == Leader || rf.state == Candidate {
			stepAside = true
		}
		rf.state = Follower
	}
	reply.Term = rf.currentTerm

	isUpToDate := true
	lastLog := rf.getLogEntry(rf.getLastLogIndex())
	if lastLog != nil {
		isUpToDate = args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persister.SaveRaftState(rf.getPersistState())

		rf.rtFlag = true
		rf.mu.Unlock()
		if len(rf.rtCh) == 0 {
			rf.rtCh <- 1
		}
	} else {
		reply.VoteGranted = false

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

// ⚠️ When calling sendRequestVote, MUST NOT hold the lock.
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
	OutDated      bool
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Return the logical index of first log whose entry has corresponding term.
func firstLog(logs []logEntry, term int) int {
	for i := range logs {
		if logs[i].Term == term {
			return logs[i].Index
		}
	}
	return -1
}

// Return the logical index of last log whose entry has corresponding term.
func lastLog(logs []logEntry, term int) int {
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i].Term == term {
			return logs[i].Index
		}
	}
	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// ATTENTION: consider leader cannnot step aside, brain split -> commit inconsistency?
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.OutDated = true
		rf.mu.Unlock()
		return
	}

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
		rf.persister.SaveRaftState(rf.getPersistState())
	}
	rf.state = Follower
	reply.Term = rf.currentTerm

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// (We make sure that args.PrevLogIndex is in valid range of rf's log by checking in ticker())
	if args.PrevLogIndex != -1 && (args.PrevLogIndex > rf.getLastLogIndex() ||
		(rf.getLogEntry(args.PrevLogIndex) != nil && rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm)) {
		reply.Success = false

		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.ConflictIndex = rf.getLastLogIndex() + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.getLogEntry(args.PrevLogIndex).Term
			reply.ConflictIndex = firstLog(rf.log, reply.ConflictTerm)
		}
	} else {
		reply.Success = true

		idx := args.PrevLogIndex + 1
		isConflict := false
		endIdx := -1
		if rf.getLastLogIndex() > args.PrevLogIndex {
			for i := range args.Entries {
				if idx == rf.getLastLogIndex()+1 {
					break
				}

				// if args.Entries[i] == *rf.getLogEntry(idx) {
				if reflect.DeepEqual(args.Entries[i], *rf.getLogEntry(idx)) {
					idx++
				} else {
					isConflict = true
					endIdx = i
					break
				}
			}
		}

		if isConflict {
			rf.log = rf.log[:rf.getLogRealIndex(idx)]
			rf.log = append(rf.log, args.Entries[endIdx:]...)
			rf.persister.SaveRaftState(rf.getPersistState())
		} else {
			if idx == rf.getLastLogIndex()+1 {
				rf.log = rf.log[:rf.getLogRealIndex(args.PrevLogIndex)+1]
				rf.log = append(rf.log, args.Entries...)
				rf.persister.SaveRaftState(rf.getPersistState())
			}
		}

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			Debug(dLog2, "S%d CIDX -> %d", rf.me, rf.commitIndex)
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

// ⚠️ When calling sendAppendEntries, MUST NOT hold the lock.
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

type InstallSnapshotRequest struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persister.SaveRaftState(rf.getPersistState())
	}

	// received outdated snapshot due to retransmission by leader
	// actrually we can use 'if args.LastIncludedIndex == rf.lastIncludedIndex' as the condition
	if args.LastIncludedTerm < rf.lastIncludedTerm || args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	rf.rtFlag = true
	rf.mu.Unlock()
	if len(rf.rtCh) == 0 {
		rf.rtCh <- 1
	}

	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex + 1, SnapshotTerm: args.LastIncludedTerm}
	Debug(dCommit, "S%d installing SN LII:%d", rf.me, args.LastIncludedIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm

	log := logEntry{command, rf.currentTerm, index}
	rf.log = append(rf.log, log)
	Debug(dLog, "S%d appends log at %d", rf.me, index)
	rf.persister.SaveRaftState(rf.getPersistState())

	rf.matchIndex[rf.me] = rf.getLastLogIndex()

	rf.hasReq = true
	rf.mu.Unlock()

	return index + 1, term, true
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
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		reply := &RequestVoteReply{}

		idx := i
		// Send RequestVote RPCs concurrently.
		go func(int) {
			ok := rf.sendRequestVote(idx, args, reply)
			rf.mu.Lock()
			if ok && rf.state == Candidate && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persister.SaveRaftState(rf.getPersistState())
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
						for i := range rf.matchIndex {
							rf.matchIndex[i] = -1
						}
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
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

		interval := 500 + rand.Intn(250) // need to be big enough to pass 2c's unreliable test.

		rf.mu.Lock() // may stuck here if send data to rf.rtCh or ch(election) while holding the lock.

		if rf.rtFlag {
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
				// timeout, start election...
				rf.mu.Lock()
				if rf.rtFlag {
					<-rf.rtCh
					rf.rtFlag = false
				} else {
					rf.currentTerm++
					rf.state = Candidate
					rf.votedFor = rf.me
					rf.persister.SaveRaftState(rf.getPersistState())
				}
				rf.mu.Unlock()
			}

		} else if rf.state == Candidate {
			args := &RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.getLastLogIndex()
			if args.LastLogIndex == -1 {
				args.LastLogTerm = -1
			} else {
				args.LastLogTerm = rf.getLogEntry(args.LastLogIndex).Term
			}
			rf.mu.Unlock()

			go rf.election(args)

			select {
			case <-time.After(time.Duration(interval) * time.Millisecond):
				rf.mu.Lock()
				if rf.rtFlag {
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
			for {
				if rf.killed() {
					break
				}

				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					idx := i
					rf.mu.Lock()
					// corresponding log has been truncated, should send InstallSnapshotRPC instead of AppendEntriesRPC
					if rf.nextIndex[idx] <= rf.lastIncludedIndex {
						snargs := &InstallSnapshotRequest{}
						snreply := &InstallSnapshotReply{}
						snargs.Term = rf.currentTerm
						snargs.LeaderId = rf.me
						snargs.LastIncludedIndex = rf.lastIncludedIndex
						snargs.LastIncludedTerm = rf.lastIncludedTerm
						snargs.Data = rf.persister.ReadSnapshot()
						rf.mu.Unlock()

						go func(int) {
							timeout := time.After(2000 * time.Millisecond)
							okCh := make(chan bool, 1)
							Debug(dSnap, "S%d send SN to S%d, LII:%d LIT:%d", rf.me, idx, snargs.LastIncludedIndex, snargs.LastIncludedTerm)
							select {
							case okCh <- rf.sendInstallSnapshot(idx, snargs, snreply):
								rf.mu.Lock()
								defer rf.mu.Unlock()
								ok := <-okCh
								if ok && rf.state == Leader && rf.currentTerm == snargs.Term {
									if snreply.Term > rf.currentTerm {
										rf.currentTerm = snreply.Term
										rf.persister.SaveRaftState(rf.getPersistState())
										rf.state = Follower
										Debug(dTerm, "S%d <- S%d IS no reply, become follower", rf.me, idx)
										return
									}
									if snreply.Success {
										if snargs.LastIncludedIndex > rf.matchIndex[idx] { // reply has been outdated (Compared with AE)!!!
											// still some bugs here, I think...
											rf.matchIndex[idx] = snargs.LastIncludedIndex
											rf.nextIndex[idx] = snargs.LastIncludedIndex + 1
											Debug(dLeader, "S%d <- S%d IS yes reply, set its NI -> %d", rf.me, idx, rf.nextIndex[idx])
										}
									}
								}
							case <-timeout:
							}
						}(idx)
					} else {
						args := &AppendEntriesArgs{}
						reply := &AppendEntriesReply{}

						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.PrevLogIndex = rf.nextIndex[idx] - 1

						if args.PrevLogIndex == -1 {
							args.PrevLogTerm = -1
						} else {
							args.PrevLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
						}
						args.LeaderCommit = rf.commitIndex

						originalLogs := make([]logEntry, len(rf.log))
						copy(originalLogs, rf.log)
						if rf.getLastLogIndex() >= rf.nextIndex[idx] && rf.getLogRealIndex(rf.nextIndex[idx]) != -1 {
							args.Entries = make([]logEntry, len(rf.log[rf.getLogRealIndex(rf.nextIndex[idx]):]))
							copy(args.Entries, rf.log[rf.getLogRealIndex(rf.nextIndex[idx]):])
						}
						rf.mu.Unlock()

						go func(int) {
							timeout := time.After(2000 * time.Millisecond)
							okCh := make(chan bool, 1)
							select {
							case okCh <- rf.sendAppendEntries(idx, args, reply):
								rf.mu.Lock()
								defer rf.mu.Unlock()
								ok := <-okCh
								if ok && reply.OutDated {
									return
								}
								if ok && rf.state == Leader && rf.currentTerm == args.Term {
									if reply.Term > rf.currentTerm {
										rf.currentTerm = reply.Term
										rf.persister.SaveRaftState(rf.getPersistState())
										rf.state = Follower
										Debug(dTerm, "S%d <- S%d AE no reply, become follower", rf.me, idx)
										return
									}
									if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[idx] { // consider the reply has been outdated (Compared with IS)!!!
										if reply.Success {
											rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
											rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1
											Debug(dLeader, "S%d <- S%d AE yes reply, set its NI -> %d", rf.me, idx, rf.nextIndex[idx])

											// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
											// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
											cp := make([]int, len(rf.matchIndex))
											copy(cp, rf.matchIndex)
											sort.Ints(cp)
											N := cp[(len(cp)-1)/2]
											// Leader can only commit log entries of its own term.
											if N > rf.commitIndex && (rf.getLogEntry(N)).Term == rf.currentTerm {
												rf.commitIndex = N
												Debug(dLeader, "S%d CIDX -> %d", rf.me, rf.commitIndex)
											}
										} else {
											if reply.ConflictTerm != -1 && lastLog(originalLogs, reply.ConflictTerm) != -1 {
												rf.nextIndex[idx] = lastLog(originalLogs, reply.ConflictTerm) + 1
											} else {
												rf.nextIndex[idx] = reply.ConflictIndex
											}
											Debug(dLeader, "S%d <- S%d AE no reply, set its NI -> %d", rf.me, idx, rf.nextIndex[idx])
										}
									}
								}
							case <-timeout:
							}
						}(idx)
					}
				}

				// time.Sleep(105 * time.Millisecond)
				heartBeatInterval := time.After(105 * time.Millisecond)
				select {
				case <-heartBeatInterval:
				case <-rf.hasReqCh:
				}
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastAppliedIndex < rf.commitIndex {
			for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex; i++ {
				lastIncludedIndex := rf.lastIncludedIndex
				if i > lastIncludedIndex {
					logEntry := rf.getLogEntry(i)
					idx := logEntry.Index
					applyMsg := ApplyMsg{CommandValid: true, Command: logEntry.Command, CommandIndex: logEntry.Index + 1}
					Debug(dLog2, "S%d applied at %d", rf.me, i)
					rf.mu.Unlock()
					rf.applyCh <- applyMsg
					Debug(dCommit, "S%d applied cmd at %d, LII:%d", rf.me, idx, lastIncludedIndex)
				} else {
					rf.mu.Unlock()
				}
				rf.mu.Lock()
			}
			rf.lastAppliedIndex = rf.commitIndex
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) reqDetector() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.hasReq {
			rf.hasReq = false
			rf.mu.Unlock()
			rf.hasReqCh <- 1
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(20 * time.Millisecond)
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

	rf.mu.Lock()
	rf.state = Follower
	rf.rtCh = make(chan int) // Remember initialization of channel.
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastAppliedIndex = -1

	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	rf.hasReqCh = make(chan int)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	go rf.reqDetector()

	return rf
}
