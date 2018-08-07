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
	"labrpc"
	"math/rand"
	"sync"
	"time"

	"MIT6.824/src/labgob"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Heartbeat       = time.Millisecond * 100
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

type enum int

const (
	Follower enum = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state     enum
	sumofvote int
	applyCh   chan ApplyMsg
	timer     *time.Timer
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ReplicatedIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("%d AppendEntries", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term

		if args.PrevLogIndex >= 0 &&
			(len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.ReplicatedIndex = len(rf.log) - 1
			if reply.ReplicatedIndex > args.PrevLogIndex {
				reply.ReplicatedIndex = args.PrevLogIndex
			}
			for reply.ReplicatedIndex >= 0 {
				if rf.log[reply.ReplicatedIndex].Term == args.PrevLogTerm {
					break
				}
				reply.ReplicatedIndex--
			}
			reply.Success = false
		} else if args.Entries != nil {
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...) //	切片被打散传入
			if len(rf.log)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLog()
			}
			reply.ReplicatedIndex = len(rf.log) - 1
			reply.Success = true
		} else {
			if len(rf.log)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLog()
			}
			reply.ReplicatedIndex = args.PrevLogIndex
			reply.Success = true
		}
	}
	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntriesToAll() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			if rf.nextIndex[i] <= len(rf.log)-1 {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			args.LeaderCommit = rf.commitIndex

			go func(server int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok {
					rf.handleAppendEntries(server, reply)
				}
			}(i, args)
		}
	}
}

func (rf *Raft) handleAppendEntries(server int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.ReplicatedIndex + 1
		rf.matchIndex[server] = reply.ReplicatedIndex
		sum_of_reply := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				if rf.matchIndex[i] >= rf.matchIndex[server] {
					sum_of_reply += 1
				}
			}
		}
		if sum_of_reply >= (len(rf.peers)/2+1) &&
			rf.commitIndex < rf.matchIndex[server] &&
			rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLog()
		}
	} else {
		rf.nextIndex[server] = reply.ReplicatedIndex + 1
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d RequestVote", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		if (len(rf.log) > 1 && ((rf.log[len(rf.log)-1].Term < args.LastLogTerm) ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex))) ||
			len(rf.log) == 1 {
			DPrintf("%d become follower", rf.me)
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.persist()
			rf.resetTimer()
			return
		}
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if (len(rf.log) > 1 && ((rf.log[len(rf.log)-1].Term < args.LastLogTerm) ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex))) ||
			len(rf.log) == 1 {
			DPrintf("%d become follower", rf.me)
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.persist()
			rf.resetTimer()
			return
		}
	}
	reply.VoteGranted = false
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleRequestVote(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if rf.state == Candidate && reply.VoteGranted {
		rf.sumofvote += 1
		if rf.sumofvote >= (len(rf.peers)/2 + 1) {
			rf.state = Leader
			DPrintf("%d become leader", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}
			rf.sendAppendEntriesToAll()
			rf.resetTimer()
		}
	}

}

func (rf *Raft) commitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > len(rf.log)-1 {
		rf.commitIndex = len(rf.log) - 1
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		DPrintf("%d apply %d", rf.me, i)
		rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: i, Command: rf.log[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	if rf.state != Leader {
		return index, term, isLeader
	}

	log := LogEntry{command, rf.currentTerm}
	isLeader = true
	rf.log = append(rf.log, log)
	index = len(rf.log) - 1
	term = rf.currentTerm
	// Your code here (2B).
	rf.persist()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) handleTimer() {
	DPrintf("%d handleTimer", rf.me)
	rf.mu.Lock()
	if rf.state != Leader {
		rf.state = Candidate
		DPrintf("%d become candidate", rf.me)
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.sumofvote = 1
		rf.persist()
		args := &RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
		if len(rf.log) > 1 {
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
		}
		rf.resetTimer()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int, args *RequestVoteArgs) {
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(i, args, reply)
					if ok {
						rf.handleRequestVote(reply)
					}
				}(i, args)
			}
		}
	} else {
		rf.sendAppendEntriesToAll()
		rf.resetTimer()
	}
	rf.mu.Unlock()
}

func (rf *Raft) resetTimer() {
	timeout := Heartbeat
	if rf.state != Leader {
		timeout = time.Millisecond * time.Duration(ElectionMinTime+rand.Intn(150))
	}
	rf.timer.Reset(timeout)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}

	rf.state = Follower
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.timer = time.NewTimer(time.Millisecond * time.Duration(ElectionMinTime+rand.Intn(150)))
	go func() {
		for {
			<-rf.timer.C
			rf.handleTimer()
		}
	}()
	DPrintf("make: %d", rf.me)
	return rf
}
