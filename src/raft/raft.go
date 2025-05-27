package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyChan passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyChan, but set CommandValid to false for these
// other uses.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Role of currentState node
type currentState int

const (
	Follower currentState = iota
	Candidate
	Leader
)

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

const (
	MaxVoteTime = 200
	MinVoteTime = 150

	HeartbeatSleep = 120
	AppliedSleep   = 30
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's currentState
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted currentState
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// All servers require persistent variables:
	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg

	currentState currentState
	voteCount    int
	votedTimer   time.Time

	// Used to pass in snapshot points in 2D
	lastIncludedIndex int
	lastIncludedTerm  int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent currentState, and also initially holds the most
// recent saved currentState, if any. applyChan is a channel on which the
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
	rf.mu.Lock()

	rf.currentState = Follower
	rf.currentTerm = 0
	rf.voteCount = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.mu.Unlock()

	rf.readPersist(persister.ReadRaftState())

	// Synchronize snapshot information
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}

	go rf.CheckForElections()

	go rf.HandleLeader()

	go rf.ApplyLogs()

	return rf
}

// ------------------------------------------------ --------Checking for new elections---------------------------------------- ----------
func (rf *Raft) CheckForElections() {
	for rf.killed() == false {
		nowTime := time.Now()
		time.Sleep(time.Duration(getRandomElectionTimeout()) * time.Millisecond)

		rf.mu.Lock()

		if rf.votedTimer.Before(nowTime) && rf.currentState != Leader {
			rf.currentState = Candidate
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.currentTerm += 1
			rf.persist()
			rf.startElections()
			rf.votedTimer = time.Now()
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) HandleLeader() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.currentState == Leader {
			rf.mu.Unlock()
			rf.sendHeartBeats()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) ApplyLogs() {
	for rf.killed() == false {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
			})
		}

		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}

}

//------------------------------------------------Leader election------------------------------------------------- ----------

func (rf *Raft) startElections() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			rf.getLastIndex(),
			rf.getLastTerm(),
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}

}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.resetToFollower(args.Term)
	}
	if rf.outOfDateLogs(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm
	rf.resetToFollower(rf.currentTerm)
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

}
func (rf *Raft) resetToFollower(term int) {
	rf.voteCount = 0
	rf.currentState = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votedTimer = time.Now()
	rf.persist()
}
func (rf *Raft) outOfDateLogs(candidateLastIdx int, candidateLastTerm int) bool {
	lastIdx := rf.getLastIndex()
	if lastIdx < 0 {
		return false
	}
	lastTerm := rf.getLastTerm()
	return lastTerm > candidateLastTerm || (lastTerm == candidateLastTerm && lastIdx > candidateLastIdx)
}

// ---------------------------------------------Leader Heartbeats------------------------------------------------ ----------
func (rf *Raft) sendHeartBeats() {

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.currentState != Leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[server]-1 < rf.lastIncludedIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			go rf.sendAppendEntries(server, &args, &reply)

		}(index)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.CommitIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.CommitIndex = -1

	rf.resetToFollower(args.Term)

	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		reply.CommitIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.CommitIndex = rf.getLastIndex()
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			curTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastIncludedIndex; index-- {
				if rf.restoreLogTerm(index) != curTerm {
					reply.CommitIndex = index + 1
					break
				}
			}
			reply.Success = false
			return
		}
	}

	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	return
}

// ---------------------------------------------Log compression (Snapshot) Section------------------------------------------------ ----------
func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.currentState != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			rf.resetToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	reply.Term = args.Term

	rf.resetToFollower(args.Term)
	rf.votedTimer = time.Now()

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludedIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.Save(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()

	rf.applyChan <- msg

}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedIndex >= index || index > rf.commitIndex {
		return
	}
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	if index == rf.getLastIndex()+1 {
		rf.lastIncludedTerm = rf.getLastTerm()
	} else {
		rf.lastIncludedTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludedIndex = index
	rf.logs = sLogs

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.Save(rf.persistData(), snapshot)
}

// ----------------------------------------------Persistence (persist) part------------------------------------------------ ----------
//
// save Raft's persistent currentState to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.mu.Lock()
	defer rf.persister.mu.Unlock()
	rf.persister.raftstate = clone(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == Leader
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
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
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.currentState != Leader {
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		return index, term, true
	}
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

// ----------------------------------------------- UTILITY Functions -------------------------------------------------------------
func getRandomElectionTimeout() int {
	return rand.Intn(MaxVoteTime) + MinVoteTime
}

func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logs[curIndex-rf.lastIncludedIndex]
}

func (rf *Raft) restoreLogTerm(curIndex int) int {
	if curIndex-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logs[curIndex-rf.lastIncludedIndex].Term
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastTerm() int {

	if len(rf.logs)-1 == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

// ------------------------------------------------ RPC Coroutines ------------------------------------------------------
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()

	if rf.currentState != Candidate || args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}

	if reply.Term > args.Term {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
		}
		rf.resetToFollower(reply.Term)
		rf.mu.Unlock()
		return false
	}

	if reply.VoteGranted == true && rf.currentTerm == args.Term {
		rf.voteCount += 1
		if rf.voteCount >= len(rf.peers)/2+1 {

			rf.currentState = Leader
			rf.votedFor = -1
			rf.voteCount = 0
			rf.persist()

			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getLastIndex() + 1
			}

			rf.matchIndex = make([]int, len(rf.peers))
			rf.matchIndex[rf.me] = rf.getLastIndex()

			rf.votedTimer = time.Now()
			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
		return true
	}

	rf.mu.Unlock()
	return ok

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != Leader {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.resetToFollower(reply.Term)
		return false
	}

	if reply.Success {

		rf.commitIndex = rf.lastIncludedIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		for index := rf.getLastIndex(); index >= rf.lastIncludedIndex+1; index-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum += 1
					continue
				}
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}

			if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}

		}
	} else {
		if reply.CommitIndex != -1 {
			rf.nextIndex[server] = reply.CommitIndex
		}
	}
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
