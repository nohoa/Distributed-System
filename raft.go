package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
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

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state State

	electionTimeout time.Duration
	electionStart   time.Time

	applyCh chan raftapi.ApplyMsg

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) realIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.realIndex(index)].Term
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = rf.persister.ReadSnapshot()

	if len(rf.log) == 0 {
		rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}
	}
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastLogIndex() {
		return
	}

	term := rf.termAt(index)

	newLog := []LogEntry{{Term: term}}
	if index < rf.lastLogIndex() {
		newLog = append(newLog, rf.log[rf.realIndex(index)+1:]...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot

	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	rf.state = Follower
	rf.resetTime()
	reply.Term = rf.currentTerm

	prevId := args.PrevLogIndex
	prevTerm := args.PrevLogTerm
	entries := args.Entries

	if prevId < rf.lastIncludedIndex {
		offset := rf.lastIncludedIndex - prevId

		if offset >= len(entries) {
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = minInt(args.LeaderCommit, rf.lastLogIndex())
			}
			reply.Success = true
			return
		}

		entries = entries[offset:]
		prevId = rf.lastIncludedIndex
		prevTerm = rf.lastIncludedTerm
	}

	if prevId > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	if rf.termAt(prevId) != prevTerm {
		reply.ConflictTerm = rf.termAt(prevId)

		index := prevId
		for index > rf.lastIncludedIndex && rf.termAt(index-1) == reply.ConflictTerm {
			index--
		}

		reply.ConflictIndex = index
		return
	}

	indexInsert := prevId + 1

	for i := 0; i < len(entries); i++ {
		logIndex := indexInsert + i

		if logIndex > rf.lastLogIndex() {
			rf.log = append(rf.log, entries[i:]...)
			rf.persist()
			break
		}

		if rf.termAt(logIndex) != entries[i].Term {
			rf.log = rf.log[:rf.realIndex(logIndex)]
			rf.log = append(rf.log, entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.lastLogIndex())
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	rf.state = Follower
	rf.resetTime()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex < rf.lastLogIndex() &&
		rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog := make([]LogEntry, 0)
		newLog = append(newLog, LogEntry{Term: args.LastIncludedTerm})
		newLog = append(newLog, rf.log[rf.realIndex(args.LastIncludedIndex)+1:]...)
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	rf.persist()

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Unlock()

	rf.applyCh <- msg
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			index := rf.lastApplied

			if index <= rf.lastIncludedIndex {
				continue
			}

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.realIndex(index)].Command,
				CommandIndex: index,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	lastIndex := rf.lastLogIndex()
	lastTerm := rf.termAt(lastIndex)

	upToDate := false
	if args.LastLogTerm > lastTerm {
		upToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetTime()
		rf.persist()
		reply.VoteGranted = true
	}
}

func (rf *Raft) replicateOne(server int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}

		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader || args.Term != rf.currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		return
	}

	nextIdx := rf.nextIndex[server]
	prevIdx := nextIdx - 1
	prevTerm := rf.termAt(prevIdx)

	entries := make([]LogEntry, len(rf.log[rf.realIndex(nextIdx):]))
	copy(entries, rf.log[rf.realIndex(nextIdx):])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)

		rf.matchIndex[server] = match
		rf.nextIndex[server] = match + 1

		for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
			if rf.termAt(N) != rf.currentTerm {
				continue
			}

			count := 1
			for s := range rf.peers {
				if s == rf.me {
					continue
				}
				if rf.matchIndex[s] >= N {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				return
			}
		}
	} else {
		if reply.ConflictTerm != -1 {
			found := false

			for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.termAt(i) == reply.ConflictTerm {
					rf.nextIndex[server] = i + 1
					found = true
					break
				}
			}

			if !found {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}

		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			rf.nextIndex[server] = rf.lastIncludedIndex
		}
	}
}

func (rf *Raft) Broadcast() {
	for server := range rf.peers {
		if server != rf.me {
			go rf.replicateOne(server)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm

	entry := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.persist()

	index := rf.lastLogIndex()

	rf.nextIndex[rf.me] = rf.lastLogIndex() + 1
	rf.matchIndex[rf.me] = rf.lastLogIndex()

	go rf.Broadcast()

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state == Leader {
			rf.mu.Unlock()
			rf.Broadcast()
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			continue
		}

		if time.Since(rf.electionStart) >= rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) resetTime() {
	rf.electionTimeout = time.Duration(500+rand.Intn(300)) * time.Millisecond
	rf.electionStart = time.Now()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.state = Candidate
	rf.currentTerm++
	termStarted := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
	rf.resetTime()

	cntVote := 1

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.termAt(lastLogIndex)

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Candidate || rf.currentTerm != termStarted {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.resetTime()
				rf.persist()
				return
			}

			if reply.VoteGranted {
				cntVote++

				if cntVote > len(rf.peers)/2 && rf.state == Candidate {
					rf.state = Leader

					for i := range rf.peers {
						rf.nextIndex[i] = rf.lastLogIndex() + 1
						rf.matchIndex[i] = 0
					}

					rf.matchIndex[rf.me] = rf.lastLogIndex()
					rf.nextIndex[rf.me] = rf.lastLogIndex() + 1

					go rf.Broadcast()
				}
			}
		}(i)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = persister.ReadSnapshot()

	rf.log = []LogEntry{
		{Term: 0},
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.electionTimeout = time.Duration(500+rand.Intn(300)) * time.Millisecond
	rf.electionStart = time.Now()

	rf.applyCh = applyCh

	
	rf.readPersist(persister.ReadRaftState())

	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.nextIndex[rf.me] = rf.lastLogIndex() + 1

	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	go rf.ticker()
	go rf.applier()

	return rf
}