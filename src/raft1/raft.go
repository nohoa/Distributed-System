package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
		"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

		"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type State int


type AppendEntriesArgs struct {
	Term int 
	LeaderId int 
	PrevLogIndex int 
	PrevLogTerm int 
	Entries    []LogEntry 
	LeaderCommit	int 

	
}

type AppendEntriesReply struct {
	Term int
	Success	bool
}

const (
	Follower State = iota
	Candidate
	Leader
)


type LogEntry struct {
	Term  int 
	Command interface{}
}



// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm	int 
	votedFor 	int 
	log	[]LogEntry

	commitIndex int 
	lastApplied int 

	nextIndex	[]int

	matchIndex	[]int 

	state		State

	electionTimeout time.Duration

	electionStart	time.Time

	applyCh chan raftapi.ApplyMsg



}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm

	isleader = (rf.state == Leader)
	
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

}


func (rf* Raft) AppendEntries(arg* AppendEntriesArgs, reply* AppendEntriesReply )  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf -> current server 
	// arg : leader 
	reply.Term = rf.currentTerm 
	reply.Success = false 

	if rf.currentTerm > arg.Term {
		return 
	}

	rf.state = Follower

	prevId := arg.PrevLogIndex

	prevTerm := arg.PrevLogTerm

	rf.resetTime()


	// If the server has been left behind compare to the leader, update it

	if arg.Term > rf.currentTerm {
		rf.currentTerm = arg.Term
		rf.votedFor = -1

		rf.persist()
	}

	if prevId >= len(rf.log) {
		return 
	}

	if rf.log[prevId].Term != prevTerm {
		return 
	}

	indexInsert := prevId +1 

	for i := 0 ; i < len(arg.Entries) ; i++ {
		logIndex := indexInsert + i
		if logIndex < len(rf.log){
			if rf.log[logIndex].Term != arg.Entries[i].Term {
				rf.log = rf.log[:logIndex]

				for j := i ;j < len(arg.Entries) ; j ++ {
					rf.log = append(rf.log,arg.Entries[j])
				}

				rf.persist()

				break 

				}
			} else {
				rf.log = append(
				rf.log,
				arg.Entries[i:]...,
				)

				rf.persist()

				break 
			}

	}


	if arg.LeaderCommit > rf.commitIndex {
		lastNewEntry := len(rf.log) - 1
		rf.commitIndex = min(arg.LeaderCommit,lastNewEntry )
	} 

	reply.Success = true 

}


func (rf *Raft) applier() {

	for !rf.killed() {

		rf.mu.Lock()

		for rf.lastApplied < rf.commitIndex {

			rf.lastApplied++

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()

			rf.applyCh <- msg

			rf.mu.Lock()
		}

		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

    var currentTerm int
    var votedFor int
    var log []LogEntry

    if d.Decode(&currentTerm) != nil ||
        d.Decode(&votedFor) != nil ||
        d.Decode(&log) != nil {
        return
    }

    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.log = log
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int 
	LastLogIndex int 
	LastLogTerm int 
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int 
	VoteGranted bool
}



func (rf *Raft) replicateOne(server int) {
    rf.mu.Lock()

    if rf.state != Leader {
        rf.mu.Unlock()
        return
    }

    nextIdx := rf.nextIndex[server]
    prevIdx := nextIdx - 1
    prevTerm := rf.log[prevIdx].Term

    entries := make([]LogEntry, len(rf.log[nextIdx:]))
    copy(entries, rf.log[nextIdx:])

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


		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {

			if rf.log[N].Term != rf.currentTerm {
				continue 
			}

			count := 1 

			for s := range rf.peers {
				if s  == rf.me {
					continue 
				}
				if rf.matchIndex[s] >= N {
					count ++ 
				}
			}
			
			if count > len(rf.peers)/ 2 {
				rf.commitIndex = N 
				return 
			}
		}
        

    } else {
		rf.nextIndex[server] -- ;
		go rf.replicateOne(server)
		return 
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("Candidate",args.CandidateId, "term ", args.Term, "Request ", rf.me, "with term ", rf.currentTerm)

    // args is server request vote 
	// rf is current server 

	// catch up because term inconsistency 
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 
		reply.VoteGranted = false  
		rf.state = Follower
		rf.persist()

	}

	// the request server is not the most up-to-date -> not grant vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false 
		reply.Term = rf.currentTerm
		rf.persist()
		return 
	}

	lastIndex :=  len(rf.log) -1 
	lastTerm := rf.log[lastIndex].Term

	upToDate := false   

	if args.LastLogTerm > lastTerm {
		upToDate = true 
	} else if lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex {
		upToDate = true 
	}


	if (rf.votedFor == -1 ||
	rf.votedFor == args.CandidateId) &&
	upToDate {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm 
		rf.resetTime()
		reply.VoteGranted = true 
		rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()

	defer rf.mu.Unlock()

	leader := rf.state

	if leader != Leader {
		return -1,-1,false 
	}



	term = rf.currentTerm


	entry := LogEntry {
		Term : term,
		Command : command, 
	}


	rf.log = append(rf.log,entry)

	rf.persist()

	index = len(rf.log) -1 

	// persist to those candidate 
	// rf.

	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) -1
	
	go rf.Broadcast()



	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
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


		// pause for a random amount of time between 50 and 350
		// milliseconds.
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
	rf.resetTime()

	cnt_vote := 1


	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex : len(rf.log) - 1, 
				LastLogTerm :  rf.log[len(rf.log)-1].Term,
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
				cnt_vote++

				if cnt_vote > len(rf.peers)/2 {
					rf.state = Leader

					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[rf.me] = len(rf.log) -1

					go rf.Broadcast()
				}


			}
		}(i)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.currentTerm = 0 
	rf.votedFor = -1
	rf.state = Follower 

	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		 rf.matchIndex = append(rf.matchIndex,0)
	}

	rf.log = []LogEntry{
	{Term: 0},
}

	rf.electionTimeout = time.Duration(500+rand.Intn(300)) * time.Millisecond

	rf.electionStart = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCh = applyCh 

	// start ticker goroutine to start elections
	go rf.ticker()


	// start go apply channel 
	go rf.applier()


	return rf
}

