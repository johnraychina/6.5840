package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// broadcastTime << electionTimeout << MTBF
const termDuration = 500 * time.Millisecond

type LogEntry struct {
	term    int
	command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int         // latest Term server seen (initialized to 0 on first boot, increase monotonically)
	voteForId   int         // CandidateId that received vote in current Term
	log         []*LogEntry // log entries

	// volatile state on all servers
	commitIndex    int       // index of highest log entry known to be committed
	lastApplied    int       // index of highest log entry applied to state machine
	lastAppendTime time.Time // last append time (to trigger election)

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArg struct {
	LeaderId int         // so follower can redirect clients
	Term     int         // leader's Term
	Command  interface{} // client's Command

	PreviousLogIndex int
	PreviousLogTerm  int

	LeaderCommitIndex int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Receiver implementation:
// 1. Reply false if Term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose Term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	// todo
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int // candidate's Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int //Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
// Receiver implementation:
// 1. Reply false if Term < currentTerm (§5.1)
// 2. If votedFor is null or CandidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// todo
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if time.Now().After(rf.lastAppendTime.Add(termDuration)) {

			rf.mu.Lock()

			// vote for myself
			rf.currentTerm++
			rf.voteForId = rf.me
			currentTerm := rf.currentTerm
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.log[lastLogIndex].term
			}

			rf.mu.Unlock()

			// hey guys, please vote for me!
			granted, peerReplies := rf.broadcastVote(currentTerm, lastLogIndex, lastLogTerm)

			// over half grants
			// hey guys, I'm the new leader!
			if granted*2 > len(rf.peers) {
				rf.promote(currentTerm, peerReplies, lastLogIndex, lastLogTerm)
			}
		}
	}
}

func (rf *Raft) broadcastVote(currentTerm int, lastLogIndex int, lastLogTerm int) (int, []*RequestVoteReply) {
	peerReplies := make([]*RequestVoteReply, len(rf.peers))
	//todo go routine

	granted := 1 // I've voted for myself
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:         currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}

		ok := rf.sendRequestVote(i, args, reply)
		if ok {
			peerReplies[i] = reply
			// If my Term not larger than peers, give up for this Term.
			// why not vote for the peer here?
			// peer may have sent RequestVote to me in another thread, and I've voted for him
			if currentTerm <= reply.Term {
				return 0, nil
			}
			if reply.VoteGranted {
				granted++
			}
		}
	}

	return granted, peerReplies
}

// todo promote myself to leader by broadcasting empty AppendEntries
func (rf *Raft) promote(currentTerm int, peerReplies []*RequestVoteReply, previousLogIndex, previousLogTerm int) {

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		arg := &AppendEntriesArg{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PreviousLogIndex:  previousLogIndex,
			PreviousLogTerm:   previousLogTerm,
			LeaderCommitIndex: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}

		// todo if over a half of the broadcast is lost, peers won't know I'm the new leader , should I start a new round vote?
		ok := rf.sendAppendEntries(i, arg, reply)
		if !ok {
			DPrintf("RpcError[promote] leader:%d, follower:%d, reply:%+v", rf.me, i, reply)
		}
		if currentTerm < reply.Term {
			// someone get a larger Term in the same time, give up for this Term.
			// the peer may be broadcasting to me later.
			DPrintf("TermSuppressedByPeer[promote] me:%d, Term:%d, peer:%d, Term:%d", rf.me, currentTerm, i, reply.Term)
			return
		}
		// The peer may request voting, but I'm already the common leader.
		if currentTerm == reply.Term {
			DPrintf("TermsEqual[promote] me:%d, Term:%d, peer:%d, Term:%d", rf.me, currentTerm, i, reply.Term)
		}

		// todo as a leader, I need to know the followers states by checking each AppendEntriesReply,
		rf.mu.Lock()
		//rf.nextIndex =
		//rf.matchIndex =
		rf.mu.Unlock()
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

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
