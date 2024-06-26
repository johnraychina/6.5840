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
	"slices"
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
const electionTimeout = 1000 * time.Millisecond
const heartBeatTimeout = electionTimeout / 5
const NoneCandidateId = -1

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
	// (Updated on stable storage before responding to RPCs)
	currentTerm int         // latest Term server seen (initialized to 0 on first boot, increase monotonically)
	voteForId   int         // CandidateId that received vote in current Term
	log         []*LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)
	leaderId    int

	// volatile state on all servers
	lastHeartBeatTime time.Time // last heartbeat time
	commitIndex       int       // index of highest log entry known to be committed,  initialized to 0
	lastApplied       int       // index of highest log entry applied to state machine,  initialized to 0
	applyCh           chan ApplyMsg

	// volatile state on leaders
	// (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server, initialized to leader last log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on server,  initialized to 0

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isLeader := rf.leaderId == rf.me && rf.lastHeartBeatTime.Add(electionTimeout).After(time.Now())

	return term, isLeader
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
	LeaderId int           // so follower can redirect clients
	Term     int           // leader's Term
	Commands []interface{} // client's Command

	PreviousLogIndex int // to avoid negative values, PreviousLogIndex start at 0, and nextIndex[*] start at 1
	PreviousLogTerm  int

	LeaderCommitIndex int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Receiver implementation:
// 1. Reply false if Term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose Term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {

	//1. Reply false if Term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		//sorry, you're not leader anymore
		DPrintf("[%d]AppendEntries[RejectOldTerm] --> [%d]: my term:%d > arg term:%d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// accept AppendEntries, suppress my election intention
	rf.lastHeartBeatTime = time.Now()
	rf.leaderId = args.LeaderId

	preIdx := args.PreviousLogIndex
	preTerm := args.PreviousLogTerm
	DPrintf("[%d]AppendEntries[arg] preIdx:%d, preTerm:%+v, commands:%+v", rf.me, preIdx, preTerm, args.Commands)

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose Term matches prevLogTerm (§5.3)
	if preIdx >= 1 && (len(rf.log) <= preIdx || rf.log[preIdx].term != preTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// a new leader won the election, he is broadcasting.
	//if len(args.Commands) == 0 {
	//	rf.leaderId = args.LeaderId
	//	reply.Term = rf.currentTerm
	//	reply.Success = true
	//	return
	//}

	rf.mu.Lock()
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if preIdx >= 1 && len(rf.log) > preIdx+1 && rf.log[preIdx].term != preTerm {
		rf.delSince(preIdx)
		//reply.Term = rf.currentTerm
		//reply.Success = true
		//return
	}

	// 4. Append any new entries not already in the log
	base := preIdx + 1
	for i, cmd := range args.Commands {
		entry := &LogEntry{term: args.Term, command: cmd}
		if len(rf.log) > base+i {
			//overwrite existing
			rf.log[base+i] = entry
			DPrintf("[%d]AppendEntries[overwrite] index:%d, entry:%+v", rf.me, base+i, entry)
		} else {
			rf.log = append(rf.log, entry)
			DPrintf("[%d]AppendEntries[append] index:%d, entry:%+v", rf.me, len(rf.log)-1, entry)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	DPrintf("[%d]AppendEntries[commit] my commitIndex:%d, lastIdx:%d, leader:%d commitIndex:%d", rf.me, rf.commitIndex, len(rf.log)-1, args.LeaderId, args.LeaderCommitIndex)
	if args.LeaderCommitIndex > rf.commitIndex {
		old := rf.commitIndex
		lastIdx := len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommitIndex, lastIdx)

		// follower apply msg after commit
		rf.applyMsg(old, rf.commitIndex)
	}
	rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) applyMsg(old int, newCommitIndex int) {
	if newCommitIndex > old {
		for k := old + 1; k <= rf.commitIndex; k++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[k].command,
				CommandIndex: k,
			}
			DPrintf("[%d]ApplyMsg: %+v", rf.me, msg)
			rf.applyCh <- msg
		}
	}
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

	// I've voted for larger term, you're late!
	if args.Term < rf.currentTerm {
		DPrintf("[%d]RequestVote[reject] candidateId:%d, term:%d < currentTerm:%d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		//DPrintf("RequestVote[grant] candidateId:%d, term:%d > me:%d, currentTerm:%d", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		rf.grantVote(args)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// term equal
	// I'm not voting, or I've voted for you!
	if rf.voteForId == NoneCandidateId || rf.voteForId == args.CandidateId {

		if len(rf.log) == 0 {
			//DPrintf("RequestVote[grant] candidateId:%d, term:%d, me:%d, currentTerm:%d", args.CandidateId, args.Term, rf.me, rf.currentTerm)
			rf.grantVote(args)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}

		myLastLogIndex := len(rf.log) - 1
		myLastLog := rf.log[myLastLogIndex]
		if args.LastLogTerm > myLastLog.term {
			//DPrintf("RequestVote[grant] candidateId:%d, lastLogTerm:%d > me:%d, myLastLogTerm:%d",
			//	args.CandidateId, args.LastLogTerm, rf.me, myLastLog.term)
			rf.grantVote(args)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}

		//reject vote
		// impossible:  arg.Term >= currentTerm and arg.LastLogTerm < myLastLogTerm
		if args.LastLogTerm < myLastLog.term {
			panic("illegal states")
		}

		// equal term
		// candidate's log index equal or larger than me
		if args.LastLogIndex >= myLastLogIndex {
			// impossible: multiple leader in one term
			if rf.voteForId != args.CandidateId {
				panic("illegal states")
			}
			//DPrintf("RequestVote[grant] candidateId:%d, LastLogIndex:%d >= me:%d, myLastLogIndex:%d",
			//	args.CandidateId, args.LastLogIndex, rf.me, myLastLogIndex)
			rf.grantVote(args)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		} else {
			DPrintf("[%d]RequestVote[reject] candidateId:%d, LastLogIndex:%d < myLastLogIndex:%d",
				rf.me, args.CandidateId, args.LastLogIndex, myLastLogIndex)
			// candidate's log index less than me: reject
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}

	// I won't vote two candidates in the same term.
	DPrintf("[%d]RequestVote[reject] term:%d, voteForId:%d != candidateId:%d", rf.me, rf.currentTerm, rf.voteForId, args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
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

// Start start agreement on a new log entry:
//
//	rf.Start(command interface{}) (index, term, isleader)
//
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
	// Your code here (3B)
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	rf.mu.Lock()
	rf.log = append(rf.log, &LogEntry{term: term, command: command})
	lastLogIndex := len(rf.log) - 1
	DPrintf("New client command, lastLogIndex:%d", lastLogIndex)
	rf.mu.Unlock()

	go func() {
		rf.broadCastAppendEntries(term, lastLogIndex)
	}()

	return lastLogIndex, term, isLeader
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

//Leaders:
//Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
// repeat during idle periods to prevent election timeouts (§5.2)
//• If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
//• If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
//• If successful: update nextIndex and matchIndex for
//follower (§5.3)
//• If AppendEntries fails because of log inconsistency:
//decrement nextIndex and retry (§5.3)
//• If there exists an N such that N > commitIndex, a majority
//of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if time.Now().After(rf.lastHeartBeatTime.Add(electionTimeout)) {

			rf.mu.Lock()

			// vote for myself
			rf.currentTerm++ // 0(init) -> 1(first vote) -> ...
			rf.voteForId = rf.me
			currentTerm := rf.currentTerm
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := 0
			if lastLogIndex >= 1 { // valid log index [1,...)
				lastLogTerm = rf.log[lastLogIndex].term
			}
			rf.mu.Unlock()

			// hey guys, please vote for me!
			DPrintf("[%d]Election Start, term:%d", rf.me, currentTerm)
			granted := rf.broadcastVote(currentTerm, lastLogIndex, lastLogTerm)
			if granted*2 <= len(rf.peers) {
				DPrintf("[%d]Election NotEnoughGrants, term:%d, granted:%d", rf.me, currentTerm, granted)
				continue
			}
			DPrintf("[%d]Election Win, term:%d, granted:%d", rf.me, currentTerm, granted)

			// over half grants
			// hey guys, I'm the new leader!
			rf.lastHeartBeatTime = time.Now() // for GetState() return is leader
			rf.initNextIndex(rf.leaderId)
			rf.leaderId = rf.me

			go func() {
				for !rf.killed() {
					if term, isLeader := rf.GetState(); isLeader {
						lastIdx := len(rf.log) - 1 // lastLogIndex may change between each heartbeat
						rf.broadCastAppendEntries(term, lastIdx)

						// must smaller than electionTimeout
						// not too small: The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader (if a majority of peers can still communicate).
						time.Sleep(heartBeatTimeout)

					} else {
						DPrintf("Election[LeaderChanged] me:%d, term:%d, voteForId:%d", rf.me, term, rf.voteForId)
						break // not leader
					}
				}
			}()

		}
	}
}

func (rf *Raft) broadcastVote(currentTerm int, lastLogIndex int, lastLogTerm int) int {

	grantCh := make(chan int, len(rf.peers))
	rejectCh := make(chan int, len(rf.peers))

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

		peer := i
		// 由于这里rpc可能阻塞3s超时，导致竞选没及时发出去，必须用 go routine来避免这种问题
		go func() {
			ok := rf.sendRequestVote(peer, args, reply)
			if !ok {
				rejectCh <- peer // take rpc error as rejection
				return
			}
			// If my Term less than peers, give up for this Term.
			// why not vote for the peer here?
			// peer may have sent RequestVote to me in another thread, and I've voted for him
			if currentTerm < reply.Term || !reply.VoteGranted {
				rejectCh <- peer
				return
			}

			grantCh <- peer
			return
		}()
	}

	// wait half of the grants or rejections
	half := len(rf.peers) / 2
	granted := 1 // I've voted for myself
	rejected := 0
	for !rf.killed() {
		select {
		case <-grantCh:
			granted++
		case <-rejectCh:
			rejected++
		}
		if granted > half || rejected > half {
			break
		}
	}

	return granted
}

// broadCastAppendEntries broadcasting empty AppendEntries: I'm the leader
// suppress other peers from requesting vote
//
// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
// • If successful: update nextIndex and matchIndex for
// follower (§5.3)
// • If AppendEntries fails because of log inconsistency:
// decrement nextIndex and retry (§5.3)
// • If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
func (rf *Raft) broadCastAppendEntries(currentTerm int, lastLogIndex int) {
	successCh := make(chan int, len(rf.peers))
	failCh := make(chan int, len(rf.peers))

	rf.nextIndex[rf.me] = lastLogIndex + 1
	rf.matchIndex[rf.me] = lastLogIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		peerId := i

		// collect commands starting at peerNextIndex
		peerNextIndex := rf.nextIndex[peerId] // need to be initialized during Make
		peerPrevTerm := 0
		if peerNextIndex > 1 && len(rf.log) > 1 {
			if peerNextIndex > len(rf.log) {
				panic("illegal state")
			}
			peerPrevTerm = rf.log[peerNextIndex-1].term // valid log index starts at [1,...)
		}

		// commands that peer need to catchup,
		// maybe empty in heartbeat/election win broadcast.
		commands := rf.getCommands(peerNextIndex, lastLogIndex)
		arg := &AppendEntriesArg{
			Term:              currentTerm,
			LeaderId:          rf.me,
			PreviousLogIndex:  peerNextIndex - 1, // 0-init, log index start at 1
			PreviousLogTerm:   peerPrevTerm,      // 0-init, term start at 1
			LeaderCommitIndex: rf.commitIndex,    // 0-init, commit index start at 1, append entry this time, then commit it next time(heartbeat)
			Commands:          commands,
		}

		go func() {

			// as a leader, I need to know the followers states by checking each AppendEntriesReply,
			success := rf.sendPeerAppendEntries(peerId, arg)
			if success {
				// If successful: update nextIndex and matchIndex for follower (§5.3)
				rf.mu.Lock()
				rf.nextIndex[peerId] = arg.PreviousLogIndex + len(arg.Commands) + 1
				rf.matchIndex[peerId] = arg.PreviousLogIndex + len(arg.Commands)
				rf.mu.Unlock()

				successCh <- peerId
			} else {
				rf.mu.Lock()
				// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
				rf.nextIndex[peerId] = max(rf.nextIndex[peerId]-1, 1)   // valid log index starts from 1
				rf.matchIndex[peerId] = max(rf.matchIndex[peerId]-1, 0) // match log index starts from 0
				rf.mu.Unlock()
				failCh <- peerId
			}
		}()
	}

	// wait for over a half successes / failures
	success := 1
	fail := 0
	half := len(rf.peers) / 2
	for !rf.killed() {
		select {
		case <-successCh:
			success++
		case <-failCh:
			fail++
		}

		if fail > half {
			break
		}
		// over half success
		// reset my timer, suppress myself from requesting vote
		// why not reset at the start? I may have lost leadership, I can detect that in this way.
		if success > half {
			rf.mu.Lock()
			//rf.currentTerm++ // heartbeat done, term++
			rf.lastHeartBeatTime = time.Now()
			//If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
			N := majorityIndex(rf.matchIndex)
			DPrintf("[%d]half success, commitIndex:%d, N:%d, matchIndex:%v", rf.me, rf.commitIndex, N, rf.matchIndex)
			old := rf.commitIndex
			rf.commitIndex = max(N, rf.commitIndex)

			rf.mu.Unlock()

			// leader commit apply message
			rf.applyMsg(old, rf.commitIndex)
			break
		}
	}

}

func majorityIndex(matchIndex []int) int {
	temp := slices.Clone(matchIndex)
	slices.Sort(temp)
	return temp[len(temp)/2] // the middle value
}

func (rf *Raft) getCommands(peerNextIndex int, currentIndex int) []interface{} {
	var commands []interface{}
	for k := peerNextIndex; k <= currentIndex; k++ {
		if k >= 1 { // valid log index starts from 1
			commands = append(commands, rf.log[k].command)
		}
	}
	return commands
}

func (rf *Raft) grantVote(args *RequestVoteArgs) {
	rf.mu.Lock() // guard the access to rf.log and rf.voteFor
	defer rf.mu.Unlock()

	//rf.lastHeartBeatTime = time.Now()
	rf.voteForId = args.CandidateId
	rf.currentTerm = args.Term
	rf.leaderId = NoneCandidateId // convert to follower
}

func (rf *Raft) sendPeerAppendEntries(peer int, arg *AppendEntriesArg) bool {

	reply := &AppendEntriesReply{}

	// todo if over a half of the broadcast is lost, peers won't know I'm the new leader , should I start a new round vote?
	ok := rf.sendAppendEntries(peer, arg, reply)
	DPrintf("[%d]sendAppendEntries --> peer:%d, arg:%+v, reply:%+v, ok:%t", rf.me, peer, arg, reply, ok)

	if !ok {
		return false
	}

	// someone get a larger Term in the same time, give up for this Term.
	// the peer may be broadcasting to me later.
	if arg.Term < reply.Term {
		return false
	}

	return true
}

func (rf *Raft) delSince(idx int) {
	//rf.mu.Lock()
	//rf.mu.Unlock()
	rf.log = rf.log[:idx]
	rf.commitIndex = idx
	if idx == 0 {
		rf.currentTerm = 0
	} else {
		rf.currentTerm = rf.log[idx-1].term
	}
}

func (rf *Raft) initNextIndex(oldLeaderId int) {
	if oldLeaderId != rf.me {
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	// see GetState()
	rf.voteForId = NoneCandidateId // candidateId start from 0 to N, default voteForId must be out of it.
	rf.log = make([]*LogEntry, 1)  // valid log index starts from 1, pay attention!
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// todo when a new leader replaced the old one, we should init nextIndex/matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.lastApplied = 0 //todo

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
