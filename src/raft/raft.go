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
	"fmt"
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
	voteState VoteState
	state     State
	peerNum   int
	log       LogStorage
}

type LogStorage struct {
	mu           sync.Mutex
	log          []Log
	commitIndex  int
	lastAplied   int
	lastLogIndex int
	lastLogTerm  int
	nextIndex    []int
	matchIndex   []int
}

type Log struct {
	term  int
	index int
	store interface{}
}

// append log and return log No
func (rf *Raft) appendLog(log interface{}) (index, term int) {
	rf.log.mu.Lock()
	defer rf.log.mu.Unlock()
	currTerm := rf.getCurrentTerm()
	rf.log.lastLogIndex++
	rf.log.lastLogTerm = currTerm
	rf.log.log = append(rf.log.log, Log{term: currTerm, store: log, index: rf.log.lastLogIndex})
	return rf.log.lastLogIndex, rf.log.lastLogTerm
}

// indepent locked voteState
type VoteState struct {
	count    int
	votedFor int
	mu       sync.Mutex
}

func (rf *Raft) newVote() {
	rf.voteState.mu.Lock()
	defer rf.voteState.mu.Unlock()
	rf.voteState.count++
}

func (rf *Raft) getVote() int {
	rf.voteState.mu.Lock()
	defer rf.voteState.mu.Unlock()
	return rf.voteState.count
}

func (rf *Raft) voteFor(n int) {
	rf.voteState.mu.Lock()
	defer rf.voteState.mu.Unlock()
	rf.voteState.votedFor = n
	if n == rf.me {
		rf.voteState.count++
	}
}

func (rf *Raft) getVotedFor() int {
	rf.voteState.mu.Lock()
	defer rf.voteState.mu.Unlock()
	return rf.voteState.votedFor
}

// independent mu-shared State
type State struct {
	mu                    sync.Mutex
	code                  int
	currentTerm           int
	haveHeartBeat         bool
	hasSendGoroutineLunch []bool
}

// return state 0 as fo, 1 as candi, 2 as leader
func (rf *Raft) getState() int {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	return rf.state.code
}

// set state
func (rf *Raft) setState(newCode int) {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	rf.state.code = newCode
}

func (rf *Raft) changeTheStateToLeader() {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	rf.state.hasSendGoroutineLunch = make([]bool, rf.peerNum)
	rf.state.code = 2
}

func (rf *Raft) setHeartBeatState(st bool) {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	rf.state.haveHeartBeat = st
}

func (rf *Raft) getHeartBeatState() bool {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	return rf.state.haveHeartBeat
}

func (rf *Raft) getCurrentTerm() int {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	return rf.state.currentTerm
}

// also implemtnet rule 2, bigger means follower
func (rf *Raft) setCurrentTerm(t int) bool {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	fmt.Printf("Changing Current Term for Node %v, ori %v, target %v \n", rf.me, rf.state.currentTerm, t)
	if t < rf.state.currentTerm {
		fmt.Printf("Error: Smaller term \n")
		return false
	}
	if t > rf.state.currentTerm { // figure2 allserver rule 2
		fmt.Printf("Downgrading: Bigger term, be FO \n")
		rf.state.code = 0

	}
	rf.state.currentTerm = t
	return true
}

func (rf *Raft) incremnetCurrentTerm() int {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	rf.state.currentTerm++
	return rf.state.currentTerm
}

func (rf *Raft) getFollowerSendGoroutineState(n int) bool {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	return rf.state.hasSendGoroutineLunch[n]
}

func (rf *Raft) setFollowerSendGoroutineState(n int, b bool) {
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	rf.state.hasSendGoroutineLunch[n] = b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.state.mu.Lock()
	defer rf.state.mu.Unlock()
	return rf.state.currentTerm, rf.state.code == 2
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.Term > rf.getCurrentTerm() { // STATE CHANGE 6-B a higher term
		fmt.Printf("Election: Node %v is receiving Vote Req from Node %v\n", rf.me, args.CandidateId)
		rf.setHeartBeatState(true)
		rf.setCurrentTerm(args.Term) // once receive a candidate has larger term, be a follower
		rf.voteFor(args.CandidateId)
		reply.VoteGranted = true
		fmt.Println(rf.me, "vote for", args.CandidateId)
	}
	reply.Term = rf.getCurrentTerm()
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

// Simple Send without rerty!!!
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) beCandadite() {
	runningTerm := rf.incremnetCurrentTerm() // perevent term get change by leader during a running, as only after send all reqVote to count the vote
	fmt.Printf("BeCandaite: Node %v is running for term %v \n", rf.me, runningTerm)
	rf.setState(1)             // be candidate
	rf.voteState = VoteState{} // new running, new voteCount! (old vote will not count)
	rf.voteFor(rf.me)          // Vote count for oneself is also down is this func

	// Request other nodes to vote and count the votes
	for id := 0; id < rf.peerNum; id++ {
		if id != rf.me && rf.getState() == 1 {
			go func(id int) {
				fmt.Printf("Candaite: Node %v is asking %v for a vote \n", rf.me, id)
				ok := false
				reply := &RequestVoteReply{}
				request := &RequestVoteArgs{
					CandidateId: rf.me, Term: runningTerm,
				}
				for !ok && rf.getState() == 1 {
					ok = rf.sendRequestVote(id, request, reply)
					fmt.Printf("Retrying SendRequestVote: Node %v think it unsuccessfullly send to Node %v \n", rf.me, id)
				}
				if reply.VoteGranted {
					fmt.Printf("Candaite: Node %v get a vote! \n", rf.me)
					rf.newVote()
					fmt.Printf("Candaite: Node %v now vote number: %v, Majority is %v \n", rf.me, rf.getVote(), len(rf.peers)/2)
					if rf.getState() == 1 && rf.getVote() > len(rf.peers)/2 { // STATE CHANGE 4
						rf.changeTheStateToLeader()
						fmt.Printf("Candaite: Node %v now thinging it is the leader!, Term %v\n", rf.me, rf.getCurrentTerm())
						rf.doLeaderJob()
						return
					}
				}
				if reply.Term > rf.getCurrentTerm() {
					rf.setCurrentTerm(reply.Term)
				}
			}(id)
		}
	}
	// STATE CHANGE 4 Candi to Lead or 3 new Elec be cand or 6 find a Leader
	// Check if vote pass a half
	// for rf.isCandidate { // STATE CHANGE 3,6
	// 	if rf.voteCount.getVote() > len(rf.peers)/2 { // STATE CHANGE 4
	// 		rf.isCandidate, rf.isLeader = false, true
	// 	}

	// }

}

// AppendEntries Request
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// PrevLogIndex int
	// PrevLogTerm  int
	// entries      []string
	// LeaderCommit int
}

// AppendEntries Reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term >= rf.getCurrentTerm() { // STATE CHANGE 5,6 A leader or a highger term
		rf.setState(0)
		rf.setHeartBeatState(true)
		reply.Term = args.Term
		reply.Success = true
		if args.Term > rf.getCurrentTerm() {
			rf.setCurrentTerm(args.Term) // term sync
			fmt.Printf("Follower: Node %v is receving AppendEntries, updating current term \n", rf.me)
		}
		fmt.Printf("Follower: Node %v is receving AppendEntries, current term is %v \n", rf.me, rf.getCurrentTerm())
		return
	}
	reply.Term = rf.getCurrentTerm()
	reply.Success = false
}

// AppendEntries BoradCaster
// Add retry with controlable timeout
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Println("Leader Send: Node", rf.me, "is sending AppendEntries to Node", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// DoSend AppendEntries
func (rf *Raft) doLeaderJob() {
	for id := 0; id < rf.peerNum; id++ {
		if id != rf.me && !rf.getFollowerSendGoroutineState(id) {
			fmt.Printf("Leader HB: Node %v thinking its goroutine to send HB to Node %v is not running, Starting \n", rf.me, id)
			rf.setFollowerSendGoroutineState(id, true)
			go func(id int) {
				defer rf.setFollowerSendGoroutineState(id, false)
				args := &AppendEntriesArgs{
					LeaderId: rf.me, Term: rf.getCurrentTerm(),
				}
				reply := &AppendEntriesReply{}
				for ok := rf.sendAppendEntries(id, args, reply); !ok; {
					ok = rf.sendAppendEntries(id, args, reply)
					fmt.Printf("Retrying SendAppendEntries: Node %v think it unsuccessfullly send to Node %v \n", rf.me, id)
				}
				fmt.Printf("Leader HB: Node %v think it successfullly send to Node %v, Reply is %v, reply.term: %v \n", rf.me, id, reply.Success, reply.Term)
				if reply.Term != 0 && (!reply.Success || reply.Term > rf.getCurrentTerm()) { //STATE CHANGE 5
					fmt.Printf("Downgrading: Receving false reply or higher Term %v, Node %v (currentTerm %v) is downgrading to Follower \n", reply.Term, rf.me, rf.getCurrentTerm())
					rf.setCurrentTerm(reply.Term) //+ALL server rule 2
				}

			}(id)
		}
	}
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
	if rf.getState() == 2 {
		// TODO: may need some mutex to handel concurrent
		index, term = rf.appendLog(command)
		go rf.doLeaderJob()
		isLeader = true
	}

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
		// Relection timeout
		// pause for a random amount of time between 500 and 1000
		// milliseconds.
		ms := 500 + (rand.Int63n(500))
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (3A)
		// Check if a leader election should be started.
		// Paper: broadcast time should be an order of magnitude less than the election timeout
		// STATE CHANGE 2,3 be Candi
		if !rf.getHeartBeatState() && rf.getState() != 2 {
			fmt.Printf("Election: Leader timeout, %v be Candidate\n", rf.me)
			rf.setState(0) // STATE CHANGE 3, Candi to Candi, set to false to end last elect cycle, see STATE CHANGE 5
			go rf.beCandadite()
		}
		rf.setHeartBeatState(false)
	}
}

func (rf *Raft) tickerLeader() {
	for rf.killed() == false {
		// Do leader's send AppendEntries job
		if rf.getState() == 2 {
			rf.doLeaderJob()
		}

		// pause for a random amount of time between 100 and 150
		// milliseconds.
		ms := 100 + (rand.Int63n(50))
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.voteState = VoteState{}
	rf.state = State{}
	rf.peerNum = len(rf.peers)
	rf.log = LogStorage{}
	rf.log.log = make([]Log, 0)
	rf.state.hasSendGoroutineLunch = make([]bool, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.tickerLeader()

	return rf
}
