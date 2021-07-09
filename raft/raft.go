// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// Randomized election timeout
	randElectionTimeout int
}

// Util for cleanup stale raft state information
func (r *Raft) cleanState(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandElectionTimeout()
	r.State = StateFollower
	r.votes = map[uint64]bool{}

	for pr, prg := range r.Prs {
		prg.Match = 0
		prg.Next = r.RaftLog.LastIndex() + 1
		if pr == r.id {
			prg.Match = r.RaftLog.LastIndex()
		}
	}
	r.resetRandElectionTimeout()
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	rand.Seed(time.Now().UnixNano())

	prs := map[uint64]*Progress{}


	for _, v := range c.peers {
		prs[v] = &Progress{}
	}

	return &Raft{
		id:                  c.ID,
		RaftLog:             newLog(c.Storage),
		Prs:                 prs,
		State:               StateFollower,
		votes:               map[uint64]bool{},
		msgs:                []pb.Message{},
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		randElectionTimeout: c.ElectionTick * rand.Intn(c.ElectionTick),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
    if pr.Next < r.RaftLog.FirstIndex() {
        return false
    }
    /*
     *term, errt := r.RaftLog.Term(pr.Next - 1)
     */
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	log.Infof("Send heartbeat to:%d", to)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) resetRandElectionTimeout() {
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randElectionTimeout {
			r.becomeCandidate()
			r.campain()
		}
	case StateCandidate:
		r.electionElapsed++
		log.Debugf("elapsed: %d, timeout: %d, term: %d", r.electionElapsed, r.randElectionTimeout, r.Term)
		// If electionTimeout elapses: start new campain
		if r.electionElapsed >= r.randElectionTimeout {
			r.becomeCandidate()
			r.campain()
		}
	case StateLeader:
		r.heartbeatElapsed++
		r.electionElapsed++
		for prs := range r.Prs {
			if prs == r.id {
				continue
			}
			r.sendHeartbeat(prs)
		}
	}
}

// becomeFollower transform this peer's state to Follower
/*
* Respond to rpcs from candidates and leaders
*  appendEntries
*  requestVote
*  heartBeat (since this implementation separate out appendEntry and heartbeat)
*
* Elaction timeout without getting either AppendEntries nor RequestVote rpcs, become candidate
* */
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.cleanState(term)
	r.Lead = lead
	log.Debugf("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Handle start election
	// Increment current term
	// vote for self
	// reset election timer
	// send request vote rpcs to all other servers
	log.Debugf("%x becoming candidate", r.id)
	r.cleanState(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
}

// becomeLeader transform this peer's state to leader
// Send inital empty AppendEntries (heartbeat) rpc
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	log.Debugf("%x becoming leader", r.id)
	r.cleanState(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	r.leaderAppendEntries(&pb.Entry{})
}

func (r *Raft) leaderAppendEntries(entries ...*pb.Entry) {
    idx := r.RaftLog.LastIndex()
    for i := range entries {
        entries[i].Term = r.Term
        entries[i].Index = idx + uint64(i) + 1
    }
    r.RaftLog.append(entriesToValue(entries)...)
    r.Prs[r.id].Match = entries[len(entries) - 1].Index
    r.Prs[r.id].Next = r.Prs[r.id].Match + 1


}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	// If RPC request or response contains term > current term, set current term == term
	// convert to follower
	// Follower can receive 3 type of rpcs:
	// 1. RequestVote
	// 2. AppendEntries
	// 3. HeartBeat
	if r.Term > m.Term {
		log.Errorf("Invalid state for node: %d, request term: %d, current term: %d", r.id, m.GetTerm(), r.Term)

		return nil
	}
	log.Debugf("elapsed: %d, timeout: %d", r.electionElapsed, r.randElectionTimeout)

    /*
	 *if r.electionElapsed >= r.randElectionTimeout {
     *    log.Info("become candidate again")
	 *    r.becomeCandidate()
	 *}
     */

	// return false if log doesn't contain an entry at preLogIndex whose
	// term matches preLogTerm

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.campain()
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
	case pb.MessageType_MsgAppend:
		r.Lead = m.From
		r.Term = m.Term
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.GetFrom()
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgPropose,
			To:      r.Lead,
			From:    m.From,
		})
	}

	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campain()
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
		/*
		 *if r.Term > m.GetTerm() {
		 *    r.msgs = append(r.msgs, pb.Message{
		 *        MsgType: pb.MessageType_MsgRequestVoteResponse,
		 *        Reject:  true,
		 *    })
		 *} else {
		 *    // Got term greater, become follower
		 *    r.becomeFollower(m.GetTerm(), m.GetFrom())
		 *}
		 */
	case pb.MessageType_MsgRequestVoteResponse:
		// calc votes
		// If votes received from majority of servers: become leader
		if m.Reject {
			log.Infof("%x rejected vote for %d", m.From, r.id)
		}
		r.votes[m.From] = !m.Reject

        /*
		 *log.Infof("StepCandidate count votes")
         */
		supportive, denial, votesToWin := r.countVotes()
		if supportive >= votesToWin {
			r.becomeLeader()
			r.bcastAppend(m)
		} else if denial >= votesToWin {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	if r.Term < m.Term {
		r.Term = m.Term
		r.becomeFollower(m.Term, m.From)
	}
    log.Infof("%v, %d", r.Prs, m.From)
	pr := r.Prs[m.From]
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartBeat()
		return nil
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
		return nil
	case pb.MessageType_MsgPropose:
		// append entries to log
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgPropose", r.id)
		}
		r.handleAppendEntries(m)
		r.bcastAppend(m)
		return nil
	case pb.MessageType_MsgAppend:
		r.bcastAppend(m)
		return nil
	case pb.MessageType_MsgAppendResponse:
        log.Infof("%v, %v", pr, m)
        if pr == nil {
            log.Errorf("Node not in record: %d, %v", m.From, pr)
            return nil
        }
		pr.Match = m.Index
		if m.Reject {
			pr.Next--
		} else {
			pr.Next = m.Index + 1
		}

		if pr.Next < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
		return nil
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
		return nil
	}
	return nil
}

func (r *Raft) bcastAppend(m pb.Message) {
	// call sendAppend
	for prs := range r.Prs {
		if prs == r.id {
			continue
		}
		r.sendAppend(prs)
	}
}

func (r *Raft) bcastHeartBeat() {
	for prs := range r.Prs {
		if prs == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      prs,
			Term:    r.Term,
		})
	}
}

func (r *Raft) campain() {
	r.Vote = r.id
	r.votes[r.id] = true

    /*
	 *log.Infof("%x starting campain for term %d", r.id, r.Term)
     */
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

    /*
	 *log.Infof("%v, %d", r.Prs, r.Term)
     */
	for prs := range r.Prs {
		if prs == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      prs,
			Term:    r.Term,
			From:    r.id,
			LogTerm: logTerm,
			Index:   r.RaftLog.LastIndex(),
		})
	}
    /*
	 *log.Infof("Campain count votes")
     */
	supportive, _, votesToWin := r.countVotes()
	if supportive >= votesToWin {
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.electionElapsed = 0
	last, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, entriesToValue(m.Entries)...)
	if !ok {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Reject:  true,
		})
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   last,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
	})
}

func (r *Raft) handleVote(m pb.Message) {
	// Handle RequestVote rpc
	// if votedFor is null or candidateId and candidate's log is at least as up-to-date
	// as receiver's log, grant vote
	// How to verify second part?
	reject := false

	if m.Term < r.Term || m.Term == r.Term && (r.Vote != None && r.Vote != m.From ||
		r.Lead != None) ||
		!r.candidateIsUpToDate(m.LogTerm, m.Index) {
		reject = true
	} else {
		r.Term = m.Term
		r.Vote = m.From
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) candidateIsUpToDate(term, index uint64) bool {
	lastTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		log.Panicf("error getting last term: %v", err)
	}
	return term > lastTerm || (term == lastTerm && index >= r.RaftLog.LastIndex())

}

func (r *Raft) countVotes() (int, int, int) {
	// calc votes
	votesToWin := len(r.Prs)/2 + 1
	supportive := 0
	for pr := range r.Prs {
		if r.votes[pr] {
			supportive += 1
		}
	}
    /*
	 *log.Infof("count votes: %d, %d, %d", votesToWin, supportive, votesToWin-supportive)
     */

	return supportive, votesToWin - supportive, votesToWin
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func entriesToValue(ptr []*pb.Entry) []pb.Entry {
	r := make([]pb.Entry, len(ptr))
	for i, e := range ptr {
		r[i] = *e
	}
	return r
}
