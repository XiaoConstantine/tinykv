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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	return &Raft{
		id:      c.ID,
		RaftLog: newLog(c.Storage),
		Prs:     map[uint64]*Progress{},
		State:   StateFollower,
		votes:   map[uint64]bool{},
		msgs:    []pb.Message{},
		// random number
		heartbeatTimeout: 1,
		electionTimeout:  10,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateCandidate:
		r.electionElapsed += 1
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
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
	// respond to rpcs from candidates and leaders
	// appendEntries
	// requestVote
	if term < r.Term {
		log.Errorf("Invalid state for node: %d, request term: %d, current term: %d", r.id, term,
			r.Term)
		return
	}
	if r.electionElapsed >= r.electionTimeout {
		// timeout getting rpcs, become candidate
		r.becomeCandidate()
	}
	// Handle RequestVote rpc
	// if votedFor is null or candidateId and candidate's log is at least as up-to-date
	// as receiver's log, grant vote
	// How to verify second part?
	if r.Vote == 0 || lead != 0 {
		if r.votes == nil {
			r.votes = make(map[uint64]bool)
		}
		r.Vote = lead
		r.votes[lead] = true
	}
	// Handle AppendEntries rpc
	// reply false if log doesn't contain an entry at preLogIndex ?
	// if an existing entry conflicts with a new one (same index but different terms), delete
	// existing entry and all that follow it ?
	// append any new entries not already in the log
	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// Do nothing

	// Handle HeartBeat rpc
	// Reset electionElapsed to avoid a new election
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Handle start election
	// Increment current term
	// vote for self
	// reset election timer
	// send request vote rpcs to all other servers
	r.Term += 1
	r.Vote += 1
	if r.votes == nil {
		r.votes = make(map[uint64]bool)
	}
	r.votes[r.id] = true
	r.electionElapsed = 0
	// Where to find all the servers?
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		// last log term & last log index
	})

	// Hanlde requestVote repsonse
	// If votes received from majority of servers: become leader
	totalNodes := len(r.votes)
	supportive := 0
	for _, v := range r.votes {
		if v == true {
			supportive += 1
		}
	}
	if supportive > (totalNodes / 2) {
		r.becomeLeader()
	}

	// Handle AppendEntries rpc
	// ?

	// if election timeout: starts a new election
	if r.electionElapsed >= r.electionTimeout {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

// becomeLeader transform this peer's state to leader
// Send inital empty AppendEntries (heartbeat) rpc
//
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		From:    r.id,
		Term:    r.Term,
	})
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
/**
 * Follower:
 *   electionTimeout tick
 *   heart beat tick
 *   if electionTimeout:
 *      transit => Candidate
 *   rpc: HeartBeatReq & AppendEntries
 *          heartbeatTimeout reset & append log entry
 *        RequestVote(req: RequestVote)
 *          if term < req.term => vote for requester
 *                                update term == req.term
 *                                reset electionTimeout = 0
 * Candidate:
 *   term++
 *   vote++(self vote)
 *   rpc: RequestVote(req: RequestVote)
 *
 * Leader:
 *   rpc: AppendEntries to followers
 *        HeartBeatReq
 *
 * */
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
	// return false if log doesn't contain an entry at preLogIndex whose
	// term matches preLogTerm

	r.Term = m.Term
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgRequestVote:
		// Handle RequestVote rpc
		// if votedFor is null or candidateId and candidate's log is at least as up-to-date
		// as receiver's log, grant vote
		// How to verify second part?
		if r.Vote == 0 || m.GetFrom() != 0 {
			if r.votes == nil {
				r.votes = make(map[uint64]bool)
			}
			// verify request log is up-to-date
			r.votes[m.GetFrom()] = true
			r.Vote = m.From
		}
	case pb.MessageType_MsgAppend:
		t, err := r.RaftLog.Term(m.GetIndex())
		if err != nil {
			return err
		}
		if t != m.Term {
			// unset entry after index
			r.RaftLog.entries = r.RaftLog.entries[:m.GetIndex()]
		}
		// Append any new entries not already in the log

		// if learderCommit > commitIndex
		// commitIndex = min(leadercommit, index of last new entry)
		if m.GetCommit() > r.RaftLog.committed {
			//TODO  index of last new entry is wrong
			idxLastNewEntry := uint64(len(r.RaftLog.entries) - 1)
			r.RaftLog.committed = min(m.GetCommit(), idxLastNewEntry)
		}
	case pb.MessageType_MsgHeartbeat:
		// reset electionElapsed
		r.Lead = m.GetFrom()
		r.electionElapsed = 0
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
	if r.Term > m.Term {
		log.Errorf("Invalid state for node: %d, request term: %d, current term: %d", r.id, m.GetTerm(), r.Term)

		return nil
	}
	r.Term++
	r.Vote = r.id

	switch m.MsgType {
	case pb.MessageType_MsgAppend:
        r.becomeFollower(m.Term, m.From)
        r.msgs = append(r.msgs, pb.Message{
            MsgType: pb.MessageType_MsgAppendResponse,
        })
    case pb.MessageType_MsgAppendResponse:
        r.handleAppendEntries(m)



	}

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
