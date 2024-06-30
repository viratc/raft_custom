"""
## raftserver.py

- Idea: Make the Raft Server *ONLY* respond to messages.  The servers communicate
  with each other using messages.  Maybe everything (including client interaction)
- Could be handled in the form of a "message" of some kind.-
- TODO:
    - Look at "Actor Model".
    - Objects only respond to messages. Only send messages.

@author: Virat Chourasia
@version: 0.5, 10/04/2022
"""


from time import time
from raftlog import append_entries, LogEntry



class Message:
    pass

# -- Internal messages. Sent to a server by the runtime, but not on the actual network.

class ClientLogAppend(Message):
    # Message sent by an application (e.g., KV server) to the Raft leader that
    # asks it to add a new entry to its log.
    def __init__(self, value, future=None):
        self.value = value
        self.future = future

    def __repr__(self):
        return f'ClientLogAppend({self.value}, {self.future})'

class HeartBeat(Message):
    pass

class ElectionTimeout(Message):
    pass

# -- Server messages.  Sent between machines.

class AppendEntries(Message):
    def __init__(self, source, dest, term, prev_index, prev_term, entries, leader_commit=-1):
        self.source = source
        self.dest = dest
        self.term = term
        self.prev_index = prev_index
        self.prev_term = prev_term
        self.entries = entries
        self.leader_commit = leader_commit

    def __repr__(self):
        return f'AppendEntries({self.source}, {self.dest}, {self.term}, {self.prev_index}, {self.prev_term}, {self.entries}, {self.leader_commit})'

class AppendEntriesResponse(Message):
    def __init__(self, source, dest, term, success, match_index):
        self.source = source
        self.dest = dest
        self.term = term
        self.success = success
        self.match_index = match_index    # <<< Needs to be added (Missing in Figure 2)

    def __repr__(self):
        return f'AppendEntriesResponse({self.source}, {self.dest}, {self.term}, {self.success}, {self.match_index})'

class RequestVote(Message):
    def __init__(self, source, dest, term, last_log_index, last_log_term):
        self.source = source
        self.dest = dest
        self.term = term
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def __repr__(self):
        return f'RequestVote({self.source}, {self.dest}, {self.term}, {self.last_log_index}, {self.last_log_term})'

class RequestVoteResponse(Message):
    def __init__(self, source, dest, term, vote_granted):
        self.source = source
        self.dest = dest
        self.term = term
        self.vote_granted = vote_granted

    def __repr__(self):
        return f'RequestVoteResponse({self.source}, {self.dest}, {self.term}, {self.vote_granted})'

class RaftServer:
    def __init__(self, address, numpeers):
        # Network topology (my own address and other peers in the network)
        self.address = address
        self.peers = [ n for n in range(numpeers) if n != address ]

        # Raft state variables
        self.role = 'FOLLOWER'     # Maybe 'LEADER', 'CANDIDATE'
        self.log = [ ]
        self.current_term = 1      # Included in EVERY message sent
        self.commit_index = -1     # Index of highest committed entry (replicated on a majority)
        self.last_applied = -1     # Index of highest "applied" entry (given to application)

        # Where the log entry goes on each follower.  Initialized by leaders
        self.next_index = { }

        # How much of the log is known to match leader on each follower.
        self.match_index = { }

        # Leader election
        self.voted_for = None
        self.votes_granted = None

    def __repr__(self):
        return f'RaftServer<{self.address}, {self.role}, term={self.current_term}, log={self.log}>'

    # Make the Raft server "comparable".   Might be useful for testing/simulation
    def __eq__(self, other):
        return isinstance(other, RaftServer) and self.__dict__ == other.__dict__



    # Internal helper function to initialize variables on role changes
    def _become_leader(self):
        print(f"{self.address} became leader")
        self.role = 'LEADER'
        self.next_index = { peer: len(self.log) for peer in self.peers }
        self.match_index = { peer: -1 for peer in self.peers }
        # print("end_time:", time())

    def _become_candidate(self):
        print(f"{self.address} became candidate")
        self.role = 'CANDIDATE'
        self.current_term += 1
        self.votes_granted = { peer: False for peer in self.peers }
        # print("end_time:", time())

    def _become_follower(self):
        print(f"{self.address} became follower")
        self.role = 'FOLLOWER'
        # print("end_time:", time())


    # Handles a single message. Returns a list of outgoing messages that should be
    # sent elsewhere (if any)
    def handle_message(self, msg:Message):

        # Internal messages.  Used by the runtime to interact directly with the server.
        # Not sent over the actual network.
        if isinstance(msg, ClientLogAppend):
            response = self._handle_client_log_append(msg)
        elif isinstance(msg, HeartBeat):
            response = self._handle_heartbeat()
        elif isinstance(msg, ElectionTimeout):
            response = self._handle_election_timeout()

        else:
            # Network messages.  Sent between Raft servers.

            # Get a message with a lower term than myself. Ignore (out of date)
            # Enhancement: Could reply with a message to inform sender of new term
            if msg.term and msg.term < self.current_term:
                return [ ]

            # Get a message with a higher term than myself. Become follower.
            if msg.term and msg.term > self.current_term:
                self.current_term = msg.term
                self._become_follower()
                self.voted_for = None

            if isinstance(msg, AppendEntries):
                response = self._handle_append_entries(msg)
            elif isinstance(msg, AppendEntriesResponse):
                response = self._handle_append_entries_response(msg)
            elif isinstance(msg, RequestVote):
                response = self._handle_request_vote(msg)
            elif isinstance(msg, RequestVoteResponse):
                response = self._handle_request_vote_response(msg)
            else:
                raise RuntimeError(f'Unknown message {msg}')

        return response if response else [ ]

    # Application is making a request to add an entry to the leader log
    def _handle_client_log_append(self, msg:ClientLogAppend):
        assert self.role == 'LEADER', self.role
        entry = LogEntry(self.current_term, msg.value)
        prev_index = len(self.log) - 1
        prev_term = self.log[prev_index].term if prev_index >= 0 else -1
        assert append_entries(self.log, prev_index, prev_term, [entry])

    # Heartbeat causes the leader to update all followers
    def _handle_heartbeat(self):
        if self.role != 'LEADER':
            return

        outgoing = [ ]
        for follower in self.next_index:
            prev_index = self.next_index[follower] - 1
            prev_term = self.log[prev_index].term if prev_index >= 0 else -1
            entries = self.log[prev_index+1:]
            outgoing.append(
                AppendEntries(
                    source=self.address,
                    dest=follower,
                    term=self.current_term,
                    prev_index=prev_index,
                    prev_term=prev_term,
                    entries=entries,
                    leader_commit=self.commit_index
                    )
                )
        return outgoing

    # A leader is telling a follower to add an entry to its log
    def _handle_append_entries(self, msg:AppendEntries):
        # There is a chance that as candidate, we get an AppendEntries message from a new
        # leader with the same term number (would happen if another server called an election
        # in the same term and won the election). In this case, we revert to follower and
        # accept the AppendEntries.
        if self.role == 'CANDIDATE':
            self._become_follower()
        success = append_entries(self.log, msg.prev_index, msg.prev_term, msg.entries)
        if success:
            # Leader tells the follower its commit index.  If greater than the follower's,
            # the follower updates its own value (with the caveat that it can't be
            # longer than our own log)
            new_commit_index = min(len(self.log)-1, msg.leader_commit)
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index

        return [
            AppendEntriesResponse(
                source=msg.dest,
                dest=msg.source,
                term=self.current_term,
                success=success,
                match_index=msg.prev_index + len(msg.entries),  # See TLA Spec. Line 368.
                )]

    # A Follower is telling the leader if a log update worked or not
    def _handle_append_entries_response(self, msg:AppendEntriesResponse):
        if self.role != 'LEADER':
            return

        if msg.success:
            # It worked
            self.next_index[msg.source] = msg.match_index + 1
            # Raft paper describes "match_index" as "increasing monotonically".
            if msg.match_index > self.match_index[msg.source]:
                self.match_index[msg.source] = msg.match_index

                # Determine consensus here.... sort the match_indices, pick the median
                match_indices = sorted(self.match_index.values())
                new_commit_index = match_indices[len(match_indices)//2]   # The median
                if new_commit_index > self.commit_index:
                    self.commit_index = new_commit_index
                    # Discussion: self.last_applied tracks the last log entry
                    # applied to the application program.   I've decided to leave it
                    # untouched here.  The outer runtime that feeds messages to
                    # the Raft server should look at it to see if it can be
                    # advanced.

        else:
            # It failed. Back up by one position.
            # Q: Do we immediately try sending an updated message to the follower or do
            #    we wait until the next heartbeat to do it?
            self.next_index[msg.source] -= 1

    def _handle_election_timeout(self):
        if self.role == 'LEADER':
            return
        self._become_candidate()
        self.voted_for = self.address     # Vote for myself
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index].term if last_log_index >= 0 else -1
        outgoing = [
            RequestVote(
                source=self.address,
                dest=follower,
                term=self.current_term,
                last_log_term=last_log_term,
                last_log_index=last_log_index
                )
            for follower in self.peers ]
        return outgoing

    # Message from candidate
    def _handle_request_vote(self, msg):
        # Very careful reading of section 5.4.1 of the paper.
        # Vote is only granted if the candidate's log is at least as up to date as our log.
        # "Raft determines which of two logs is more up-to-date by comparing the
        #  index and term of the last entries in the logs.  If the logs have last entries
        #  with different terms, then the log with the later term is more up to date. If
        #  the logs end with the same term, then whichever log is longer is more up-to-date."
        my_last_log_index = len(self.log) - 1
        my_last_log_term = self.log[my_last_log_index].term if my_last_log_index >=0 else -1

        # Can only vote once in a given term
        vote_granted = self.voted_for is None or self.voted_for == msg.source
        if my_last_log_term > msg.last_log_term:
            vote_granted = False
        elif (my_last_log_term == msg.last_log_term) and \
             (my_last_log_index > msg.last_log_index):
            vote_granted = False

        if vote_granted:
            self.voted_for = msg.source

        return [ RequestVoteResponse(
                    source=msg.dest,
                    dest=msg.source,
                    term=self.current_term,
                    vote_granted = vote_granted) ]

    # Message indicating if vote granted or not
    def _handle_request_vote_response(self, msg):
        if self.role == 'FOLLOWER':
            return

        # Note: Allow votes to be collected even when leader.  Note used for anything
        # except testing.
        self.votes_granted[msg.source] = msg.vote_granted
        # Have we won enough votes?
        if self.role == 'CANDIDATE' and sum(self.votes_granted.values()) >= len(self.votes_granted) // 2:
            # Yes. Become leader
            self._become_leader()

            # On becoming leader, initiate a heartbeat to assert leadership
            return self._handle_heartbeat()


# ----- Tests

# Thought: Could FakeCluster be used as basis for a simulator? Some kind of verification
# similar to TLA+.  Complexity: What are you verifying?  (Don't lose log entries already committed).

class FakeCluster:
    # Represent a cluster of Raft nodes
    def __init__(self, numnodes):
        self.nodes = [ RaftServer(n, numnodes) for n in range(numnodes) ]

    # Inject a message into the cluster and deliver all messages that get generated
    # until nothing else is happening.
    def send_message(self, msg, dest=None):
        from collections import deque
        messages = deque([(msg, dest)])
        while messages:
            msg, dest =  messages.popleft()
            if dest is None:
                dest = msg.dest
            messages.extend((msg, msg.dest) for msg in self.nodes[dest].handle_message(msg))

def test_raft_server():
    # Add a new entry to the leader
    server = RaftServer(0, 3)
    server.role = 'LEADER'
    server.handle_message(ClientLogAppend('hello'))
    assert server.log == [ LogEntry(server.current_term, 'hello') ], server.log

    # Receive an AppendEntriesMessage
    server = RaftServer(0, 3)
    server.role = 'FOLLOWER'
    result = server.handle_message(AppendEntries(
        source=0,
        dest=1,
        term=1,
        prev_index=-1,
        prev_term=-1,
        entries=[LogEntry(1, 'hello') ]))
    assert server.log == [ LogEntry(1, 'hello') ], server.log
    assert isinstance(result[0], AppendEntriesResponse), result
    assert result[0].dest == 0
    assert result[0].source == 1
    assert result[0].success == True

    print("Good server")

# Set up a cluster of machines that look like Figure 6 in Raft Paper
def create_figure_6():
    cluster = FakeCluster(5)
    cluster.nodes[0].log = [LogEntry(1, 'x<3'), LogEntry(1, 'y<1'), LogEntry(1, 'y<9'),
                            LogEntry(2, 'x<2'), LogEntry(3, 'x<0'), LogEntry(3, 'y<7'),
                            LogEntry(3, 'x<5'), LogEntry(3, 'x<4')]
    cluster.nodes[1].log = [LogEntry(1, 'x<3'), LogEntry(1, 'y<1'), LogEntry(1, 'y<9'),
                            LogEntry(2, 'x<2'), LogEntry(3, 'x<0')]
    cluster.nodes[2].log = [LogEntry(1, 'x<3'), LogEntry(1, 'y<1'), LogEntry(1, 'y<9'),
                            LogEntry(2, 'x<2'), LogEntry(3, 'x<0'), LogEntry(3, 'y<7'),
                            LogEntry(3, 'x<5'), LogEntry(3, 'x<4')]
    cluster.nodes[3].log = [LogEntry(1, 'x<3'), LogEntry(1, 'y<1')]
    cluster.nodes[4].log = [LogEntry(1, 'x<3'), LogEntry(1, 'y<1'), LogEntry(1, 'y<9'),
                            LogEntry(2, 'x<2'), LogEntry(3, 'x<0'), LogEntry(3, 'y<7'),
                            LogEntry(3, 'x<5')]
    for node in cluster.nodes:
        node.current_term = 3
    cluster.nodes[0]._become_leader()
    return cluster

def test_figure_6():
    cluster = create_figure_6()
    # Run 10 heartbeats (that number picked arbitrarily)
    for _ in range(10):
        cluster.send_message(HeartBeat(), dest=0)

    # See if the logs are all the same as leader
    assert all(node.log == cluster.nodes[0].log for node in cluster.nodes), cluster.nodes

    print('Good figure 6')
    return cluster

def test_election():
    cluster = create_figure_6()
    cluster.nodes[0]._become_follower()

    # Test who would win elections or not
    cluster.send_message(ElectionTimeout(), dest=0)
    assert cluster.nodes[0].role == 'LEADER'
    assert cluster.nodes[0].votes_granted == { 1: True, 2: True, 3: True, 4: True }, cluster.nodes[0].votes_granted

    cluster.send_message(ElectionTimeout(), dest=1)
    assert cluster.nodes[1].role == 'CANDIDATE'     # NO. Not enough log entries
    assert cluster.nodes[1].votes_granted == { 0: False, 2: False, 3: True, 4: False }, cluster.nodes[1].votes_granted

    cluster.send_message(ElectionTimeout(), dest=2)
    assert cluster.nodes[2].role == 'LEADER'
    assert cluster.nodes[2].votes_granted == { 0: True, 1: True, 3: True, 4: True }, cluster.nodes[2].votes_granted

    cluster.send_message(ElectionTimeout(), dest=3) # Not enough log entries
    assert cluster.nodes[3].role == 'CANDIDATE'
    assert cluster.nodes[3].votes_granted == { 0: False, 1: False, 2: False, 4: False }, cluster.nodes[3].votes_granted

    cluster.send_message(ElectionTimeout(), dest=4)
    assert cluster.nodes[4].role == 'LEADER'
    assert cluster.nodes[4].votes_granted == { 0: False, 1: True, 2: False, 3: True }, cluster.nodes[4].votes_granted

    print("Good election")


# Recreate figure 7 from the Raft paper
def create_figure_7():
    def create_log(terms):
        return [ LogEntry(term, 'x') for term in terms ]

    cluster = FakeCluster(7)
    cluster.nodes[0].log = create_log([1,1,1,4,4,5,5,6,6,6])
    cluster.nodes[1].log = create_log([1,1,1,4,4,5,5,6,6])
    cluster.nodes[2].log = create_log([1,1,1,4])
    cluster.nodes[3].log = create_log([1,1,1,4,4,5,5,6,6,6,6])
    cluster.nodes[4].log = create_log([1,1,1,4,4,5,5,6,6,6,7,7])
    cluster.nodes[5].log = create_log([1,1,1,4,4,4,4,])
    cluster.nodes[6].log = create_log([1,1,1,2,2,2,3,3,3,3,3])

    for node in cluster.nodes:
        node.current_term = 8

    cluster.nodes[0]._become_leader()
    return cluster

def test_figure_7():
    cluster = create_figure_7()
    # Add a new log entry
    cluster.nodes[0].handle_message(ClientLogAppend('y'))

    # Run 10 heartbeats (that number picked arbitrarily)
    for _ in range(10):
        cluster.send_message(HeartBeat(), dest=0)

    # See if the logs are all the same as leader
    assert all(node.log == cluster.nodes[0].log for node in cluster.nodes), cluster.nodes

    print('Good figure 7')
    return cluster

if __name__ == '__main__':
    test_raft_server()
    test_figure_6()
    test_figure_7()
    test_election()
