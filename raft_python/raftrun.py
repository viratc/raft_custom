"""
## raftrun.py

- Runtime environment for Raft

@author: Virat Chourasia
@version: 0.5, 10/04/2022
"""

# -- Python standard library
import threading
import queue
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import pickle
from collections import defaultdict
import time
import random

# -- Raft
from raftserver import RaftServer, ClientLogAppend, HeartBeat, ElectionTimeout, AppendEntries
import transport

# Network addresses of the various servers.   This would be part of some kind of
# configuration file

SERVERS = {
    # Maps "node number" -> network address
    0: ('localhost', 15000),
    1: ('localhost', 16000),
    2: ('localhost', 17000),
    3: ('localhost', 18000),
    4: ('localhost', 19000),
    }

HEARTBEAT_TIMER = 1
ELECTION_TIMER = 5
ELECTION_RANDOM = 3

# Encoding/decoding of messages.  Using pickle is easy, but insecure. Could consider
# JSON or some other encoding.
def encode_message(msg) -> bytes:
    return pickle.dumps(msg)

def decode_message(rawmsg: bytes):
    return pickle.loads(rawmsg)

class Raft:
    def __init__(self, nodenum, apply_entry=None):
        self.nodenum = nodenum
        self.server = RaftServer(nodenum, len(SERVERS))
        self.incoming_messages = queue.Queue()
        self.outgoing_messages = defaultdict(queue.Queue)
        self.heard_from_leader = False

        # Staging area for client requests that need consensus.  This dictionary
        # maps log indices to (Future, value) pairs.  This is checked when log entries are
        # "Applied to the state machine"
        self.pending_requests = { }

        # Optional callback to apply log entries
        if apply_entry:
            self.apply_entry = apply_entry

    # Default implementation of code to "apply the state machine".  Can be
    # overridden by applications.
    def apply_entry(self, value):
        print("Applying:", value)

    # Thread that runs messages
    def _process_messages(self):
        while True:
            msg = self.incoming_messages.get()

            # Special case. For client append operations, we record information
            # in the pending_requests staging area.
            if isinstance(msg, ClientLogAppend):
                if self.server.role != 'LEADER':
                    msg.future.set_exception(RuntimeError('Not leader'))
                    continue
                else:
                    self.pending_requests[len(self.server.log)] = (msg.future, msg.value)

            # Single entry point for controlling Raft
            outgoing = self.server.handle_message(msg)

            if isinstance(msg, AppendEntries) and outgoing:
                self.heard_from_leader = True

            # Deliver all of the outgoing messages
            for outmsg in outgoing:
                self.outgoing_messages[outmsg.dest].put(outmsg)

            # See if any log entries can be applied to the application.
            while self.server.last_applied < self.server.commit_index:
                self.server.last_applied += 1
                value = self.server.log[self.server.last_applied].value
                result = self.apply_entry(value)
                request = self.pending_requests.pop(self.server.last_applied, None)
                if request and request[1] == value:
                    request[0].set_result(result)

    # Thread that accepts incoming connections from other servers
    def _accept_connections(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(SERVERS[self.nodenum])
        sock.listen()
        while True:
            client, addr = sock.accept()
            threading.Thread(target=self._receive_messages, args=(client,)).start()

    # Thread that receives all incoming messages on a socket until the
    # connection is closed
    def _receive_messages(self, sock):
        while True:
            try:
                rawmsg = transport.receive_message(sock)
                msg = decode_message(rawmsg)
                self.incoming_messages.put(msg)
            except EOFError:
                break
        sock.close()

    # Thread that takes all outgoing messages and tries to deliver them.
    # Question: What if there is some kind of transmission problem?  Or a network
    # stall?
    def _send_messages(self, nodenum):
        sock = None
        while True:
            # Only send the most recent message (throw all earlier away)
            msg = self.outgoing_messages[nodenum].get()
            try:
                if sock is None:
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect(SERVERS[nodenum])
                transport.send_message(sock, encode_message(msg))
            except OSError as err:
                sock = None
                continue

    # Thread that periodically generates heartbeat events
    def _generate_heartbeats(self):
        while True:
            time.sleep(HEARTBEAT_TIMER)
            self.incoming_messages.put(HeartBeat())

    def _generate_election_timeouts(self):
        peers = [n for n in SERVERS if n != self.nodenum ]
        while True:
            time.sleep(ELECTION_TIMER + random.random()*ELECTION_RANDOM)
            if not self.heard_from_leader:
                self.incoming_messages.put(ElectionTimeout())
            self.heard_from_leader = False

    def is_leader(self):
        # This is not reliable in partitioned network. (Must rework)
        # For now. A quick hack
        return self.server.role == 'LEADER'

    def run(self):
        threading.Thread(target=self._process_messages).start()
        threading.Thread(target=self._accept_connections).start()
        threading.Thread(target=self._generate_heartbeats).start()
        threading.Thread(target=self._generate_election_timeouts).start()
        for n in SERVERS:
            threading.Thread(target=self._send_messages, args=(n,)).start()
        print(f"Raft node {self.nodenum} running")

    def submit(self, item):
        from raftserver import ClientLogAppend
        from concurrent.futures import Future

        # Create a "Future" to hold the final result of "applying the state machine"
        fut = Future()
        self.incoming_messages.put(ClientLogAppend(item, fut))
        return fut.result()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: raftrun.py nodenum")
    raft = Raft(int(sys.argv[1]))
    raft.run()
