"""
## raft.py

- Top-level interface to Raft.  Application programs will use this to request the services
  of Raft and get responses.
- Mostly pending

@author: Virat Chourasia
@version: 0.5, 10/04/2022
"""

from raftserver import RaftServer

class Raft:
    def __init__(self):
        self.server = RaftServer()

    # Submit some kind of request to Raft
    def submit(self, request, request_handler):
        print("Raft: Received", request)
        ...
        # Do Rafty stuff (TO-DO)
        ...
        # Temporary shim... what eventually has to happen before returning. On consensus,
        # the request is given to the supplied request_handler.
        result = request_handler(request)    # Callback to actually run it.
        return result

    # Sketch of an idea for running the Raft server.
    def run(self):
        message_queue = queue.Queue()

        # Threads that do different things.
        def process_messages():
            while True:
                msg = message_queue.get()
                outgoing = self.server.handle_message(msg)
                send_messages(outgoing)

        def send_messages(outgoing):
            for msg in outgoing:
                send(msg)          # TO-DO

        def receive_messages():
            while True:
                msg = receive()    # TO-DO
                message_queue.put(msg)
