"""
## kvserver.py

@author: Virat Chourasia
@version: 0.5, 10/04/2022
"""


# Client connection points for the KV application (these are not Raft)
KVSERVERS = {
    0: ('localhost', 20000),
    1: ('localhost', 21000),
    2: ('localhost', 22000),
    3: ('localhost', 23000),
    4: ('localhost', 24000),
    }

import time

import transport
import json
from raftrun import Raft

def decode_request(requestmsg:bytes):
    return json.loads(requestmsg.decode('utf-8'))

def encode_response(response) -> bytes:
    return json.dumps({'response': response}).encode('utf-8')

# Application. Will use Raft
class KVServer:
    def __init__(self, nodenum):
        self.nodenum = nodenum
        self.data = { }
        self.raft = Raft(nodenum, self.handle_request)      # To be written
        self.raft.run()

    def run(self):
        start_time = time.time()
        print("start time:", start_time)
        address = KVSERVERS[self.nodenum]
        sock = transport.make_server(address)
        print('KVServer running on', sock)
        while True:
            client, addr = sock.accept()
            requestmsg = transport.receive_message(client)
            request = decode_request(requestmsg)

            if request['method'] != 'get':
                try:
                    response = self.raft.submit(request)  # Waits until consensus is reached
                    # end_time = time.time()
                    # print("end time:", end_time)
                except Exception as err:
                    client.close()     # Violently drop connection (improve later)
                    continue
            else:
                if self.raft.is_leader():
                    response = self.handle_request(request)
                    # end_time = time.time()
                    # print("end time:", end_time)
                else:
                    client.close()
                    continue

            responsemsg = encode_response(response)
            transport.send_message(client, responsemsg)
            client.close()

    # Handler that processes requests (when consensus is reached)
    def handle_request(self, request):     # "Applies the state machine" (in the Raft paper)
        print("KV: Applying", request)
        if request['method'] == 'get':
            return self.data.get(request['key'], '')
        elif request['method'] == 'set':
            self.data[request['key']] = request['value']
            return 'ok'
        elif request['method'] == 'delete':
            if request['key'] in self.data:
                del self.data[request['key']]
            return 'ok'

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("Usage: kvserver nodenum")
    serv = KVServer(int(sys.argv[1]))
    serv.run()
