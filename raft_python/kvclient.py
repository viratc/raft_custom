"""
# kvclient.py

@author: Virat Chourasia
@version: 0.5, 10/04/2022
"""


import json
import transport
import time

from kvserver import KVSERVERS

def encode_request(request):
    return json.dumps(request).encode('utf-8')

def decode_response(responsemsg):
    return json.loads(responsemsg.decode('utf-8'))['response']

class KVClient:
    def make_request(self, request):
        while True:
            for address in KVSERVERS.values():
                try:
                    sock = transport.make_connection(address)
                    requestmsg = encode_request(request)
                    transport.send_message(sock, requestmsg)
                except OSError:
                    continue
                try:
                    responsemsg = transport.receive_message(sock)
                    response = decode_response(responsemsg)
                    return response
                except EOFError:
                    pass
                finally:
                    sock.close()
            print("Couldn't find leader")
            time.sleep(5)

    def get(self, key):
        return self.make_request({'method': 'get', 'key': key})

    def set(self, key, value):
        return self.make_request({'method': 'set', 'key': key, 'value':value})

    def delete(self, key):
        return self.make_request({'method': 'delete', 'key': key})

def main(argv):
    kv = KVClient()
    if argv[1] == 'get':
        print(kv.get(argv[2]))
    elif argv[1] == 'set':
        print(kv.set(argv[2], argv[3]))
    elif argv[1] == 'delete':
        print(kv.delete(argv[2]))
    else:
        raise SystemExit("Bad command")

if __name__ == '__main__':
    import sys
    main(sys.argv)
