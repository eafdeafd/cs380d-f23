import argparse
import xmlrpc.client
import xmlrpc.server
import threading
serverId = 0
basePort = 9000


class KVSRPCServer:

    def __init__(self):
        self.version = 0
        self.kvs = {}
        self.lock = threading.Lock()
        self.key_to_version = {}
        self.shutdown = False

    def update_data(self, data, version):
        with self.lock:
            self.kvs = data
            self.key_to_version = version
            return "Success"

    # put: Insert a new-key-value pair or updates an existing
    # one with new one if the same key already exists.
    def put(self, key, value):
        with self.lock:
            if key not in self.key_to_version:
                self.key_to_version[key] = 0
            self.key_to_version[key] += 1
            self.kvs[key] = value
            return "[Server " + str(serverId) + "] Receive a put request: " + "Key = " + str(key) + ", Value = " + str(value)

    # get: Get the value associated with the given key.
    def get(self, key):
        with self.lock:
            return f"{self.kvs.get(key, 'ERR_KEY')}:{self.key_to_version[key]}"

    # printKVPairs: Print all the key-value pairs at this server.
    def printKVPairs(self):
        with self.lock:
            return '\n'.join([f"{k}:{v}" for k, v in self.kvs.items()]) + "\n"

    # shutdownServer: Terminate the server itself normally.
    def shutdownServer(self):
        with self.lock:
            self.kvs = {}
            self.key_to_version = {}
            self.shutdown = True
            return "[Server " + str(serverId) + "] Receive a request for a normal shutdown"

    def heartbeat(self):
        return "Sucess"

    def should_shutdown(self):
        return "True" if self.should_shutdown else "False"


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    server = xmlrpc.server.SimpleXMLRPCServer(
        ("localhost", basePort + serverId))
    server_instance = KVSRPCServer()
    server.register_instance(server_instance)
    while not server_instance.should_shutdown() == "True":
        server.handle_request()
    print("Server is shutting down...")
