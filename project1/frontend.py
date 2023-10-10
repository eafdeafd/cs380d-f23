import xmlrpc.client
import xmlrpc.server
import time
import threading
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import random

# All Servers
kvsServers = dict()
# Active/Up-to-date Servers
activeServers = set()
# Servers -> Locks (needed bc of RPC call timeouts)
serverLocks = dict()
requests = list()
baseAddr = "http://localhost:"
baseServerPort = 9000


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class FrontendRPCServer:
    def __init__(self):
        # Key Lock for dicts
        self.kLock = threading.Lock()
        # Add/Remove Server lock for kvsServers and activeServers
        self.wLock = threading.Lock()
        # Per-key Locking
        self.key_to_lock = {}
        # Master Log
        self.log = {}
        # Heartbeat parameters
        self.heartbeat_rate = 10  # Rate = # heartbeats per second
        self.heartbeat_max = 3  # Number of allowed heartbeats till we mark it as dead
        self.start_heartbeat()


    # Forever heartbeat on thread.
    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_check)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    # Ping every server. If alive, reset counter. Otherwise count it dead after some timeout.
    def heartbeat_check(self):
        while True:
            servers_to_remove = []
            serverList = list(kvsServers.keys())
            heartbeats = {k: 0 for k in serverList}
            for _ in range(self.heartbeat_max + 1):
                for i in serverList:
                    try:
                        with serverLocks[i]:
                            kvsServers[i].heartbeat()
                        heartbeats[i] = 0
                    except:
                        heartbeats[i] += 1
                    if heartbeats[i] >= self.heartbeat_max:
                        servers_to_remove.append(i)
                time.sleep(1 / self.heartbeat_rate)
            # Remove marked servers
            for serverId in servers_to_remove:
                with self.wLock:
                    kvsServers.pop(serverId, None)
                    activeServers.discard(serverId)
            time.sleep(1 / self.heartbeat_rate)

    # put: This function routes requests from clients to proper
    # servers that are responsible for inserting a new key-value
    # pair or updating an existing one.
    # Per key versioning
    def put(self, key, value):
        if len(kvsServers) == 0:
            return "ERR_NOSERVERS"
        # Create lock per key
        with self.kLock:
            key = str(key)            
            if key not in self.key_to_lock:
                self.key_to_lock[key] = threading.Lock()
                self.key_to_lock[key].acquire()
            else:
                self.key_to_lock[key].acquire()
            self.log[key] = value
        # Try and put for all active servers, otherwise deprecate it
        activeServersList = list(activeServers)
        for i in activeServersList:
            try:
                with serverLocks[i]:
                    kvsServers[i].put(key, value)
            except:
                with self.kLock:
                    activeServers.discard(i)
        # Try and repair ones that were missed from previous puts and reactive them
        activeServersList = list(activeServers)
        serverIds = set(kvsServers.keys())
        repairServers = [i for i in serverIds if i not in activeServersList]
        for i in repairServers:
            try:
                with self.kLock:
                    with serverLocks[i]:
                        kvsServers[i].update_data({k: v for k, v in self.log.items()})
                    activeServers.add(i)
            except:
                pass
        self.key_to_lock[key].release()
        return f"Success put {key}:{value}"


    # get: This function routes requests from clients to proper
    # servers that are responsible for getting the value
    # associated with the given key.
    def get(self, key):
        key = str(key)
        if key not in self.log:
            return "ERR_KEY"
        if len(kvsServers) == 0:
            return "ERR_NOSERVERS" 
        # Could potentially have concurrent get/put, put will create the lock
        while key not in self.key_to_lock:
            time.sleep(.00001)
        # Operations are atomic for get/put on same key
        with self.key_to_lock[key]:
            serverIds = list(kvsServers.keys())
            activeServersList = list(activeServers)
            # Get with retries, while there are still servers that could be alive
            while len(serverIds) != 0:
                # Get from random active server
                if len(activeServersList) > 0:
                    server = random.choice(activeServersList)
                    try:
                        with serverLocks[server]:
                            value = kvsServers[server].get(key)
                            return f"{key}:{value}"
                    except Exception:
                        pass
                serverIds = list(kvsServers.keys())
                activeServersList = list(activeServers)
                time.sleep(.01)
            return "ERR_NOSERVERS"

    # printKVPairs: This function routes requests to servers
    # matched with the given serverIds.
    def printKVPairs(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        print(f"printKVPairs {serverId}")
        with serverLocks[serverId]:
            return kvsServers[serverId].printKVPairs()

    # addServer: This function registers a new server with the
    # serverId to the cluster membership.
    def addServer(self, serverId):
        with self.wLock:
            kvsServers[serverId] = xmlrpc.client.ServerProxy(
                baseAddr + str(baseServerPort + serverId))
            serverLocks[serverId] = threading.Lock()
            # Adding a server and populate with master log
            with self.kLock:
                with serverLocks[serverId]:
                    kvsServers[serverId].update_data({k: v for k, v in self.log.items()})
                activeServers.add(serverId)
                return "Success"

    def listServer(self):
        serverList = list(kvsServers.keys())
        if len(serverList) == 0:
            return "ERR_NOSERVERS"
        serverList.sort()
        serverList = [str(i) for i in serverList]
        serverList = ", ".join(serverList)
        return serverList

    # shutdownServer: This function routes the shutdown request to
    # a server matched with the specified serverId to let the corresponding
    # server terminate normally.
    def shutdownServer(self, serverId):
        with self.wLock:
            if serverId not in kvsServers.keys():
                return "ERR_NOEXIST"
            try:
                with serverLocks[serverId]:
                    kvsServers[serverId].shutdownServer()
                kvsServers.pop(serverId, None)
                serverLocks.pop(serverId, None)
                activeServers.discard(serverId)
                return f"[Shutdown Server {serverId}]"
            except:
                return f"[ERROR SHUTTING DOWN SERVER {serverId}]"


server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())
server.serve_forever()
