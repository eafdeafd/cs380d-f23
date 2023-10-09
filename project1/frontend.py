import xmlrpc.client
import xmlrpc.server
import time
import threading
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import random
import sys

kvsServers = dict()
activeServers = set()
requests = list()
baseAddr = "http://localhost:"
baseServerPort = 9000


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


class FrontendRPCServer:
    def __init__(self):
        self.kLock = threading.Lock()
        self.wLock = threading.Lock()


        self.key_to_version = {}
        self.key_to_lock = {}
        self.log = {}
        self.heartbeat_rate = 10  # Rate = # heartbeats per second
        self.heartbeat_max = 5  # Number of allowed heartbeats till we mark it as dead
        print("HIIII")
        #self.start_heartbeat()


    # Forever heartbeat on thread.
    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_check)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    # Timer every second, ping every server. If alive, reset counter. Otherwise remove server after 5 seconds for death.
    def heartbeat_check(self):
        while True:
            servers_to_remove = []
            serverList = list(kvsServers.keys())
            heartbeats = {k: 0 for k in serverList}
            for _ in range(self.heartbeat_max + 1):
                for i in serverList:
                    try:
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
    # passing lock to frontend
    def put(self, key, value):
        sys.stderr.write(f"[PUT] {key} {value}\n")
        if len(kvsServers) == 0:
            sys.stderr.write("ERR_NOSERVERS\n")
            return "ERR_NOSERVERS"
        with self.kLock:
            key = str(key)            
            if key not in self.key_to_version:
                self.key_to_lock[key] = threading.Lock()
                self.key_to_lock[key].acquire()
                self.key_to_version[key] = 0
            else:
                self.key_to_lock[key].acquire()
            self.log[key] = value
            self.key_to_version[key] += 1
        activeServersList = list(activeServers)
        least_one = False
        for i in activeServersList:
            try:
                kvsServers[i].put(key, value)
                least_one = True
            except:
                with self.kLock:
                    activeServers.discard(i)
        activeServersList = list(activeServers)
        serverIds = set(kvsServers.keys())
        repairServers = [i for i in serverIds if i not in activeServersList]
        for i in repairServers:
            try:
                with self.kLock:
                    kvsServers[i].update_data({k: v for k, v in self.log.items()}, {
                                                 k: v for k, v in self.key_to_version.items()})
                    activeServers.add(i)
                least_one = True
            except:
                pass
        self.key_to_lock[key].release()
        return f"Success put {key}:{value}" + repr(kvsServers) + repr(self.log) + repr(self.key_to_version) + repr(self.key_to_lock)


    # get: This function routes requests from clients to proper
    # servers that are responsible for getting the value
    # associated with the given key.
    def get(self, key):
        key = str(key)
        sys.stderr.write(f"[GET] {key}\n")
        if key not in self.log:
            sys.stderr.write("ERR_KEY\n")
            return "ERR_KEY"
        # Get with retries
        # most up to date version
        if len(kvsServers) == 0:
            sys.stderr.write("ERR_NOSERVERS\n")
            return "ERR_NOSERVERS" #+ repr(kvsServers) + '.'.join([str(i) for i in serverIds]) + repr(kvsServers) + repr(self.log) + repr(self.key_to_version) + repr(self.key_to_lock)
        while key not in self.key_to_lock:
            time.sleep(.00001)
        with self.key_to_lock[key]:
            serverIds = list(kvsServers.keys())
            activeServersList = list(activeServers)
            while len(serverIds) != 0:
                if len(activeServersList) > 0:
                    server = random.choice(activeServersList)
                    try:
                        value, version = kvsServers[server].get(key).split(":")
                        if str(self.key_to_version[key]) <= str(version):
                            sys.stderr.write(f"{key}:{value}\n")
                            return f"{key}:{value}"
                        #return f"{key}:{value}:{version}:{self.key_to_version[key]}:{self.log[key]}"
                    except Exception:
                        pass
                #Try and repair?
                serverIds = list(kvsServers.keys())
                activeServersList = list(activeServers)
                time.sleep(.01)
            return "ERR_NOSERVERS" + repr(kvsServers) + '.'.join([str(i) for i in serverIds]) + repr(kvsServers) + repr(self.log) + repr(self.key_to_version) + repr(self.key_to_lock)

    # printKVPairs: This function routes requests to servers
    # matched with the given serverIds.
    def printKVPairs(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        sys.stderr.write(f"printKVPairs {serverId}\n")
        return kvsServers[serverId].printKVPairs()

    # addServer: This function registers a new server with the
    # serverId to the cluster membership.
    def addServer(self, serverId):
        with self.wLock:
            #transport = xmlrpc.client.Transport()
            #transport.timeout = 0.2  # 200ms
            kvsServers[serverId] = xmlrpc.client.ServerProxy(
                baseAddr + str(baseServerPort + serverId))#, transport=transport)
            with self.kLock:
                kvsServers[serverId].update_data({k: v for k, v in self.log.items()}, {
                                                 k: v for k, v in self.key_to_version.items()})
                activeServers.add(serverId)
                sys.stderr.write(f"Sucess Adding Server {serverId}\n")
                return "Success"

    def listServer(self):
        serverList = list(kvsServers.keys())
        if len(serverList) == 0:
            return "ERR_NOSERVERS"
        serverList.sort()
        serverList = [str(i) for i in serverList]
        serverList = ", ".join(serverList)
        sys.stderr.write(f"[listServer] {serverList}\n")
        return serverList

    # shutdownServer: This function routes the shutdown request to
    # a server matched with the specified serverId to let the corresponding
    # server terminate normally.
    def shutdownServer(self, serverId):
        with self.wLock:
            if serverId not in kvsServers.keys():
                return "ERR_NOEXIST"
            try:
                kvsServers[serverId].shutdownServer()
                kvsServers.pop(serverId, None)
                activeServers.discard(serverId)
                sys.stderr.write(f"[Shutdown Server {serverId}]\n")
                return f"[Shutdown Server {serverId}]"
            except:
                sys.stderr.write(f"[ERROR SHUTTING DOWN SERVER {serverId}]\n")
                return f"[ERROR SHUTTING DOWN SERVER {serverId}]"


server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())
server.serve_forever()
