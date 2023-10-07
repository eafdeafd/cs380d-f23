import xmlrpc.client
import xmlrpc.server
import time
import threading
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import random

kvsServers = dict()
requests = list()
baseAddr = "http://localhost:"
baseServerPort = 9000

class ShutdownSignal(Exception):
    pass

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    def __init__(self):
        self.heartbeat_counter = dict() # serverId: heartbeat_counter, keep track of heartbeats per server.
        self.start_heartbeat()
        #self.VERSION = 0
        self.kLock = threading.Lock()
        self.wLock = threading.Lock()
        self.key_to_version = {}
        self.key_to_lock = {}
        self.log = {}

    # Forever heartbeat on thread.
    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target = self.heartbeat_check)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        self.heartbeat_rate = 10 # Rate = # heartbeats per second
        self.heartbeat_max = 2 # Number of allowed heartbeats till we mark it as dead

    # Timer every second, ping every server. If alive, reset counter. Otherwise remove server after 5 seconds for death.
    def heartbeat_check(self):
        while True:
            time.sleep(1 / self.heartbeat_rate)
            with self.wLock:
                servers_to_remove = []
                serverList = list(kvsServers.keys())
                for i in serverList:
                    # Heartbeat servers
                    try:
                        response = kvsServers[i].heartbeat()
                        if response:
                            self.heartbeat_counter[i] = 0
                    # Mark for removal when dead
                    except: 
                        self.heartbeat_counter[i] += 1
                        if self.heartbeat_counter[i] >= self.heartbeat_max:
                            servers_to_remove.append(i)        
                # Remove marked servers
                for serverId in servers_to_remove:
                    kvsServers.pop(serverId, None)
                    self.heartbeat_counter.pop(serverId, None)


    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    # Per key versioning
    # passing lock to frontend
    def put(self, key, value):      
        if len(kvsServers) == 0:
            return "ERR_NOSERVERS"
        while self.wLock.locked():
            time.sleep(.0001)
        key = str(key)
        with self.kLock:
            if key not in self.key_to_version:
                self.key_to_lock[key] = threading.Lock()
                self.key_to_version[key] = 0
        with self.key_to_lock[key]:       
            serverIds = list(kvsServers.keys())
            retry = set()
            least_one = False
            for i in serverIds:
                try:
                    kvsServers[i].put(key, value)
                    least_one = True
                except:
                    retry.add(i)
            while len(retry) > 0:
                serverIds = set(kvsServers.keys())
                retry = retry & serverIds
                for i in retry:
                    try:
                        kvsServers[i].put(key, value)
                        least_one = True
                    except:
                        pass
            # If at least one put operation succeeded, update the VERSION and key-to-version
            with self.kLock:
                if least_one:
                    #self.VERSION += 1
                    self.log[key] = value
                    self.key_to_version[key] += 1
                    return f"Success put {key}:{value}"
                else:
                    return "ERR_NOSERVERS"
                    
    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
    def get(self, key):
        key = str(key)
        if key not in self.log:
            return "ERR_KEY"
        # Get with retries
        # most up to date version
        serverIds = list(kvsServers.keys())
        if len(serverIds) == 0:
            return "ERR_NOSERVERS"
        while len(serverIds) > 0: 
            server = random.choice(serverIds)
            try:
                value, version = kvsServers[server].get(key)
                if self.key_to_version[key] == version:
                    return value
                else:
                    break
            except:
                pass
            serverIds = list(kvsServers.keys())
        return "ERR_NOSERVERS"

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        return kvsServers[serverId].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        with self.wLock:
            kvsServers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
            self.heartbeat_counter[serverId] = 0
            with self.kLock:
                kvsServers[serverId].update_data({k:v for k,v in self.log.items()}, {k:v for k,v in self.key_to_version.items()})
            return "Success"

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        if len(kvsServers) == 0:
            return "ERR_NOSERVERS"
        serverList = list(kvsServers.keys())
        serverList.sort()
        serverList = [str(i) for i in serverList]
        return ", ".join(serverList)

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        with self.wLock:
            if serverId not in kvsServers.keys():
                return "ERR_NOEXIST"
            try:
                kvsServers[serverId].shutdownServer()
            except ShutdownSignal:
                kvsServers.pop(serverId, None)
                self.heartbeat_counter.pop(serverId, None)
                return f"[Shutdown Server {serverId}]"
            except:
                return f"[ERROR SHUTTING DOWN SERVER {serverId}]"

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())
server.serve_forever()
