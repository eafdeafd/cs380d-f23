import xmlrpc.client
import xmlrpc.server
import time
import threading
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import random
from contextlib import contextmanager
from threading  import Lock

kvsServers = dict()
requests = list()
baseAddr = "http://localhost:"
baseServerPort = 9000

class RWLock(object):
    """ RWLock class; this is meant to allow an object to be read from by
        multiple threads, but only written to by a single thread at a time. See:
        https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock
        Usage:
            from rwlock import RWLock
            my_obj_rwlock = RWLock()
            # When reading from my_obj:
            with my_obj_rwlock.r_locked():
                do_read_only_things_with(my_obj)
            # When writing to my_obj:
            with my_obj_rwlock.w_locked():
                mutate(my_obj)
    """

    def __init__(self):

        self.w_lock = Lock()
        self.num_r_lock = Lock()
        self.num_r = 0

    # ___________________________________________________________________
    # Reading methods.

    def r_acquire(self):
        self.num_r_lock.acquire()
        self.num_r += 1
        if self.num_r == 1:
            self.w_lock.acquire()
        self.num_r_lock.release()

    def r_release(self):
        assert self.num_r > 0
        self.num_r_lock.acquire()
        self.num_r -= 1
        if self.num_r == 0:
            self.w_lock.release()
        self.num_r_lock.release()

    @contextmanager
    def r_locked(self):
        """ This method is designed to be used via the `with` statement. """
        try:
            self.r_acquire()
            yield
        finally:
            self.r_release()

    # ___________________________________________________________________
    # Writing methods.

    def w_acquire(self):
        self.w_lock.acquire()

    def w_release(self):
        self.w_lock.release()

    @contextmanager
    def w_locked(self):
        """ This method is designed to be used via the `with` statement. """
        try:
            self.w_acquire()
            yield
        finally:
            self.w_release()

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

kvsServers_lock = RWLock()

class FrontendRPCServer:
    def __init__(self):
        self.heartbeat_counter = dict() # serverId: heartbeat_counter, keep track of heartbeats per server.
        self.start_heartbeat()
        self.VERSION = 0
        self.kLock = threading.Lock()
        self.key_to_version = {}
        self.key_to_lock = {}
        self.log = {}
        self.stale_servers = set()

    # Forever heartbeat on thread.
    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target = self.heartbeat_check)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        self.heartbeat_rate = 4 # Rate = # heartbeats per second
        self.heartbeat_max = 2 # Number of allowed heartbeats till we mark it as dead

    # Timer every second, ping every server. If alive, reset counter. Otherwise remove server after 5 seconds for death.
    def heartbeat_check(self):
        while True:
            time.sleep(1 / self.heartbeat_rate)
            servers_to_remove = []
            with kvsServers_lock.w_locked():
                for i in kvsServers.keys():
                    # Heartbeat servers
                    try:
                        response = kvsServers[i].heartbeat()
                        if response:
                            self.heartbeat_counter[i] = 0
                            if i in self.stale_servers:
                                try:
                                    kvsServers[i].update_data({k:v for k,v in self.log.items()}, self.VERSION)
                                    self.stale_servers.remove(i)
                                except:
                                    pass
                    # Mark for removal when dead
                    except: 
                        self.heartbeat_counter[i] += 1
                        if self.heartbeat_counter[i] >= self.heartbeat_max:
                            servers_to_remove.append(i)        
                # Remove marked servers
                for serverId in servers_to_remove:
                    kvsServers.pop(serverId, None)
                    self.heartbeat_counter.pop(serverId, None)
                    self.stale_servers.discard(serverId)


    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    # Per key versioning
    # passing lock to frontend
    def put(self, key, value):
        with kvsServers_lock.r_locked():       
            if len(kvsServers) == 0:
                return "ERR_NOSERVERS"
            key, value = str(key), str(value)
            serverIds = list(kvsServers.keys())
            #with self.kLock:
            if key not in self.key_to_version:
                self.key_to_lock[key] = threading.Lock()
            with self.key_to_lock[key]:
                #with self.kLock:
                self.VERSION += 1 # -------- GETS CAN NO LONGER GET OLD VALUE --------------
                self.log[key] = value
                self.key_to_version[key] = self.VERSION
                non_updated_servers = []
                active_servers = [i for i in serverIds if i not in self.stale_servers]                
                for i in active_servers:
                    try:
                        kvsServers[i].put(key, value)
                    except:
                        non_updated_servers.append(i)
                        self.stale_servers.add(i)
                if len(non_updated_servers) == 0:
                    return f"Success put {key}:{value}"
                else:
                    return f" WARNING Did not put {key}:{value} on servers: " + ",".join(non_updated_servers)
                    
    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
    def get(self, key):
        serverIds = list(kvsServers.keys())
        if len(serverIds) == 0:
            return "ERR_NOSERVERS"
        key = str(key)
        # Get with retries
        # most up to date version
        retries = self.heartbeat_max
        random.shuffle(serverIds)
        for i in serverIds:
            for _ in range(retries):
                try:
                    value, version = kvsServers[i].get(key)
                    # Check version against primary
                    if self.key_to_version[key] <= version:
                        return value
                    else:
                        break
                except KeyError:
                    return "ERR_KEY"
                except:
                    pass
        return "ERR_KEY"

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        with kvsServers_lock.r_locked():
            if serverId not in kvsServers:
                return "ERR_NOEXIST"
            return kvsServers[serverId].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        with kvsServers_lock.w_locked():
            kvsServers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
            self.heartbeat_counter[serverId] = 0
            if self.VERSION != 0:
                kvsServers[serverId].update_data({k:v for k,v in self.log.items()}, self.VERSION)
            return "Success"

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        with kvsServers_lock.r_locked():
            if len(kvsServers) == 0:
                return "ERR_NOSERVERS"
            serverList = []
            for serverId in sorted(kvsServers):
                serverList.append(str(serverId))
            return ", ".join(serverList)

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        with kvsServers_lock.w_locked():
            if serverId not in kvsServers.keys():
                return "ERR_NOEXIST"
            result = kvsServers[serverId].shutdownServer()
            kvsServers.pop(serverId, None)
            self.stale_servers.discard(serverId)
            self.heartbeat_counter.pop(serverId, None)
            return result

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())
server.serve_forever()
