## CS380d Project 1: Distributed Key-Value Store (KVS)

Through this project, we will learn how to build a distributed key-value store
and how to test its performance/correctness under various scenarios (such as a 
node crashing). Furthermore, we will gain some experience with the Kubernetes 
container orchestration system. This repository provides some skeleton codes
for the distributed KVS instances and guildelines to set up a basic KVS cluster 
configuration with them using Kubernetes.

## Contents

1. `create_cluster.py`: A python script that automatically sets up Kubernetes cluster configurations.
2. `run_cluster.py`: An implementation for `event trigger`.
3. `client.py`, `frontend.py`, `server.py`: Implementations containing skeleton codes for each cluster instance.
4. `dockerfiles/`: dockerfiles to generate the container images of each cluster instance.
5. `yaml/`: configuration files for the Kubernetes pods of each cluster instance.
6. `kubespray/`: A copy of [Kubespray](https://github.com/kubernetes-sigs/kubespray) repository.
7. `cluster.md`: A guildline to set up and run a KVS cluster.

## Setup & Run KVS cluster

We present a [guideline](https://github.com/vijay03/cs380d-f23/blob/master/project1/cluster.md) to set up a basic KVS cluster configuration using
Kubernetes. For this project, we use a single local machine and simulate 
distributed environments using multiple container instances within the local
machine. We provide scripts and source codes that are used to install
necessary dependencies and packages for Kubernetes cluster. You don't need to
download any third-party packages or source codes separately in addition to
what we provide through this repository. Please refer to [cluster.md](https://github.com/vijay03/cs380d-f23/blob/master/project1/cluster.md) for more details.

## Submission
Please submit a zip file containing `server.py` and `frontend.py`, and a README explaining
your design and implementation.

## Design 
The distributed key-value store design I have implemented is a primary-backup architecture where the frontend acts as the primary, 
keeping a master log and ensuring that all backups are up-to-date before serving clients to the backups for gets. 

Consistency is ensured through an active server list that is kept up-to-date with each put.
Clients are not served inactive servers. Repair is attempted at every put if some servers fall behind.

Per-key locking is done with a dictionary of keys to locks, and every put/get on the same key will require this lock.
This ensures that gets will get the latest put, and puts on the same key occur atomically.
Server locking is done with a dictionary of servers to locks, every RPC call is done with the appropriate server lock acquire-release, 
ensuring atomicity and blocking.

Heartbeating is achieved in another thread that the frontend runs permanently in the background at some heartbeat rate, 
querying the servers with a simple heartbeat function that returns "Success". Failed heartbeats through exceptions are caught 
and increments a heart counter that will add the server to a removal list for removal done at the end of each heartbeat cycle.

## Implementation details
Locks used:
    kLock - Key Lock for dict changing such as key_to_lock
    wLock - Add/Remove Server lock for kvsServers and activeServers
    key_to_lock - Per-key locking
    serverLocks - Per-server locking
addServer
    With wLock, adds the server and uses masterlog to update and put into active server list and all server list
listServer
	Gets a snapshot of all servers inside all server list and sorts by serverId
shutdownServer [serverID]
    With wLock, removes server by flipping a flag (server.py checks for this boolean flag in a while flag: hand_request vs. serve_forever)
    Pops from active server list, server lock dict, and all server list
put [key] [value]
    With key_to_lock, tries to call put with key and value on every server inside the active server list.
    If exception, we remove from the active server list.
    Then, try and repair all servers that are in the all server list but not in the active server list by replacing their entire log with the FE's log.
    If successful, add to the active server list. If not, then continue.
get [key]
	With key_to_lock, tries to call get with a random server inside active server list. If it fails, it tries forever as long as there exists 
    servers inside the all server list.
printKVPairs [serverID]
    Prints the key-value pairs inside the server via loop and join 
## Scalability
On my machine, the scaling tests are all about the same for run throughput, with +/- 100 ops/sec variance based on randomness and background resources.
This could be because the frontend is fully saturated. Since servers are chosen randomly from the active list, there should be positive scaling with more servers for get requests.
4 Clients 1 Server:
    Load throughput = 2040.2ops/sec
    Run throughput = 2046.5ops/sec
4 Clients 2 Servers:
    Load throughput = 1481.6ops/sec
    Run throughput = 2115.9ops/sec
4 Clients 4 Servers:
    Load throughput = 991.4ops/sec
    Run throughput = 2021.7ops/sec
4 Clients 8 Servers:
    Load throughput = 574.9ops/sec
    Run throughput = 1892.6ops/sec
## Running the code
No changes need to be made besides copying over frontend.py and server.py. Everything runs in python.