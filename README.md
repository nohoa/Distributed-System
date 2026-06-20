# Distributed System

A collection of distributed systems projects implementing core distributed computing concepts including MapReduce, the Raft consensus algorithm, fault-tolerant key/value storage, and scalable sharded storage systems.

## Project Status

| Component                      | Status        |
| ------------------------------ | ------------- |
| MapReduce Framework            | ✅ Completed   |
| Raft Leader Election           | ✅ Completed   |
| Raft Log Replication           | ✅ Completed   |
| Raft Persistence               | ✅ In Progress |
| Fault-Tolerant Key/Value Store | ✅ Completed   |
| Shard Controller               | ⬜ Not Started |
| Sharded Key/Value Store        | ⬜ Not Started |

---

## Project Structure

```text
src/
├── mr/            # MapReduce framework
├── mrapps/        # MapReduce applications
├── raft/          # Raft consensus algorithm
├── kvraft/        # Fault-tolerant Key/Value service
├── shardkv/       # Sharded Key/Value service
├── shardmaster/   # Shard configuration manager
├── labrpc/        # Simulated RPC network
├── labgob/        # Serialization utilities
├── models/        # Testing models
├── porcupine/     # Linearizability checker
└── main/          # Executables and test runners
```

---

# ✅ MapReduce (`src/mr`)

## Overview

The `mr` package implements a distributed MapReduce framework consisting of:

* Coordinator
* Worker processes
* Task scheduling
* Fault recovery
* Intermediate file management

Workers communicate with the coordinator using RPC and execute map and reduce tasks in parallel.

### Main Files

```text
src/mr/
├── coordinator.go
├── worker.go
└── rpc.go
```

### Testing Scenarios

* ✅ Basic map and reduce execution
* ✅ Parallel worker execution
* ✅ Multiple worker coordination
* ✅ Worker crash recovery
* ✅ Task reassignment after timeout
* ✅ Correct final output generation

---

# ✅ Raft Consensus (`src/raft`)

## Overview

The `raft` package implements the Raft distributed consensus protocol for maintaining a replicated log across multiple servers.

### Main Files

```text
src/raft/
├── raft.go
├── config.go
├── persister.go
└── util.go
```

### Completed Features

* ✅ Leader election
* ✅ Heartbeats
* ✅ Term management
* ✅ Log replication
* ✅ Commit index advancement
* ✅ Follower log synchronization
* ✅ Recovery after leader failure

### Testing Scenarios

#### Leader Election

* ✅ Initial leader election
* ✅ Single leader per term
* ✅ Leader re-election after failure
* ✅ Election timeout handling

#### Log Replication

* ✅ Command replication to followers
* ✅ Majority agreement before commit
* ✅ Multiple command replication
* ✅ Follower catch-up after reconnection
* ✅ Consistent log ordering across replicas

---

# ✅ Fault-Tolerant Key/Value Store (`src/kvraft`)

## Overview

The `kvraft` package builds a replicated key/value database on top of Raft.

### Main Files

```text
src/kvraft/
├── client.go
├── server.go
├── common.go
└── config.go
```

### Supported Operations

* Put(key, value)
* Get(key)
* Append(key, value)

All client requests are replicated through Raft before execution.

### Testing Scenarios

* ✅ Basic Put/Get operations
* ✅ Append operations
* ✅ Concurrent client requests
* ✅ Leader failure recovery
* ✅ Network partition handling
* ✅ Duplicate request detection
* ✅ Linearizability verification

---

# ✅ Persistence

Planned features:

* Persistent Raft state
* Crash recovery
* State restoration after restart
* Durable log storage

---

# ⬜ Shard Controller

Planned features:

* Group join
* Group leave
* Shard migration
* Dynamic rebalancing

---

# ⬜ Sharded Key/Value Store

Planned features:

* Distributed shard ownership
* Dynamic configuration changes
* Shard transfer between groups
* Fault-tolerant sharded storage

---

## Running Tests

### MapReduce

```bash
cd src/main
bash test-mr.sh
```

### Raft

```bash
cd src/raft
go test
```

### Key/Value Store

```bash
cd src/kvraft
go test
```

---

## Current Progress

* ✅ MapReduce framework completed
* ✅ Raft leader election completed
* ✅ Raft log replication completed
* ✅ Fault-tolerant key/value store completed
* ✅ Persistence not implemented
* ⬜ Shard controller not implemented
* ⬜ Sharded key/value store not implemented
