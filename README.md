<div align="center">

![ChronosDB Banner](https://capsule-render.vercel.app/api?type=waving&color=0:1a1a1a,100:ffffff&height=220&section=header&text=ChronosDB&fontSize=90&fontColor=ffffff&animation=fadeIn&fontAlignY=35&rotate=0&stroke=ffffff&strokeWidth=0&desc=Vector-Native.%20Time-Traveling.%20Distributed.&descSize=20&descAlignY=60)

![Language](https://img.shields.io/badge/language-Rust-orange.svg?style=for-the-badge&logo=rust)
![License](https://img.shields.io/badge/license-MIT-green.svg?style=for-the-badge)
![Architecture](https://img.shields.io/badge/architecture-Distributed%20Raft-blue.svg?style=for-the-badge)

**A crash-safe, strictly serializable, and horizontally scalable research database.**

</div>

---

> [!WARNING]
> **Proof of Concept & Research Only**
> 
> This project is strictly for **educational and research purposes**. It is designed to demonstrate the implementation of distributed consensus, vector indexing, and bi-temporal data management in Rust.
> 
> **It is not intended for production use**, and active development is not guaranteed. There are no warranties regarding data integrity or security.

---

## 📖 Overview

**ChronosDB** is a high-performance distributed database built in Rust. It uniquely combines **Vector Similarity Search** (for AI applications) with **Bi-Temporal Data Management** (Time Travel) and **Distributed Consensus** (Raft).

It solves the problem of "lossy" AI memory by ensuring that every vector embedding ever written is preserved in a strictly append-only history, reachable via time-travel queries.

## 🚀 Key Features

* **Distributed Consensus (Raft):** Built on `openraft`, ensuring linearizable writes and automatic leader election. Supports dynamic membership changes (adding/removing nodes) without downtime.
* **Vector Search Engine:** Custom implementation of **HNSW** (Hierarchical Navigable Small World) graph for approximate nearest neighbor search. Supports SIMD-optimized Euclidean and Cosine distance metrics.
* **Time Travel (History):** Every record maintains a `valid_time` and `tx_time`. Data is never overwritten; it is appended. You can query the full history of any object using the `HISTORY` command.
* **Disk-Based Architecture:** Uses memory-mapped files (`mmap`) for managing storage segments. It supports both "Strict Durability" (fsync) and "High Throughput" (async) modes based on hardware detection.
* **Zero-Copy Serialization:** Utilizes `rkyv` for guaranteeing data layout alignment and zero-copy deserialization from disk.
* **Binary Snapshots:** Uses `rkyv` for compact, high-speed binary snapshots, significantly reducing storage size and recovery time compared to JSON.
* **Probabilistic Filtering:** Implements Bloom Filters and SeaHash to minimize disk reads for non-existent keys.

---

## 🏗 Architecture

### 1. The Storage Layer

ChronosDB uses an immutable, append-only log structure to ensure crash safety and historical retention.

* **Segments:** Data is written to 64MB memory-mapped file segments.
* **Records:** Each entry contains a 128-dimensional vector, a binary payload, and temporal metadata.
* **Safety:** The system detects concurrency levels and adjusts `fsync` behavior automatically via system profiling.

### 2. The Index Layer

Vector indexing is handled by a persistent HNSW graph that updates in real-time.

* **Nodes:** Graph nodes are stored in a separate optimized index file.
* **Search:** Uses a priority queue-based search (beam search) to traverse graph layers.
* **Recall:** Tuned with `M=16` and `ef_construction=100` for high recall on random data distributions.

### 3. The Cluster Layer

* **Replication:** Logs are replicated to a quorum of nodes via the Raft protocol.
* **Snapshotting:** Supports auto-snapshotting based on log length (default: every 20 logs) to compact the Write Ahead Log (WAL). Snapshots are serialized in an optimized binary format.
* **API:** Exposes an HTTP API (default port 20001) for Raft management (voting, snapshots, membership).

---

## 🛠️ Installation & Build

**Prerequisites:**

* Rust
* Cargo

**Build:**

```bash
# Clone the repository
git clone https://github.com/SSL-ACTX/chronos-db.git
cd chronos-db

cargo build --release
```

**Run a Single Node:**

```bash
# Runs on TCP 9000, Raft API 20001
./target/release/chronos --node-id 1

```

---

## 💻 SQL-Like Query Interface

ChronosDB includes a custom SQL-like parser and a dedicated CLI client.

### Connect via CLI

```bash
cargo run --bin chronos-cli

```

### Supported Commands

**1. Insert Data**
Insert a 128-dim vector and a payload.

```sql
INSERT INTO VECTORS VALUES ([0.1, 0.5, ...], "user_session_data")

```

*Note: Vectors are auto-padded if shorter than 128 dimensions.*

**2. Vector Search**
Find the nearest vectors using HNSW.

```sql
SELECT FROM VECTORS WHERE VECTOR NEAR [0.1, 0.5, ...] LIMIT 5

```

**3. Retrieve by ID**
Get the latest version of a specific record.

```sql
GET 'uuid-string-here'

```

**4. Update Data**
Update the payload of an existing record (creates a new version).

```sql
UPDATE VECTORS SET PAYLOAD="Updated Payload" WHERE ID='uuid-string-here'

```

**5. Time Travel**
View the modification history of a record.

```sql
HISTORY 'uuid-string-here'

```

*Returns a list of start/end timestamps and payloads for every version of the record.*

**6. Delete**
Logically delete a record (appends a tombstone).

```sql
DELETE FROM VECTORS WHERE ID='uuid-string-here'

```

---

## 🌐 Clustering & Distribution

ChronosDB uses a custom binary protocol for data transmission and HTTP for consensus coordination.

### Bootstrapping a Cluster

A helper script `test_cluster.py` is provided to bootstrap a 3-node cluster locally.

1. **Start Nodes:**
Nodes must be started with unique IDs and ports.
* Node 1: TCP 9000, Raft 20001
* Node 2: TCP 9001, Raft 20002
* Node 3: TCP 9002, Raft 20003


2. **Initialize Leader:**
Send an init request to Node 1.
```bash
curl -X POST http://127.0.0.1:20001/init

```


3. **Add Learners & Promote:**
Add Node 2 and Node 3 to the cluster via the HTTP API.

### Snapshotting

The cluster automatically creates snapshots when the log grows too large. This allows new nodes to catch up by downloading a compressed state rather than replaying the entire history.

* **Trigger:** Default policy creates a snapshot every 20 logs.
* **Recovery:** Nodes automatically restore HNSW graphs and Bloom filters from snapshots upon restart.

---

## 🧪 Testing

The project includes Python integration tests for verifying cluster consistency and snapshot logic.

* **Cluster Test:** `python3 test_cluster.py`
* Verifies Write -> Kill Leader -> Election -> Write -> Read Consistency.


* **Snapshot Test:** `python3 test_snapshot.py`
* Inserts data past the log limit, triggers a snapshot, adds a new node, and verifies the new node hydrated correctly from the binary snapshot.
> Buggy

* **CLI Integration:** `python3 test_cli.py`
* Verifies the full SQL grammar, vector padding, and CRUD lifecycle.



---

## ⚙️ Configuration (Internal)

ChronosDB automatically detects environment resources via a System Profile:

| Hardware | Mode | Behavior |
| --- | --- | --- |
| **1 Core** | Potato Mode | No `fsync` (Async durability), High Raft timeout (1s). |
| **< 4 Cores** | Standard | Strict durability, 500ms heartbeat. |
| **Server** | Server Mode | Strict durability, 250ms heartbeat, max worker threads. |

---

<div align="center">

**Author:** Seuriin ([SSL-ACTX](https://github.com/SSL-ACTX))

</div>
