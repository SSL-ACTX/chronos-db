import time
import socket
import struct
import uuid
import threading
import os
import subprocess
import random
import sys
import requests

# --- CONFIGURATION ---
# change to "target/release/chronos" for real performance numbers
BINARY_PATH = "target/release/chronos"
VECTOR_DIM = 128

# Bench Settings
TOTAL_INSERTS = 2000   # Total items to insert
INSERT_THREADS = 1    # Concurrent writers
TOTAL_SEARCHES = 2000  # Total searches to perform
SEARCH_THREADS = 1    # Concurrent readers

NODES = [
    {"id": 1, "tcp": 9000, "raft": 20001},
    {"id": 2, "tcp": 9001, "raft": 20002},
    {"id": 3, "tcp": 9002, "raft": 20003},
]

# Protocol
OP_INSERT = 0x01
OP_SEARCH = 0x03

def cleanup():
    print("[*] Killing old chronos processes...")
    os.system("pkill -f chronos")
    time.sleep(1)
    print("[*] Cleaning up data files...")
    os.system("rm -f node_*_wal.dat node_*_index.dat")

def start_node(n):
    cmd = [
        BINARY_PATH,
        "--node-id", str(n['id']),
        "--addr", f"127.0.0.1:{n['tcp']}",
        "--raft-port", str(n['raft'])
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def wait_for_raft(port):
    url = f"http://127.0.0.1:{port}/raft-vote"
    for _ in range(20):
        try:
            requests.get(url, timeout=0.5)
            return True
        except:
            time.sleep(0.5)
    return False

# --- CLIENT ---

class ChronosClient:
    def __init__(self, port):
        self.port = port
        self.payload_bytes = b"x" * 50 # 50 byte payload
        self.dummy_vec = [random.random() for _ in range(VECTOR_DIM)]

    def insert(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", self.port))

            req_id = uuid.uuid4()
            total_len = 16 + (VECTOR_DIM * 4) + len(self.payload_bytes)

            s.send(struct.pack('B', OP_INSERT))
            s.send(struct.pack('<I', total_len))
            s.send(req_id.bytes)
            for val in self.dummy_vec:
                s.send(struct.pack('<f', val))
            s.send(self.payload_bytes)

            resp = s.recv(2)
            s.close()
            return resp == b"OK"
        except:
            return False

    def search(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", self.port))

            total_len = 4 + (VECTOR_DIM * 4)
            s.send(struct.pack('B', OP_SEARCH))
            s.send(struct.pack('<I', total_len))
            s.send(struct.pack('<I', 5)) # Limit 5
            for val in self.dummy_vec:
                s.send(struct.pack('<f', val))

            # Read response (minimal parse to consume buffer)
            count_bytes = s.recv(4)
            if not count_bytes: return False
            count = struct.unpack('<I', count_bytes)[0]
            for _ in range(count):
                s.recv(16) # UUID
                s.recv(4)  # Dist

            s.close()
            return True
        except:
            return False

# --- WORKERS ---

def write_worker(num_ops, port, results):
    client = ChronosClient(port)
    success = 0
    start = time.time()
    for _ in range(num_ops):
        if client.insert():
            success += 1
    duration = time.time() - start
    results.append((success, duration))

def read_worker(num_ops, port, results):
    client = ChronosClient(port)
    success = 0
    start = time.time()
    for _ in range(num_ops):
        if client.search():
            success += 1
    duration = time.time() - start
    results.append((success, duration))

# --- MAIN ---

def main():
    if not os.path.exists(BINARY_PATH):
        print(f"Error: {BINARY_PATH} not found.")
        sys.exit(1)

    cleanup()

    print(f"[*] Starting 3-Node Cluster ({BINARY_PATH})...")
    procs = [start_node(n) for n in NODES]

    if not all(wait_for_raft(n['raft']) for n in NODES):
        print("[!] Nodes failed to start.")
        [p.kill() for p in procs]
        sys.exit(1)

    print("[*] Initializing Raft Cluster...")
    # Init Node 1
    requests.post(f"http://127.0.0.1:{NODES[0]['raft']}/init")
    time.sleep(2)
    # Add Node 2 & 3
    for i in [1, 2]:
        requests.post(f"http://127.0.0.1:{NODES[0]['raft']}/add-learner",
                      json=[NODES[i]['id'], f"127.0.0.1:{NODES[i]['raft']}"])
    time.sleep(1)
    requests.post(f"http://127.0.0.1:{NODES[0]['raft']}/change-membership",
                  json=[1, 2, 3])
    time.sleep(3) # Stabilize
    print("[*] Cluster Ready.")

    # --- BENCHMARK WRITES ---
    print(f"\n--- WRITE BENCHMARK ({TOTAL_INSERTS} ops, {INSERT_THREADS} threads) ---")
    print(f"[*] Directing all writes to LEADER (Node 1 TCP: {NODES[0]['tcp']})")

    threads = []
    results = []
    ops_per_thread = TOTAL_INSERTS // INSERT_THREADS

    start_global = time.time()
    for _ in range(INSERT_THREADS):
        t = threading.Thread(target=write_worker, args=(ops_per_thread, NODES[0]['tcp'], results))
        t.start()
        threads.append(t)

    for t in threads: t.join()
    total_time = time.time() - start_global

    total_success = sum(r[0] for r in results)
    print(f"Completed {total_success}/{TOTAL_INSERTS} inserts.")
    print(f"Time: {total_time:.2f}s")
    print(f"TPS:  {total_success / total_time:.2f} ops/sec")

    time.sleep(2) # Let replication catch up

    # --- BENCHMARK READS ---
    print(f"\n--- READ BENCHMARK ({TOTAL_SEARCHES} ops, {SEARCH_THREADS} threads) ---")
    print(f"[*] Distributing reads across ALL NODES (Load Balancing)")

    threads = []
    results = []
    ops_per_thread = TOTAL_SEARCHES // SEARCH_THREADS

    start_global = time.time()
    for i in range(SEARCH_THREADS):
        # Round-robin load balancing
        target_node = NODES[i % 3]
        t = threading.Thread(target=read_worker, args=(ops_per_thread, target_node['tcp'], results))
        t.start()
        threads.append(t)

    for t in threads: t.join()
    total_time = time.time() - start_global

    total_success = sum(r[0] for r in results)
    print(f"Completed {total_success}/{TOTAL_SEARCHES} searches.")
    print(f"Time: {total_time:.2f}s")
    print(f"QPS:  {total_success / total_time:.2f} ops/sec")

    # Cleanup
    print("\n[*] Shutting down...")
    [p.terminate() for p in procs]

if __name__ == "__main__":
    main()
