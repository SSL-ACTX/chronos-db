import subprocess
import time
import requests
import socket
import struct
import uuid
import os
import sys
import random # Import random

# --- CONFIG ---
BINARY_PATH = "target/release/chronos"
N1 = {"id": 1, "tcp": 9000, "raft": 20001}
N2 = {"id": 2, "tcp": 9001, "raft": 20002}
VECTOR_DIM = 128
OP_INSERT = 0x01
OP_SEARCH = 0x03

def cleanup():
    os.system("pkill -f chronos")
    os.system("rm -f node_*_wal.dat node_*_index.dat")
    time.sleep(1)

def start_node(n):
    cmd = [BINARY_PATH, "--node-id", str(n['id']), "--addr", f"127.0.0.1:{n['tcp']}", "--raft-port", str(n['raft'])]
    return subprocess.Popen(cmd)

def client_insert(port, count):
    print(f"[*] Inserting {count} DISTINCT vectors into Port {port}...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))

    payload = b"Payload"

    for i in range(count):
        req_id = uuid.uuid4()
        # Generate distinct vectors so HNSW can index them properly
        vec = [random.random() for _ in range(VECTOR_DIM)]

        total_len = 16 + (VECTOR_DIM * 4) + len(payload)
        s.send(struct.pack('B', OP_INSERT))
        s.send(struct.pack('<I', total_len))
        s.send(req_id.bytes)
        for v in vec: s.send(struct.pack('<f', v))
        s.send(payload)
        s.recv(2)
    s.close()

def client_search(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(("127.0.0.1", port))

        # Search for a random vector (doesn't matter, we just want count)
        vec = [0.5] * VECTOR_DIM
        total_len = 4 + (VECTOR_DIM * 4)

        s.send(struct.pack('B', OP_SEARCH))
        s.send(struct.pack('<I', total_len))
        s.send(struct.pack('<I', 100)) # Limit 100
        for v in vec: s.send(struct.pack('<f', v))

        count = struct.unpack('<I', s.recv(4))[0]
        s.close()
        return count
    except Exception as e:
        print(f"Search failed: {e}")
        return 0

def main():
    if not os.path.exists(BINARY_PATH):
        print("Build first!")
        sys.exit(1)

    cleanup()

    print(f"[*] Starting Node 1...")
    p1 = start_node(N1)
    time.sleep(2)
    requests.post(f"http://127.0.0.1:{N1['raft']}/init")
    time.sleep(2)

    # 1. INSERT 60 RECORDS (Crossing the 20 limit configured in main.rs)
    client_insert(N1['tcp'], 60)

    print("[*] Waiting 10s for Auto-Snapshot & Purge...")
    time.sleep(10)

    print("\n[*] Starting Node 2...")
    p2 = start_node(N2)
    time.sleep(2)

    print("[*] Joining Node 2...")
    requests.post(f"http://127.0.0.1:{N1['raft']}/add-learner", json=[N2['id'], f"127.0.0.1:{N2['raft']}"])
    time.sleep(1)
    requests.post(f"http://127.0.0.1:{N1['raft']}/change-membership", json=[1, 2])

    print("    Waiting for Snapshot Transfer (5s)...")
    time.sleep(5)

    print(f"\n[*] Verifying Data on Node 2...")
    count = client_search(N2['tcp'])
    print(f"    Node 2 Records: {count}")

    if count == 60:
        print("\n[SUCCESS] Node 2 has 60/60 records via AUTO-SNAPSHOT!")
    else:
        print(f"\n[FAIL] Node 2 has {count}/60.")

    p1.terminate()
    p2.terminate()

if __name__ == "__main__":
    main()
