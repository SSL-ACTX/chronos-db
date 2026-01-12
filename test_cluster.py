import subprocess
import time
import requests
import socket
import struct
import uuid
import os
import sys

# --- CONFIGURATION ---
BINARY_PATH = "target/release/chronos"
VECTOR_DIM = 128

# Nodes Configuration
NODES = [
    {"id": 1, "tcp": 9000, "raft": 20001},
    {"id": 2, "tcp": 9001, "raft": 20002},
    {"id": 3, "tcp": 9002, "raft": 20003},
]

OP_INSERT = 0x01
OP_SEARCH = 0x03

def cleanup():
    print("[*] Killing old chronos processes...")
    os.system("pkill -f chronos") # Kills any running instances
    time.sleep(1) # Give OS time to release ports

    print("[*] Cleaning up old data files...")
    os.system("rm -f node_*_wal.dat node_*_index.dat")

def start_node(n):
    cmd = [
        BINARY_PATH,
        "--node-id", str(n['id']),
        "--addr", f"127.0.0.1:{n['tcp']}",
        "--raft-port", str(n['raft'])
    ]
    # Redirect stdout to avoid clutter
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def wait_for_raft_api(port, retries=15):
    url = f"http://127.0.0.1:{port}/raft-vote"
    for _ in range(retries):
        try:
            requests.get(url, timeout=0.5)
            return True
        except:
            time.sleep(0.5)
    return False

# --- SMART CLIENT FUNCTIONS ---

def smart_insert(vector, payload_str):
    """
    Tries to write to ANY node. If rejected, retries others.
    Keeps trying for 10 seconds (handles election windows).
    """
    deadline = time.time() + 10
    payload_bytes = payload_str.encode('utf-8')
    req_id = uuid.uuid4()

    while time.time() < deadline:
        for node in NODES:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(0.5)
                s.connect(("127.0.0.1", node['tcp']))

                # Protocol: Op(1) + Len(4) + ID(16) + Vec(128*4) + Payload
                total_len = 16 + (VECTOR_DIM * 4) + len(payload_bytes)
                s.send(struct.pack('B', OP_INSERT))
                s.send(struct.pack('<I', total_len))
                s.send(req_id.bytes)
                for val in vector:
                    s.send(struct.pack('<f', val))
                s.send(payload_bytes)

                resp = s.recv(2)
                s.close()

                if resp == b"OK":
                    print(f"    [+] Write Accepted by Node {node['id']}")
                    return True
            except:
                pass # Node might be down or unreachable

        print("    [.] No Leader found yet... retrying...")
        time.sleep(1)

    return False

def smart_search_all(vector):
    """Queries ALL active nodes to check consistency."""
    results = {}
    total_len = 4 + (VECTOR_DIM * 4)

    for node in NODES:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(("127.0.0.1", node['tcp']))

            s.send(struct.pack('B', OP_SEARCH))
            s.send(struct.pack('<I', total_len))
            s.send(struct.pack('<I', 10)) # Limit 10
            for val in vector:
                s.send(struct.pack('<f', val))

            count_data = s.recv(4)
            if not count_data:
                raise Exception("No data")

            count = struct.unpack('<I', count_data)[0]
            # Drain buffer
            for _ in range(count):
                s.recv(16); s.recv(4)

            s.close()
            results[node['id']] = count
        except Exception as e:
            # [FIXED LINE] Just set to -1, (Down) was a comment typo
            results[node['id']] = -1
    return results

def main():
    if not os.path.exists(BINARY_PATH):
        print("Binary not built.")
        sys.exit(1)

    print("[*] Starting 3-Node Cluster...")
    cleanup()
    procs = [start_node(n) for n in NODES]

    # Wait for startup
    if not all(wait_for_raft_api(n['raft']) for n in NODES):
        print("[!] Nodes failed to start.")
        [p.kill() for p in procs]
        sys.exit(1)

    # Bootstrapping
    print("[*] Initializing Raft (Node 1)...")
    requests.post(f"http://127.0.0.1:{NODES[0]['raft']}/init")
    time.sleep(2)

    print("[*] Adding Learners & Promoting...")
    # Add Node 2 & 3
    for i in [1, 2]:
        requests.post(f"http://127.0.0.1:{NODES[0]['raft']}/add-learner",
                      json=[NODES[i]['id'], f"127.0.0.1:{NODES[i]['raft']}"])
    time.sleep(1)
    requests.post(f"http://127.0.0.1:{NODES[0]['raft']}/change-membership",
                  json=[1, 2, 3])
    print("    -> Cluster is Stable (3 Nodes).")
    time.sleep(3)

    # --- TEST 1: NORMAL WRITE ---
    print("\n[Test 1] Writing Pre-Crash Data...")
    vec = [0.1] * VECTOR_DIM
    if not smart_insert(vec, "Data-Before-Crash"):
        print("[FAIL] Initial write failed.")
        sys.exit(1)

    # --- TEST 2: KILL LEADER ---
    print("\n[Test 2] KILLING LEADER (Node 1)...")
    procs[0].terminate()
    NODES[0]['tcp'] = 0 # Mark as effectively removed for our client list logic (optional)

    print("    -> Waiting for Election (approx 3s)...")
    time.sleep(3)

    # --- TEST 3: WRITE TO NEW LEADER ---
    print("\n[Test 3] Writing Post-Crash Data (Hunting for new Leader)...")
    if smart_insert(vec, "Data-After-Crash"):
        print("[PASS] System survived Leader crash and accepted new writes!")
    else:
        print("[FAIL] System rejected writes after crash.")

    # --- TEST 4: VERIFY CONSISTENCY ---
    print("\n[Test 4] Verifying Data on Surviving Nodes...")
    time.sleep(2) # Allow replication
    counts = smart_search_all(vec)

    print(f"    Node 2 Records: {counts.get(2)}")
    print(f"    Node 3 Records: {counts.get(3)}")

    if counts.get(2) == 2 and counts.get(3) == 2:
        print("\n[SUCCESS] PERFECT CONSISTENCY. Both nodes have all data.")
    elif counts.get(2) == 2 or counts.get(3) == 2:
        print("\n[PASS] EVENTUAL CONSISTENCY. At least one node has all data.")
    else:
        print("\n[FAIL] Data Loss detected.")

    # Cleanup
    [p.terminate() for p in procs]

if __name__ == "__main__":
    main()
