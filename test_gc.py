import socket
import struct
import uuid
import time
import sys

HOST = '127.0.0.1'
PORT = 9000
VECTOR_DIM = 128

# --- OPCODES ---
OP_INSERT  = 0x01
OP_UPDATE  = 0x06
OP_HISTORY = 0x04
OP_COMPACT = 0x08 # COMPACT

def send_cmd(op, body):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    header = struct.pack('<BI', op, len(body))
    s.sendall(header + body)

    # Simple response handling
    if op in [OP_INSERT, OP_UPDATE, OP_COMPACT]:
        return s.recv(2) == b'OK'

    if op == OP_HISTORY:
        count_bytes = s.recv(4)
        if not count_bytes: return 0
        count = struct.unpack('<I', count_bytes)[0]
        # Drain the rest (we just want the count for this test)
        while True:
            chunk = s.recv(4096)
            if not chunk or len(chunk) == 0: break
        return count

def main():
    print("=== Testing Garbage Collection (Compaction) ===")

    # 1. Create a Record
    uid = uuid.uuid4()
    vec = [0.1] * VECTOR_DIM
    vec_bytes = b''.join([struct.pack('<f', x) for x in vec])

    print(f"[*] Inserting Base Record: {uid}")
    payload = b"Version 0"
    body = uid.bytes + vec_bytes + payload
    if not send_cmd(OP_INSERT, body):
        print("[!] Insert Failed")
        sys.exit(1)

    # 2. Update it 20 times (Creating 21 total versions)
    print("[*] Spamming 20 Updates...")
    for i in range(1, 21):
        payload = f"Version {i}".encode()
        body = uid.bytes + payload
        if not send_cmd(OP_UPDATE, body):
            print(f"[!] Update {i} Failed")
            sys.exit(1)
        time.sleep(0.01)

    # 3. Verify History Count (Should be 21)
    # Note: Our simple send_cmd might not parse full history cleanly,
    # but let's assume the count returned in header is correct.

    def get_history_count(u):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        s.sendall(struct.pack('<BI', OP_HISTORY, 16) + u.bytes)
        count = struct.unpack('<I', s.recv(4))[0]
        s.close()
        return count

    count_before = get_history_count(uid)
    print(f"[*] History Count Before GC: {count_before}")
    if count_before != 21:
        print(f"[!] Expected 21 versions, found {count_before}")

    # 4. Trigger Compaction (Keep last 5)
    print("[*] Triggering Compaction (Limit: 5 versions)...")
    # Body: u64 limit (8 bytes)
    limit = 5
    body = struct.pack('<Q', limit)
    if send_cmd(OP_COMPACT, body):
        print("    -> Compaction Command Accepted.")
    else:
        print("    -> Compaction Command Failed.")
        sys.exit(1)

    time.sleep(1)

    # 5. Verify History Count (Should be 5)
    count_after = get_history_count(uid)
    print(f"[*] History Count After GC:  {count_after}")

    if count_after == 5:
        print("\n[SUCCESS] Garbage Collection successfully pruned old versions.")
    elif count_after < 21:
        print(f"\n[PARTIAL] GC ran but count is {count_after} (Expected 5).")
    else:
        print("\n[FAIL] GC did not reduce history count.")

if __name__ == "__main__":
    main()
