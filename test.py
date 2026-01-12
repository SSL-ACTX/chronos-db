import socket
import struct
import uuid
import time
import random
import unittest
import requests
import sys
import os

# --- Configuration ---
HOST = '127.0.0.1'
TCP_PORT = 9000
RAFT_PORT = 20001
VECTOR_DIM = 128

# --- OpCodes ---
OP_INSERT     = 0x01
OP_GET        = 0x02
OP_SEARCH     = 0x03
OP_HISTORY    = 0x04
OP_DELETE     = 0x05
OP_UPDATE     = 0x06
OP_GET_AS_OF  = 0x07 # [NEW] Time Travel

class ChronosClient:
    def __init__(self, host=HOST, port=TCP_PORT, timeout=5.0):
        self.addr = (host, port)
        self.timeout = timeout

    def _connect(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect(self.addr)
            return s
        except ConnectionRefusedError:
            return None

    def _send_command(self, op_code, body):
        s = self._connect()
        if not s: return None
        try:
            # Protocol: [OpCode: 1B] [Length: 4B] [Body: N Bytes]
            header = struct.pack('<BI', op_code, len(body))
            s.sendall(header + body)
            return self._handle_response(s, op_code)
        except Exception as e:
            print(f"Socket Error: {e}")
            return None
        finally:
            s.close()

    def _handle_response(self, sock, op_code):
        try:
            if op_code in [OP_INSERT, OP_DELETE, OP_UPDATE]:
                return sock.recv(2) == b'OK'

            elif op_code == OP_GET or op_code == OP_GET_AS_OF:
                found = sock.recv(1)
                if not found or found[0] == 0: return None

                len_bytes = sock.recv(4)
                if not len_bytes: return None
                payload_len = struct.unpack('<I', len_bytes)[0]
                return self._recv_exact(sock, payload_len)

            elif op_code == OP_SEARCH:
                count_bytes = sock.recv(4)
                if not count_bytes: return []
                count = struct.unpack('<I', count_bytes)[0]
                results = []
                for _ in range(count):
                    uid = uuid.UUID(bytes=self._recv_exact(sock, 16))
                    dist = struct.unpack('<f', self._recv_exact(sock, 4))[0]
                    results.append((uid, dist ** 0.5))
                return results

            elif op_code == OP_HISTORY:
                count_bytes = sock.recv(4)
                if not count_bytes: return []
                count = struct.unpack('<I', count_bytes)[0]
                hist = []
                for _ in range(count):
                    s = struct.unpack('<Q', self._recv_exact(sock, 8))[0]
                    e = struct.unpack('<Q', self._recv_exact(sock, 8))[0]
                    pl = struct.unpack('<I', self._recv_exact(sock, 4))[0]
                    p = self._recv_exact(sock, pl)
                    hist.append({'start': s, 'end': e, 'payload': p})
                return hist
        except Exception:
            return None

    def _recv_exact(self, sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk: raise IOError("EOF")
            data += chunk
        return data

    def insert(self, key, vector, payload):
        vec_fmt = f'<{VECTOR_DIM}f'
        body = key.bytes + struct.pack(vec_fmt, *vector) + payload
        return self._send_command(OP_INSERT, body)

    def get_as_of(self, key, timestamp):
        # Body: [UUID (16)] [Timestamp (8)]
        body = key.bytes + struct.pack('<Q', int(timestamp))
        return self._send_command(OP_GET_AS_OF, body)

    def search(self, query, k):
        vec_fmt = f'<{VECTOR_DIM}f'
        body = struct.pack('<I', k) + struct.pack(vec_fmt, *query)
        return self._send_command(OP_SEARCH, body)

    def history(self, key):
        return self._send_command(OP_HISTORY, key.bytes)

class TestChronosDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("\n[SETUP] Initializing Client...")
        cls.client = ChronosClient()

        # Bootstrap Raft
        try:
            requests.post(f"http://{HOST}:{RAFT_PORT}/init", json={}, timeout=1)
        except: pass

        # Wait for Leader
        print("[SETUP] Waiting for Leader...", end='', flush=True)
        for _ in range(15):
            if cls.client.insert(uuid.uuid4(), [0.0]*VECTOR_DIM, b"ping"):
                print(" DONE.")
                return
            time.sleep(1)
            print(".", end='', flush=True)
        sys.exit(1)

    def test_01_basic_flow(self):
        uid = uuid.uuid4()
        vec = [random.random() for _ in range(VECTOR_DIM)]
        self.assertTrue(self.client.insert(uid, vec, b"data"))
        found = False
        for _ in range(10):
            res = self.client.search(vec, 5)
            found_ids = [r[0] for r in res]
            if uid in found_ids:
                found = True
                break
            time.sleep(0.2)
        self.assertTrue(found)

    def test_02_payload_boundary(self):
        uid = uuid.uuid4()
        vec = [0.1] * VECTOR_DIM
        self.assertFalse(self.client.insert(uid, vec, b"B"*(70*1024)))

    def test_03_vector_math_edge_cases(self):
        uid_z = uuid.uuid4()
        self.client.insert(uid_z, [0.0]*VECTOR_DIM, b"Zero")
        time.sleep(0.2)
        res = self.client.search([0.0]*VECTOR_DIM, 5)
        found_ids = [r[0] for r in res]
        self.assertIn(uid_z, found_ids)

    def test_04_deep_history_stress(self):
        uid = uuid.uuid4()
        vec = [0.0] * VECTOR_DIM
        for i in range(5):
            self.client.insert(uid, vec, f"v{i}".encode())
            time.sleep(0.05)
        hist = self.client.history(uid)
        self.assertEqual(len(hist), 5)

    def test_05_time_travel(self):
        """Verify OP_GET_AS_OF API with Real Time."""
        print("\n--- Test 05: Time Travel API ---")
        uid = uuid.uuid4()
        vec = [0.0] * VECTOR_DIM

        # 1. Capture "Before" Time
        t_before = int(time.time()) - 100

        # 2. Insert Data
        self.assertTrue(self.client.insert(uid, vec, b"Temporal Data"))
        time.sleep(0.5)

        # 3. Capture "After" Time
        t_after = int(time.time()) + 100

        # Query As Of "The Past" (Should be None)
        res_past = self.client.get_as_of(uid, t_before)
        self.assertIsNone(res_past, "Found record before it was born!")

        # Query As Of "The Future" (Should be Found)
        res_future = self.client.get_as_of(uid, t_after)
        self.assertEqual(res_future, b"Temporal Data")

if __name__ == "__main__":
    unittest.main(verbosity=2)
