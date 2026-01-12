import subprocess
import time
import os
import re
import sys

# --- Configuration ---
SERVER_BIN = "./target/debug/chronos"
CLI_BIN = "./target/debug/chronos-cli"

WAL_FILE = "node_1_wal.dat"
INDEX_FILE = "node_1_index.dat"

class ChronosIntegrationSuite:
    def __init__(self):
        self.server_process = None

    def setup(self):
        print("\n[*] Setting up test environment...")

        if os.path.exists(WAL_FILE): os.remove(WAL_FILE)
        if os.path.exists(INDEX_FILE): os.remove(INDEX_FILE)

        if not os.path.exists(SERVER_BIN) or not os.path.exists(CLI_BIN):
            print(f"[!] Binaries not found at {SERVER_BIN} or {CLI_BIN}")
            print("    Please run: cargo build --release")
            sys.exit(1)

        print(f"[*] Starting ChronosDB Server...")
        self.server_process = subprocess.Popen(
            [SERVER_BIN, "--node-id", "1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2) # Allow startup

        print("[*] Initializing Raft Cluster (becoming Leader)...")
        try:
            subprocess.run([
                "curl", "-X", "POST", "http://127.0.0.1:20001/init",
                "-H", "Content-Type: application/json",
                "-d", "{}"
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        except Exception as e:
            print(f"[!] Failed to init Raft: {e}")
            self.teardown()
            sys.exit(1)

        time.sleep(2)

    def teardown(self):
        print("\n[*] Tearing down...")
        if self.server_process:
            self.server_process.terminate()
            try:
                self.server_process.wait(timeout=2)
            except:
                self.server_process.kill()

    def run_cli_command(self, cmd_string):
        """Runs a command through the CLI binary via stdin."""
        input_str = f"{cmd_string}\nEXIT\n"
        try:
            result = subprocess.run(
                [CLI_BIN],
                input=input_str,
                text=True,
                capture_output=True,
                timeout=5
            )
            return result.stdout + result.stderr
        except Exception as e:
            return f"CLI Execution Error: {str(e)}"

    def run_tests(self):
        print("=== Running ChronosDB CLI Integration Tests ===")
        captured_uuid = None

        # [Test 1] INSERT
        print("\n[Test 1] INSERT")
        cmd = 'INSERT INTO VECTORS VALUES ([0.1, 0.1, 0.5], "Initial Payload")'
        output = self.run_cli_command(cmd)

        # Robust Regex for "[✓ OK] Inserted ID: <UUID>"
        # Handles potential unicode issues by looking for "OK] Inserted ID:"
        match = re.search(r"OK\] Inserted ID:\s*([a-f0-9\-]{36})", output)

        if match:
            captured_uuid = match.group(1)
            print(f"  [PASS] Insert successful. UUID: {captured_uuid}")
        else:
            print(f"  [FAIL] Insert failed or output format changed.\nOutput: {output}")
            return

        # [Test 2] SELECT (Search)
        print("\n[Test 2] SELECT (Vector Search)")
        cmd = 'SELECT FROM VECTORS WHERE VECTOR NEAR [0.1, 0.1, 0.5] LIMIT 1'
        output = self.run_cli_command(cmd)

        if captured_uuid in output:
             print("  [PASS] Found correct record via HNSW search.")
        else:
             print(f"  [FAIL] Search failed to find UUID.\nOutput: {output}")

        # [Test 3] UPDATE
        print("\n[Test 3] UPDATE (Set Payload)")
        cmd = f'UPDATE VECTORS SET PAYLOAD="Updated Payload" WHERE ID=\'{captured_uuid}\''
        output = self.run_cli_command(cmd)

        if "OK] Inserted ID" in output:
            print("  [PASS] Update confirmed.")
        else:
            print(f"  [FAIL] Update failed.\nOutput: {output}")

        # [Test 4] GET (Verify Update)
        print("\n[Test 4] GET (Verify Persistence)")
        cmd = f"GET '{captured_uuid}'"
        output = self.run_cli_command(cmd)

        if 'Payload: "Updated Payload"' in output:
            print("  [PASS] Payload verified.")
        else:
            print(f"  [FAIL] Payload mismatch.\nOutput: {output}")

        # [Test 5] HISTORY
        print("\n[Test 5] HISTORY (Time Travel)")
        cmd = f"HISTORY '{captured_uuid}'"
        output = self.run_cli_command(cmd)

        if "Initial Payload" in output and "Updated Payload" in output:
            print("  [PASS] Full history retrieved.")
        else:
            print(f"  [FAIL] History incomplete.\nOutput: {output}")

        # [Test 6] DELETE
        print("\n[Test 6] DELETE")
        cmd = f"DELETE FROM VECTORS WHERE ID='{captured_uuid}'"
        output = self.run_cli_command(cmd)

        if f"Deleted ID: {captured_uuid}" in output:
            print("  [PASS] Deletion confirmed.")
        else:
            print(f"  [FAIL] Delete failed.\nOutput: {output}")

        # [Test 7] Verify DELETE
        print("\n[Test 7] Verify Deletion")
        cmd = f"GET '{captured_uuid}'"
        output = self.run_cli_command(cmd)

        if "Result: <NULL>" in output or "ID Not Found" in output:
            print("  [PASS] Record is gone.")
        else:
            print(f"  [FAIL] Record still exists.\nOutput: {output}")

        # [Test 8] Parser Robustness
        print("\n[Test 8] Syntax Error Handling")
        output = self.run_cli_command("DROP DATABASE PROD")
        if "Syntax Error" in output:
            print("  [PASS] Parser correctly rejected garbage.")
        else:
             print(f"  [FAIL] Parser swallowed garbage.\nOutput: {output}")

if __name__ == "__main__":
    suite = ChronosIntegrationSuite()
    try:
        suite.setup()
        suite.run_tests()
    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
    except Exception as e:
        print(f"\n[!] Unexpected Error: {e}")
    finally:
        suite.teardown()
