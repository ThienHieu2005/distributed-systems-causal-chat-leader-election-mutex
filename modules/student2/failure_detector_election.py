import argparse
import json
import socket
import threading
import time

NODES = {i: ("127.0.0.1", 5000 + i) for i in range(1, 5)}
HEARTBEAT_INTERVAL = 2
FAILURE_TIMEOUT = 6
COORDINATOR_TIMEOUT = 5
STARTUP_ELECTION_DELAY = 3


class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.host, self.port = NODES[node_id]
        self.running = True
        self.server = None
        self.ready = threading.Event()
        self.lock = threading.Lock()
        self.last_seen = {}
        self.suspected = set()
        self.current_leader = None
        self.election_running = False
        self.leader_event = threading.Event()

    def request(self, target_id, message, expect_reply=False):
        host, port = NODES[target_id]
        try:
            with socket.create_connection((host, port), timeout=1) as sock:
                sock.settimeout(1.5)
                sock.sendall((json.dumps(message) + "\n").encode("utf-8"))
                if not expect_reply:
                    return True
                with sock.makefile("r", encoding="utf-8") as stream:
                    line = stream.readline(4096)
                return json.loads(line) if line else None
        except (OSError, ValueError, UnicodeError):
            return None if expect_reply else False

    def serve(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen()
            self.server.settimeout(1)
            print(f"[Node {self.node_id}] Listening {self.host}:{self.port}", flush=True)
            self.ready.set()
            while self.running:
                try:
                    conn, _ = self.server.accept()
                    threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
                except socket.timeout:
                    pass
                except OSError:
                    break
        except OSError as exc:
            print("Cannot listen:", exc, flush=True)
            self.running = False
            self.ready.set()
        finally:
            if self.server:
                self.server.close()

    def handle_connection(self, conn):
        with conn:
            try:
                conn.settimeout(2)
                with conn.makefile("r", encoding="utf-8") as stream:
                    line = stream.readline(4096)
                if not line:
                    return
                data = json.loads(line)
                sender = data.get("sender")
                if sender not in NODES or sender == self.node_id:
                    return
                kind = data.get("type")
                if kind == "HEARTBEAT":
                    self.receive_heartbeat(sender)
                elif kind == "CHAT":
                    print(f"\n[Node {sender}] {data.get('content', '')}", flush=True)
                elif kind == "ELECTION":
                    print(f"\n[Node {self.node_id}] ELECTION from Node {sender}", flush=True)
                    if self.node_id > sender:
                        reply = {"type": "OK", "sender": self.node_id}
                        conn.sendall((json.dumps(reply) + "\n").encode("utf-8"))
                        print(f"[Node {self.node_id}] OK -> Node {sender}", flush=True)
                        self.start_election()
                elif kind == "COORDINATOR":
                    self.handle_coordinator(data.get("leader"))
            except (OSError, ValueError, UnicodeError):
                pass

    def receive_heartbeat(self, sender):
        now = time.monotonic()
        with self.lock:
            first = sender not in self.last_seen
            self.last_seen[sender] = now
            recovered = sender in self.suspected
            self.suspected.discard(sender)
            leader = self.current_leader
        if first or recovered:
            print(f"\n[Node {self.node_id}] Node {sender} ONLINE", flush=True)
        # If a higher-priority node returns, Bully should elect it.
        if leader is not None and sender > leader:
            print(f"[Node {self.node_id}] Higher Node {sender} appeared; re-electing", flush=True)
            self.start_election()

    def heartbeat_sender(self):
        while self.running:
            for target in NODES:
                if not self.running:
                    break
                if target != self.node_id:
                    self.request(target, {"type": "HEARTBEAT", "sender": self.node_id})
            for _ in range(20):
                if not self.running:
                    break
                time.sleep(HEARTBEAT_INTERVAL / 20)

    def failure_monitor(self):
        while self.running:
            now = time.monotonic()
            new_failures = []
            with self.lock:
                for other, seen in self.last_seen.items():
                    if now - seen > FAILURE_TIMEOUT and other not in self.suspected:
                        self.suspected.add(other)
                        new_failures.append(other)
            for other in new_failures:
                print(f"\n[Node {self.node_id}] SUSPECT Node {other}: heartbeat timeout", flush=True)
                with self.lock:
                    leader_failed = self.current_leader == other
                if leader_failed:
                    print(f"[Node {self.node_id}] Leader {other} failed; AUTO ELECTION", flush=True)
                    self.start_election()
            time.sleep(1)

    def initial_election(self):
        # Allow peers time to start before the first election.
        time.sleep(STARTUP_ELECTION_DELAY)
        if self.running:
            with self.lock:
                leader = self.current_leader
            if leader is None or leader < self.node_id:
                print(f"[Node {self.node_id}] Startup election", flush=True)
                self.start_election()

    def start_election(self):
        with self.lock:
            if self.election_running:
                return
            self.election_running = True
            self.current_leader = None
            self.leader_event.clear()
        threading.Thread(target=self.election_worker, daemon=True).start()

    def election_worker(self):
        try:
            while self.running:
                with self.lock:
                    if self.leader_event.is_set():
                        return
                print(f"\n[Node {self.node_id}] START ELECTION", flush=True)
                higher_replied = False
                for other in NODES:
                    if other <= self.node_id:
                        continue
                    print(f"[Node {self.node_id}] ELECTION -> Node {other}", flush=True)
                    reply = self.request(other, {"type": "ELECTION", "sender": self.node_id}, True)
                    if isinstance(reply, dict) and reply.get("type") == "OK" and reply.get("sender") == other:
                        print(f"[Node {self.node_id}] OK <- Node {other}", flush=True)
                        higher_replied = True
                if self.leader_event.is_set():
                    return
                if not higher_replied:
                    self.become_leader()
                    return
                print(f"[Node {self.node_id}] Waiting for COORDINATOR...", flush=True)
                if self.leader_event.wait(COORDINATOR_TIMEOUT):
                    return
                print(f"[Node {self.node_id}] No COORDINATOR; retry election", flush=True)
        finally:
            with self.lock:
                self.election_running = False

    def become_leader(self):
        with self.lock:
            if self.leader_event.is_set():
                return
            self.current_leader = self.node_id
            self.leader_event.set()
        print(f"\n[Node {self.node_id}] I AM NEW LEADER!", flush=True)
        message = {"type": "COORDINATOR", "sender": self.node_id, "leader": self.node_id}
        for other in NODES:
            if other != self.node_id:
                self.request(other, message)

    def handle_coordinator(self, leader):
        if leader not in NODES:
            return
        with self.lock:
            self.current_leader = leader
            self.leader_event.set()
        print(f"\n[Node {self.node_id}] COORDINATOR: Node {leader} is Leader", flush=True)

    def show_status(self):
        now = time.monotonic()
        with self.lock:
            seen = self.last_seen.copy()
            leader = self.current_leader
        print(f"\n===== NODE {self.node_id} STATUS =====")
        print("Leader:", leader if leader is not None else "UNKNOWN")
        for node_id in NODES:
            if node_id == self.node_id:
                status = "ONLINE (SELF)"
            elif node_id not in seen:
                status = "UNKNOWN"
            elif now - seen[node_id] > FAILURE_TIMEOUT:
                status = "SUSPECTED"
            else:
                status = "ONLINE"
            print(f"Node {node_id}: {status}")
        print("===========================\n")

    def run(self):
        threading.Thread(target=self.serve, daemon=True).start()
        self.ready.wait()
        if not self.running:
            return
        threading.Thread(target=self.heartbeat_sender, daemon=True).start()
        threading.Thread(target=self.failure_monitor, daemon=True).start()
        threading.Thread(target=self.initial_election, daemon=True).start()
        print("Commands: status | election | send <id> <text> | exit")
        try:
            while self.running:
                cmd = input("> ").strip()
                if cmd == "status":
                    self.show_status()
                elif cmd == "election":
                    self.start_election()
                elif cmd.startswith("send "):
                    parts = cmd.split(maxsplit=2)
                    if len(parts) != 3 or not parts[1].isdigit() or int(parts[1]) not in NODES:
                        print("Usage: send <node_id> <message>")
                        continue
                    target = int(parts[1])
                    if target == self.node_id:
                        print("Choose another node")
                        continue
                    ok = self.request(target, {"type": "CHAT", "sender": self.node_id, "content": parts[2]})
                    print("Sent" if ok else "Connection failed")
                elif cmd == "exit":
                    break
                else:
                    print("Commands: status | election | send <id> <text> | exit")
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self.running = False
            if self.server:
                self.server.close()
            print(f"Node {self.node_id} stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, choices=list(NODES))
    args = parser.parse_args()
    Node(args.id).run()
