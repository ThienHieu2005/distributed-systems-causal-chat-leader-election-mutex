"""Day 7: causal multicast with vector timestamps + hold-back queue.

Run one copy per process:
    python main.py --id 1
    ...
    python main.py --id 4

Demo:
    Node 1: delay 3 5
    Node 1: chat A
    Wait until Node 2 prints DELIVER A.
    Node 2: chat B
    Node 3 receives B before A, holds B, then delivers A followed by B.
"""
import argparse
import json
import socket
import threading
import time

NODES = {i: ("127.0.0.1", 5000 + i) for i in range(1, 5)}
MAX_MESSAGE_BYTES = 8192


class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.host, self.port = NODES[node_id]
        self.running = True
        self.server = None
        self.ready = threading.Event()
        self.lock = threading.RLock()

        self.lamport = 0
        # delivered[i]: how many chat multicasts from node i+1 were delivered.
        # It is also the causal vector used for the next local multicast.
        self.delivered = [0] * len(NODES)
        self.holdback = []
        self.delays = {other: 0.0 for other in NODES if other != node_id}
        self.seen = set()

    def _send(self, target_id, message, delay):
        if delay:
            time.sleep(delay)
        if not self.running:
            return
        host, port = NODES[target_id]
        try:
            with socket.create_connection((host, port), timeout=2) as conn:
                conn.sendall((json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8"))
            print(f"[Node {self.node_id}] NETWORK SENT {message['id']} -> Node {target_id}", flush=True)
        except OSError as exc:
            print(f"[Node {self.node_id}] SEND FAILED {message['id']} -> Node {target_id}: {exc}", flush=True)

    def multicast(self, content):
        with self.lock:
            self.lamport += 1
            self.delivered[self.node_id - 1] += 1
            stamp = self.delivered.copy()
            timestamp = self.lamport
            message = {
                "type": "CHAT", "sender": self.node_id,
                "id": f"{self.node_id}:{stamp[self.node_id - 1]}",
                "content": content, "lamport": timestamp, "vector": stamp,
            }
            # Sender's own multicast is delivered immediately.
            self.seen.add(message["id"])
            print(f"[Node {self.node_id}] DELIVER {message['id']}: {content} | L={timestamp} V={stamp} (self)", flush=True)
            # A message from another sender might become deliverable now.
            self._drain_locked()
            delays = dict(self.delays)

        # Do not block Node 1 while A is intentionally delayed to Node 3.
        for target, delay in delays.items():
            threading.Thread(target=self._send, args=(target, message, delay), daemon=True).start()
        print(f"[Node {self.node_id}] MULTICAST {message['id']} to peers", flush=True)

    def _can_deliver(self, message):
        s = message["sender"] - 1
        v = message["vector"]
        return (
            v[s] == self.delivered[s] + 1
            and all(v[k] <= self.delivered[k] for k in range(len(v)) if k != s)
        )

    def _drain_locked(self):
        # Called with self.lock held; retry after every successful delivery.
        while True:
            for pos, message in enumerate(self.holdback):
                if self._can_deliver(message):
                    self.holdback.pop(pos)
                    sender = message["sender"] - 1
                    self.delivered[sender] += 1
                    print(
                        f"[Node {self.node_id}] DELIVER {message['id']}: "
                        f"{message['content']} | V={message['vector']} "
                        f"| delivered={self.delivered}",
                        flush=True,
                    )
                    break
            else:
                return

    def on_message(self, message):
        sender = message.get("sender")
        vector = message.get("vector")
        stamp = message.get("lamport")
        msg_id = message.get("id")
        content = message.get("content")
        if (message.get("type") != "CHAT" or type(sender) is not int
            or sender not in NODES or sender == self.node_id
            or not isinstance(content, str) or not isinstance(msg_id, str)
            or type(stamp) is not int or stamp < 0
            or not isinstance(vector, list) or len(vector) != len(NODES)
            or any(type(v) is not int or v < 0 for v in vector)
            or msg_id != f"{sender}:{vector[sender-1]}"):
            return

        with self.lock:
            if msg_id in self.seen:
                return
            self.seen.add(msg_id)
            self.lamport = max(self.lamport, stamp) + 1
            print(
                f"[Node {self.node_id}] RECEIVE {msg_id}: {content} "
                f"| incoming L={stamp} V={vector} | local L={self.lamport}",
                flush=True,
            )
            self.holdback.append(message)
            if not self._can_deliver(message):
                print(
                    f"[Node {self.node_id}] HOLD {msg_id} "
                    f"| delivered={self.delivered} | queue={len(self.holdback)}",
                    flush=True,
                )
            self._drain_locked()

    def handle_connection(self, conn):
        with conn:
            try:
                conn.settimeout(3)
                with conn.makefile("rb") as stream:
                    raw = stream.readline(MAX_MESSAGE_BYTES + 1)
                if not raw or not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE_BYTES:
                    return
                data = json.loads(raw.decode("utf-8"))
                if isinstance(data, dict):
                    self.on_message(data)
            except (OSError, ValueError, UnicodeError):
                return

    def start_server(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                self.server = server
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.bind((self.host, self.port))
                server.listen()
                server.settimeout(0.5)
                self.ready.set()
                print(f"[Node {self.node_id}] Listening on {self.host}:{self.port}", flush=True)
                while self.running:
                    try:
                        conn, _ = server.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        break
                    threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
        except OSError as exc:
            print(f"[Node {self.node_id}] Server failed: {exc}", flush=True)
            self.running = False
            self.ready.set()

    def status(self):
        with self.lock:
            print(
                f"[Node {self.node_id}] STATUS "
                f"Lamport={self.lamport} delivered={self.delivered} "
                f"holdback={[m['id'] for m in self.holdback]} "
                f"delays={self.delays}",
                flush=True,
            )

    def run(self):
        listener = threading.Thread(target=self.start_server, daemon=True)
        listener.start()
        self.ready.wait(3)
        if not self.running:
            return
        print("Commands: chat <text> | delay <node_id> <seconds> | status | event | exit", flush=True)
        try:
            while self.running:
                command = input("> ").strip()
                if command == "exit":
                    break
                elif command == "status":
                    self.status()
                elif command == "event":
                    # Internal event changes Lamport, but not causal multicast counters:
                    # non-multicast events are not received by other members.
                    with self.lock:
                        self.lamport += 1
                        print(f"[Node {self.node_id}] LOCAL EVENT L={self.lamport}", flush=True)
                elif command.startswith("chat "):
                    content = command[5:].strip()
                    if content:
                        self.multicast(content)
                elif command.startswith("delay "):
                    fields = command.split()
                    try:
                        target = int(fields[1])
                        delay = float(fields[2])
                        if len(fields) != 3 or target not in self.delays or not 0 <= delay <= 30:
                            raise ValueError()
                    except (ValueError, IndexError):
                        print("Usage: delay <other_node_id> <seconds 0..30>", flush=True)
                        continue
                    with self.lock:
                        self.delays[target] = delay
                    print(f"[Node {self.node_id}] Delay to Node {target} = {delay} s", flush=True)
                else:
                    print("Commands: chat <text> | delay <node_id> <seconds> | status | event | exit")
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self.running = False
            if self.server:
                self.server.close()
            listener.join(timeout=1)
            print(f"[Node {self.node_id}] Stopped", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, choices=list(NODES))
    args = parser.parse_args()
    Node(args.id).run()
