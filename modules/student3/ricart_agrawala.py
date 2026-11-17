"""SV3 Day 4: Ricart-Agrawala distributed mutual exclusion (fixed membership).

Run the full integrated app: python main.py --id 1 (then 2, 3, 4)
Commands: cs [seconds], status, event, exit
Educational limitations: all four peers must be online, TCP on localhost,
no crash recovery, retransmission, leases or membership changes.
"""
import argparse
import json
import socket
import threading
import time

NODES = {i: ('127.0.0.1', 5000 + i) for i in range(1, 5)}
MAX_LINE = 4096


class Node:
    def __init__(self, node_id, peers=None, on_enter=None, on_exit=None):
        self.node_id = node_id
        self.peers = dict(NODES if peers is None else peers)
        self.host, self.port = self.peers[node_id]
        self.clock = 0
        self.state = 'RELEASED'  # RELEASED -> WANTED -> HELD -> RELEASED
        self.request_ts = None
        self.replies = set()
        self.deferred = set()
        self.guard = threading.RLock()  # thread safety WITHIN this process only
        self.reply_arrived = threading.Condition(self.guard)
        self.running = threading.Event()
        self.running.set()
        self.ready = threading.Event()
        self.on_enter = on_enter
        self.on_exit = on_exit

    @staticmethod
    def read_json(conn):
        with conn.makefile('rb') as stream:
            raw = stream.readline(MAX_LINE + 1)
        if not raw or len(raw) > MAX_LINE or not raw.endswith(b'\n'):
            raise ValueError('Invalid newline-delimited JSON')
        obj = json.loads(raw.decode('utf-8'))
        if not isinstance(obj, dict):
            raise ValueError('Expected JSON object')
        return obj

    def send(self, destination, msg):
        # Independent one-way TCP connections allow delayed replies.
        try:
            with socket.create_connection(self.peers[destination], timeout=1) as conn:
                conn.settimeout(2)
                conn.sendall((json.dumps(msg) + '\n').encode('utf-8'))
            return True
        except OSError as exc:
            print(f'[Node {self.node_id}] Cannot reach Node {destination}: {exc}', flush=True)
            return False

    def make_message_locked(self, kind, **fields):
        # Caller must hold guard. Sending a message increments Lamport clock.
        self.clock += 1
        return {'type': kind, 'sender': self.node_id,
                'timestamp': self.clock, **fields}

    def receive_lamport_locked(self, timestamp):
        # Caller must hold guard.
        self.clock = max(self.clock, timestamp) + 1

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            server.settimeout(0.2)
            self.ready.set()
            print(f'[Node {self.node_id}] Listening on {self.host}:{self.port}', flush=True)
            while self.running.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                threading.Thread(target=self.handle_connection,
                                 args=(conn,), daemon=True).start()

    def handle_connection(self, conn):
        try:
            with conn:
                conn.settimeout(3)
                msg = self.read_json(conn)
            sender, stamp, kind = msg.get('sender'), msg.get('timestamp'), msg.get('type')
            if (sender not in self.peers or sender == self.node_id or
                    type(stamp) is not int or stamp < 1):
                return
            if kind == 'REQUEST':
                req_ts = msg.get('request_ts')
                if type(req_ts) is not int or req_ts < 1:
                    return
                self.on_request(sender, stamp, req_ts)
            elif kind == 'REPLY':
                req_ts = msg.get('request_ts')
                if type(req_ts) is not int or req_ts < 1:
                    return
                self.on_reply(sender, stamp, req_ts)
        except (OSError, ValueError, UnicodeError) as exc:
            print(f'[Node {self.node_id}] Receive error: {exc}', flush=True)

    def on_request(self, sender, stamp, req_ts):
        with self.guard:
            self.receive_lamport_locked(stamp)
            own_priority = (self.request_ts, self.node_id) if self.request_ts is not None else None
            incoming_priority = (req_ts, sender)
            defer = (self.state == 'HELD' or
                     (self.state == 'WANTED' and own_priority < incoming_priority))
            if defer:
                self.deferred.add((sender, req_ts))
                print(f'[Node {self.node_id}] DEFER Node {sender} request {incoming_priority}; '
                      f'state={self.state}', flush=True)
                return
            reply = self.make_message_locked('REPLY', request_ts=req_ts)
        print(f'[Node {self.node_id}] REPLY to Node {sender} request {incoming_priority}', flush=True)
        self.send(sender, reply)

    def on_reply(self, sender, stamp, req_ts):
        with self.guard:
            self.receive_lamport_locked(stamp)
            if self.state != 'WANTED' or self.request_ts != req_ts:
                print(f'[Node {self.node_id}] Ignore stale REPLY from Node {sender}', flush=True)
                return
            self.replies.add(sender)
            print(f'[Node {self.node_id}] REPLY from Node {sender}: '
                  f'{len(self.replies)}/{len(self.peers)-1}', flush=True)
            self.reply_arrived.notify_all()

    def release(self):
        with self.guard:
            self.state = 'RELEASED'
            self.request_ts = None
            self.replies.clear()
            deferred = sorted(self.deferred)
            self.deferred.clear()
            prepared = [(target, self.make_message_locked('REPLY', request_ts=req_ts))
                        for target, req_ts in deferred]
        for target, message in prepared:
            print(f'[Node {self.node_id}] SEND DEFERRED REPLY to Node {target}', flush=True)
            self.send(target, message)

    def request_cs(self, seconds=3, timeout=12, work=None):
        """Request CS; returns True on success, False on timeout/unreachable peer."""
        with self.guard:
            if self.state != 'RELEASED':
                print(f'[Node {self.node_id}] Already {self.state}', flush=True)
                return False
            self.state = 'WANTED'
            self.replies.clear()
            # One timestamp for the request, distinct from per-message send timestamps.
            self.clock += 1
            self.request_ts = self.clock
            req_ts = self.request_ts
            print(f'[Node {self.node_id}] WANTED request priority=({req_ts},{self.node_id})', flush=True)
            outbound = [(peer, self.make_message_locked('REQUEST', request_ts=req_ts))
                        for peer in self.peers if peer != self.node_id]
        all_sent = all(self.send(peer, msg) for peer, msg in outbound)
        if not all_sent:
            print(f'[Node {self.node_id}] REQUEST failed: all 4 peers required; no CS', flush=True)
            self.release()
            return False
        with self.reply_arrived:
            complete = self.reply_arrived.wait_for(
                lambda: len(self.replies) == len(self.peers)-1,
                timeout=timeout)
            if not complete:
                print(f'[Node {self.node_id}] TIMEOUT: not enough REPLY; no CS', flush=True)
            else:
                self.state = 'HELD'
                print(f'[Node {self.node_id}] >>> ENTER CS (request ts={req_ts})', flush=True)
        if not complete:
            self.release()
            return False
        try:
            if self.on_enter:
                self.on_enter(self.node_id)
            if work is not None:
                work()  # Actual shared-resource operation, while state=HELD.
            else:
                time.sleep(seconds)
            if self.on_exit:
                self.on_exit(self.node_id)
        finally:
            print(f'[Node {self.node_id}] <<< EXIT CS', flush=True)
            self.release()
        return True

    def command_loop(self):
        print('Commands: cs [seconds] | status | event | exit', flush=True)
        while self.running.is_set():
            try:
                tokens = input('> ').strip().split()
            except (KeyboardInterrupt, EOFError):
                break
            if not tokens:
                continue
            try:
                if tokens[0] == 'cs' and len(tokens) <= 2:
                    seconds = float(tokens[1]) if len(tokens) == 2 else 3
                    if seconds < 0 or seconds > 30:
                        raise ValueError('seconds must be between 0 and 30')
                    self.request_cs(seconds)
                elif tokens == ['event']:
                    with self.guard:
                        self.clock += 1
                        print(f'[Node {self.node_id}] Event -> L={self.clock}', flush=True)
                elif tokens == ['status']:
                    with self.guard:
                        print(f'[Node {self.node_id}] state={self.state}, L={self.clock}, '
                              f'priority={None if self.request_ts is None else (self.request_ts,self.node_id)}, '
                              f'replies={sorted(self.replies)}, deferred={sorted(self.deferred)}', flush=True)
                elif tokens == ['exit']:
                    break
                else:
                    print('Unknown command.', flush=True)
            except ValueError as exc:
                print(f'Invalid input: {exc}', flush=True)
        self.running.clear()
        print(f'[Node {self.node_id}] Stop.', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, required=True, choices=sorted(NODES))
    args = parser.parse_args()
    node = Node(args.id)
    threading.Thread(target=node.start_server, daemon=True).start()
    if not node.ready.wait(3):
        raise RuntimeError('TCP server did not start')
    node.command_loop()


if __name__ == '__main__':
    main()
