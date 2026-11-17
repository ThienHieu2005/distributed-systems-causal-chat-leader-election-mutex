"""SV1 Chat + SV2 Bully + SV3 Ricart-Agrawala: ONE listener per process.

Quickstart: python reset_counter.py; then four terminals:
python main.py --id 1  # similarly --id 2, 3, 4
Commands: chat <text> | status | election | delay <node> <seconds> |
          cs [seconds] | inc [seconds] | read | write <text> | board | exit
"""
import argparse
import json
import socket
import threading
import time
from pathlib import Path

from modules.student1.chat_service import CausalChatService
from modules.student1.causal_multicast import MAX_MESSAGE_BYTES, NODES
from modules.student2.failure_detector_election import Node as BullyNode, FAILURE_TIMEOUT
from modules.student3.ricart_agrawala import Node as RicartAgrawalaNode
from app.shared_file import SharedFile


class SharedTransportMutex(RicartAgrawalaNode):
    """Reuse all RA state machine methods, replace only its transport.

    Its start_server() is NEVER called. Only CombinedNode binds TCP port.
    """

    def send(self, destination, msg):
        if destination not in self.peers or destination == self.node_id:
            return False
        message = dict(msg)
        if message.get('type') not in ('REQUEST', 'REPLY'):
            return False
        message['type'] = 'MUTEX_' + message['type']
        try:
            with socket.create_connection(self.peers[destination], timeout=1) as conn:
                conn.settimeout(2)
                conn.sendall((json.dumps(message) + '\n').encode('utf-8'))
            return True
        except OSError as exc:
            print(f'[Node {self.node_id}] Mutex SEND FAILED -> {destination}: {exc}', flush=True)
            return False

    def receive_mutex_message(self, message):
        sender = message.get('sender')
        stamp = message.get('timestamp')
        req_ts = message.get('request_ts')
        kind = message.get('type')
        if (type(sender) is not int or sender not in self.peers or sender == self.node_id
                or type(stamp) is not int or stamp < 1
                or type(req_ts) is not int or req_ts < 1):
            return
        if kind == 'MUTEX_REQUEST':
            self.on_request(sender, stamp, req_ts)
        elif kind == 'MUTEX_REPLY':
            self.on_reply(sender, stamp, req_ts)


class CombinedNode(CausalChatService):
    def __init__(self, node_id, counter_path='shared_counter.txt', on_deliver=None,
                 on_leader_change=None, board_path='shared_whiteboard.txt'):
        super().__init__(node_id, on_deliver=on_deliver)
        self.election = BullyNode(node_id)
        self.mutex = SharedTransportMutex(node_id)
        self.counter_path = Path(counter_path).resolve()
        self.shared_file = SharedFile(board_path)
        self._on_leader_change = on_leader_change
        self._watch_threads = []
        self._counter_operations = 0

    def handle_connection(self, conn):
        """All message types share the same TCP listener."""
        with conn:
            try:
                conn.settimeout(3)
                with conn.makefile('rb') as stream:
                    raw = stream.readline(MAX_MESSAGE_BYTES + 1)
                if not raw or len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b'\n'):
                    return
                msg = json.loads(raw.decode('utf-8'))
                if not isinstance(msg, dict):
                    return
                kind = msg.get('type')
                if kind == 'CHAT':
                    self.on_message(msg)
                    return
                sender = msg.get('sender')
                if type(sender) is not int or sender not in NODES or sender == self.node_id:
                    return
                if kind in ('MUTEX_REQUEST', 'MUTEX_REPLY'):
                    self.mutex.receive_mutex_message(msg)
                elif kind == 'HEARTBEAT':
                    self.election.receive_heartbeat(sender)
                elif kind == 'ELECTION':
                    if self.node_id > sender:
                        conn.sendall((json.dumps({'type': 'OK', 'sender': self.node_id}) + '\n').encode('utf-8'))
                        self.election.start_election()
                elif kind == 'COORDINATOR':
                    self.election.handle_coordinator(msg.get('leader'))
                else:
                    handler = self._handlers.get(kind)
                    if handler:
                        handler(msg)
            except (OSError, ValueError, UnicodeError, TypeError) as exc:
                print(f'[Node {self.node_id}] Incoming message error: {exc}', flush=True)

    def _watch_leader(self):
        last = object()
        while self.running and self.election.running:
            with self.election.lock:
                leader = self.election.current_leader
            if leader != last:
                last = leader
                if self._on_leader_change:
                    try:
                        self._on_leader_change(leader)
                    except Exception as exc:
                        print(f'[Node {self.node_id}] Leader callback error: {exc}', flush=True)
            time.sleep(0.2)

    def start(self):
        super().start()  # ONE TCP listener: from SV1
        # SV2: NO serve(); SV3: NO start_server().
        for target in (self.election.heartbeat_sender,
                       self.election.failure_monitor,
                       self.election.initial_election,
                       self._watch_leader):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._watch_threads.append(t)
        return self

    def run_cs(self, seconds=2, timeout=12):
        if not 0 <= seconds <= 30:
            raise ValueError('cs seconds must be in range 0..30')
        return self.mutex.request_cs(seconds=seconds, timeout=timeout)

    def increment_counter(self, delay=0.15, timeout=12):
        if not 0 <= delay <= 5:
            raise ValueError('increment delay must be 0..5')
        if not self.counter_path.is_file():
            raise FileNotFoundError(f'{self.counter_path}: run reset_counter.py first')

        def update():
            # CRITICAL SECTION: only RA permission guards the read-modify-write.
            with self.counter_path.open('r+', encoding='utf-8') as file:
                old = int(file.read().strip())
                time.sleep(delay)
                new = old + 1
                file.seek(0)
                file.write(str(new) + '\n')
                file.truncate()
                file.flush()
            self._counter_operations += 1
            print(f'[Node {self.node_id}] SHARED COUNTER {old} -> {new}', flush=True)

        return self.mutex.request_cs(seconds=0, timeout=timeout, work=update)

    def write_shared_file(self, content, timeout=12):
        """Append one line only while Ricart-Agrawala holds the CS.

        This is a single-host shared-file demo, not a replicated file system.
        """
        line = self.shared_file.validate_line(content)
        if not self.shared_file.path.is_file():
            raise FileNotFoundError(f'{self.shared_file.path}: run reset_board.py first')

        def update():
            self.shared_file.append_line(self.node_id, line)
            print(f'[Node {self.node_id}] SHARED FILE WRITE OK: {line}', flush=True)

        return self.mutex.request_cs(seconds=0, timeout=timeout, work=update)

    def read_shared_file(self):
        """Diagnostic read: not protected by the distributed lock."""
        return self.shared_file.read_text()

    def get_combined_status(self):
        chat = self.get_status()
        now = time.monotonic()
        with self.election.lock:
            leader = self.election.current_leader
            seen = self.election.last_seen.copy()
        members = {}
        for peer in NODES:
            if peer == self.node_id:
                members[peer] = 'ONLINE (self)'
            elif peer not in seen:
                members[peer] = 'UNKNOWN'
            elif now - seen[peer] > FAILURE_TIMEOUT:
                members[peer] = 'SUSPECTED'
            else:
                members[peer] = 'ONLINE'
        with self.mutex.guard:
            mutex = {'state': self.mutex.state, 'clock': self.mutex.clock,
                     'request_ts': self.mutex.request_ts,
                     'replies': sorted(self.mutex.replies),
                     'deferred': sorted(self.mutex.deferred)}
        return {'node_id': self.node_id, 'leader_id': leader, 'nodes': members,
                'chat': chat, 'mutex': mutex}

    def start_election(self):
        self.election.start_election()

    def stop(self):
        self.mutex.running.clear()
        self.election.running = False
        super().stop()
        for t in self._watch_threads:
            if t.is_alive():
                t.join(timeout=0.6)


def main(node_id, counter_path, board_path="shared_whiteboard.txt"):
    def display_chat(msg):
        print(f"[CHAT UI] Node {msg['sender']}: {msg['content']} (V={msg['vector']})", flush=True)

    node = CombinedNode(node_id, counter_path=counter_path, board_path=board_path, on_deliver=display_chat,
                        on_leader_change=lambda leader: print(f'[LEADER UI] Leader = {leader}', flush=True)).start()
    print('Commands: chat <text> | status | election | delay <id> <sec> | cs [sec] | inc [sec] | read | exit', flush=True)
    try:
        while True:
            try:
                cmd = input('> ').strip()
            except (EOFError, KeyboardInterrupt):
                break
            try:
                if cmd == 'exit':
                    break
                elif cmd == 'status':
                    print(node.get_combined_status(), flush=True)
                elif cmd == 'election':
                    node.start_election()
                elif cmd.startswith('chat '):
                    node.send_chat(cmd[5:])
                elif cmd == 'cs' or cmd.startswith('cs '):
                    parts = cmd.split()
                    if len(parts) > 2:
                        raise ValueError('Usage: cs [seconds]')
                    if not node.run_cs(float(parts[1]) if len(parts) == 2 else 2):
                        print(f'[Node {node_id}] CS NOT acquired', flush=True)
                elif cmd == 'inc' or cmd.startswith('inc '):
                    parts = cmd.split()
                    if len(parts) > 2:
                        raise ValueError('Usage: inc [seconds]')
                    if not node.increment_counter(float(parts[1]) if len(parts) == 2 else 0.15):
                        print(f'[Node {node_id}] Increment NOT performed', flush=True)
                elif cmd.startswith('write '):
                    if not node.write_shared_file(cmd[6:]):
                        print(f'[Node {node_id}] Shared file write NOT performed', flush=True)
                elif cmd == 'board':
                    print(f'[Node {node_id}] SHARED FILE (unprotected diagnostic read):\n'
                          f'{node.read_shared_file()}', flush=True)
                elif cmd == 'read':
                    print(f'[Node {node_id}] Counter (unprotected diagnostic read): '
                          f'{int(node.counter_path.read_text().strip())}', flush=True)
                elif cmd.startswith('delay '):
                    _, target, seconds = cmd.split()
                    target, seconds = int(target), float(seconds)
                    if target not in node.delays or not 0 <= seconds <= 30:
                        raise ValueError('Invalid target/delay')
                    with node.lock:
                        node.delays[target] = seconds
                    print(f'[Node {node_id}] Delay to Node {target} = {seconds}s', flush=True)
                else:
                    print('Commands: chat <text> | status | election | delay <id> <sec> | cs [sec] | inc [sec] | read | exit')
            except (ValueError, OSError, UnicodeError) as exc:
                print('Invalid command:', exc, flush=True)
    finally:
        node.stop()


