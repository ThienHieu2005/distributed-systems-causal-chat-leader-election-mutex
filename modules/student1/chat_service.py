"""Adapter Sinh vien 1 (Causal Chat) cho ung dung chung.

One process / one TCP listener / one Node.  Handler types other than CHAT can be
registered, so election & mutex modules do NOT bind the same port again.
"""
from __future__ import annotations

import json
import socket
import threading
import queue
from typing import Callable

from modules.student1.causal_multicast import Node, NODES, MAX_MESSAGE_BYTES


class CausalChatService(Node):
    def __init__(self, node_id: int, on_deliver: Callable[[dict], None] | None = None):
        super().__init__(node_id)
        self._handlers: dict[str, Callable[[dict], None]] = {}
        self._subscribers: list[Callable[[dict], None]] = []
        if on_deliver is not None:
            self._subscribers.append(on_deliver)
        self._deliveries: queue.Queue[dict | None] = queue.Queue()
        self._server_thread: threading.Thread | None = None
        self._callback_thread: threading.Thread | None = None

    def register_handler(self, message_type: str, handler: Callable[[dict], None]):
        """Route non-CHAT messages to another module; handler must validate payload."""
        if not isinstance(message_type, str) or not message_type or message_type == "CHAT":
            raise ValueError('message_type must be a nonempty type other than CHAT')
        self._handlers[message_type] = handler

    def subscribe_deliver(self, callback: Callable[[dict], None]):
        self._subscribers.append(callback)

    def _queue_delivery(self, message: dict):
        self._deliveries.put(dict(message))

    def _callback_worker(self):
        while True:
            event = self._deliveries.get()
            if event is None:
                break
            for callback in list(self._subscribers):
                try:
                    callback(dict(event))
                except Exception as exc:
                    print(f'[Node {self.node_id}] DELIVER CALLBACK ERROR: {exc}', flush=True)

    def multicast(self, content):
        # Same causal logic as day 8, plus a delivery event for the local UI.
        with self.lock:
            self.lamport += 1
            self.delivered[self.node_id - 1] += 1
            stamp = self.delivered.copy()
            timestamp = self.lamport
            message = {
                'type': 'CHAT', 'sender': self.node_id,
                'id': f'{self.node_id}:{stamp[self.node_id - 1]}',
                'content': content, 'lamport': timestamp, 'vector': stamp,
            }
            self.seen.add(message['id'])
            print(f"[Node {self.node_id}] DELIVER {message['id']}: {content} | L={timestamp} V={stamp} (self)", flush=True)
            self._queue_delivery(message)
            self._drain_locked()
            delays = dict(self.delays)
        for target, delay in delays.items():
            threading.Thread(target=self._send, args=(target, message, delay), daemon=True).start()
        print(f"[Node {self.node_id}] MULTICAST {message['id']} to peers", flush=True)

    def _drain_locked(self):
        while True:
            for pos, message in enumerate(self.holdback):
                if self._can_deliver(message):
                    self.holdback.pop(pos)
                    sender = message['sender'] - 1
                    self.delivered[sender] += 1
                    print(f"[Node {self.node_id}] DELIVER {message['id']}: {message['content']} "
                          f"| V={message['vector']} | delivered={self.delivered}", flush=True)
                    self._queue_delivery(message)
                    break
            else:
                return

    def handle_connection(self, conn):
        with conn:
            try:
                conn.settimeout(3)
                with conn.makefile('rb') as stream:
                    raw = stream.readline(MAX_MESSAGE_BYTES + 1)
                if not raw or not raw.endswith(b'\n') or len(raw) > MAX_MESSAGE_BYTES:
                    return
                data = json.loads(raw.decode('utf-8'))
                if not isinstance(data, dict):
                    return
                typ = data.get('type')
                if typ == 'CHAT':
                    self.on_message(data)
                elif isinstance(typ, str):
                    handler = self._handlers.get(typ)
                    if handler is not None:
                        handler(data)
            except (OSError, ValueError, UnicodeError):
                return

    def start(self):
        self._callback_thread = threading.Thread(target=self._callback_worker, daemon=True)
        self._callback_thread.start()
        self._server_thread = threading.Thread(target=self.start_server, daemon=True)
        self._server_thread.start()
        if not self.ready.wait(3) or not self.running:
            raise RuntimeError(f'Node {self.node_id} could not listen on {self.host}:{self.port}')
        return self

    def send_chat(self, content: str):
        if not isinstance(content, str) or not content.strip():
            raise ValueError('Chat content must not be empty')
        self.multicast(content.strip())

    def send_module_message(self, target_id: int, message_type: str, payload: dict):
        """Example routing helper, NOT a replacement for election/mutex protocols.

        It does not implement ACK / RPC or reliability; caller handles those.
        """
        if target_id not in NODES or target_id == self.node_id:
            raise ValueError('Invalid target node ID')
        if not isinstance(message_type, str) or not message_type or message_type == 'CHAT':
            raise ValueError('Invalid message type')
        data = {'type': message_type, 'sender': self.node_id, **payload}
        if data['type'] != message_type or data['sender'] != self.node_id:
            raise ValueError('Payload cannot override type or sender')
        with socket.create_connection(NODES[target_id], timeout=2) as conn:
            conn.sendall((json.dumps(data, ensure_ascii=False) + '\n').encode('utf-8'))

    def get_status(self):
        with self.lock:
            return {
                'node_id': self.node_id,
                'lamport': self.lamport,
                'delivered': list(self.delivered),
                'holdback': [m['id'] for m in self.holdback],
            }

    def stop(self):
        self.running = False
        if self.server:
            self.server.close()
        if self._server_thread:
            self._server_thread.join(timeout=2)
        self._deliveries.put(None)
        if self._callback_thread:
            self._callback_thread.join(timeout=2)


if __name__ == '__main__':
    raise SystemExit('Run python main.py --id 1, not chat_service.py')
