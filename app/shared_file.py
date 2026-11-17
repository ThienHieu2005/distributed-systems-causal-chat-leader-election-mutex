"""Local shared document demo. Writes MUST be guarded by distributed RA lock.

The file is physically shared only by the four localhost processes running with
one common path. No replication, atomic cross-host storage or consistency layer.
"""
from pathlib import Path


class SharedFile:
    def __init__(self, path='shared_whiteboard.txt'):
        self.path = Path(path).resolve()

    @staticmethod
    def validate_line(content):
        line = content.strip()
        if not line:
            raise ValueError('Usage: write <nonempty text>')
        if '\n' in line or '\r' in line or '\x00' in line:
            raise ValueError('Write content must be one line without NUL')
        if len(line.encode('utf-8')) > 500:
            raise ValueError('Write content exceeds 500 UTF-8 bytes')
        return line

    def append_line(self, node_id, content):
        line = self.validate_line(content)
        if not self.path.is_file():
            raise FileNotFoundError(f'{self.path}: run reset_board.py before starting the nodes')
        # Must be called only inside mutex.request_cs(work=...) while state=HELD.
        with self.path.open('a', encoding='utf-8') as stream:
            stream.write(f'[Node {node_id}] {line}\n')
            stream.flush()

    def read_text(self):
        if not self.path.is_file():
            raise FileNotFoundError(f'{self.path}: run reset_board.py first')
        return self.path.read_text(encoding='utf-8')
