"""Four real peer processes: concurrent document appends and failed-member safety."""
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOARD = ROOT / 'shared_whiteboard.txt'
procs = {}
logs = {i: [] for i in range(1, 5)}
checks = 0


def reader(i, p):
    for line in p.stdout:
        logs[i].append(line.rstrip())


def start(i):
    p = subprocess.Popen([sys.executable, '-u', 'main.py', '--id', str(i)], cwd=ROOT,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, bufsize=1)
    procs[i] = p
    threading.Thread(target=reader, args=(i, p), daemon=True).start()


def send(i, command):
    procs[i].stdin.write(command + '\n')
    procs[i].stdin.flush()


def until(pred, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(.05)
    return False


def check(cond, label):
    global checks
    if not cond:
        raise AssertionError(label)
    checks += 1
    print(f'PASS {checks}: {label}', flush=True)


try:
    BOARD.write_text('', encoding='utf-8')
    for i in range(1, 5):
        start(i)
    check(until(lambda: all(any('Listening on' in line for line in logs[i])
                                for i in range(1, 5)), 8), 'Four processes started')
    check(until(lambda: all(any('Leader = 4' in line for line in logs[i])
                                for i in range(1, 5)), 18), 'Leader = Node 4')
    entries = {1: 'Noi dung Node mot', 2: 'Loi nhan Node hai',
               3: 'Cap nhat Node ba', 4: 'Ghi chu Node bon'}
    for i in range(1, 5):
        send(i, 'write ' + entries[i])
    check(until(lambda: sum(sum('SHARED FILE WRITE OK' in line for line in logs[i])
                            for i in range(1, 5)) == 4, 20), 'Four concurrent writes complete')
    lines = BOARD.read_text(encoding='utf-8').splitlines()
    check(len(lines) == 4, 'Exactly four document lines')
    check(set(lines) == {f'[Node {i}] {text}' for i, text in entries.items()},
          'Each document line appears exactly once with correct Node ID')
    check(all(any('>>> ENTER CS' in line for line in logs[i]) for i in range(1, 5)),
          'Every writer enters RA critical section')
    check(all(any('<<< EXIT CS' in line for line in logs[i]) for i in range(1, 5)),
          'Every writer exits RA critical section')
    send(3, 'board')
    check(until(lambda: any('SHARED FILE (unprotected diagnostic read)' in line for line in logs[3]), 3),
          'Board command displays shared document')
    send(1, 'write ' + ('x' * 501))
    check(until(lambda: any('exceeds 500' in line for line in logs[1]), 3),
          'Oversized content is rejected')
    check(len(BOARD.read_text(encoding='utf-8').splitlines()) == 4,
          'Invalid content does not modify shared file')
    procs[4].kill()
    procs[4].wait(timeout=3)
    check(until(lambda: all(any('Leader = 3' in line for line in logs[i])
                                for i in (1, 2, 3)), 18), 'Leader changes to Node 3')
    before = BOARD.read_bytes()
    send(2, 'write This must not be written')
    check(until(lambda: any('Shared file write NOT performed' in line for line in logs[2]), 8),
          'Write denied without permission from crashed Node 4')
    check(BOARD.read_bytes() == before, 'Shared file stays identical after refused write')
    print(f'RESULT: {checks}/{checks} PASS', flush=True)
except BaseException as exc:
    print('FAIL', repr(exc), flush=True)
    for i in range(1, 5):
        print(f'===== NODE {i} =====')
        print('\n'.join(logs[i][-35:]))
    raise
finally:
    for p in procs.values():
        if p.poll() is None:
            p.kill()
    for p in procs.values():
        p.wait(timeout=4)
