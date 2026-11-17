"""End-to-end: four separate Python processes, 1 TCP port per process."""
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
procs = {}
logs = {i: [] for i in range(1, 5)}
checks = 0


def reader(i, process):
    for line in process.stdout:
        logs[i].append(line.rstrip())


def start(i):
    p = subprocess.Popen([sys.executable, '-u', 'main.py', '--id', str(i)],
                         cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, bufsize=1)
    procs[i] = p
    threading.Thread(target=reader, args=(i, p), daemon=True).start()


def send(i, command):
    procs[i].stdin.write(command + '\n')
    procs[i].stdin.flush()


def until(condition, duration=15):
    end = time.monotonic() + duration
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.05)
    return False


def check(condition, label):
    global checks
    if not condition:
        raise AssertionError(label)
    checks += 1
    print(f'PASS {checks}: {label}', flush=True)


try:
    (ROOT / 'shared_counter.txt').write_text('0\n', encoding='utf-8')
    for i in range(1, 5):
        start(i)
    check(until(lambda: all(any('Listening on' in l for l in logs[i]) for i in range(1, 5)), 8),
          '4 independent processes bind 4 TCP ports (one listener each)')
    check(until(lambda: all(any('Leader = 4' in l for l in logs[i]) for i in range(1, 5)), 18),
          'SV2 election chooses Node 4')
    send(1, 'chat Hello-integrated')
    check(until(lambda: all(any('[CHAT UI] Node 1: Hello-integrated' in l for l in logs[i]) for i in (2, 3, 4)), 7),
          'SV1 causal chat delivers on other 3 nodes')

    # Contention with real shared file. Each terminal's CLI performs request_cs synchronously,
    # while network requests are served in worker threads.
    for i in (1, 2, 3, 4):
        send(i, 'inc 0.30')
    check(until(lambda: sum(sum('SHARED COUNTER' in l for l in logs[i]) for i in range(1, 5)) == 4, 20),
          'SV3 four concurrent increments complete')
    check((ROOT / 'shared_counter.txt').read_text(encoding='utf-8').strip() == '4',
          'shared counter is exactly 4')
    check(all(any('>>> ENTER CS' in l for l in logs[i]) for i in range(1, 5)),
          'each node enters critical section')
    check(all(any('<<< EXIT CS' in l for l in logs[i]) for i in range(1, 5)),
          'each node leaves critical section')
    send(2, 'status')
    check(until(lambda: any("'mutex':" in l and "'chat':" in l for l in logs[2]), 4),
          'status includes chat, election and mutex state')

    # Lost peer must NOT be bypassed by mutex even though election reconfigures leader.
    procs[4].kill()
    procs[4].wait(timeout=3)
    check(until(lambda: all(any('Leader = 3' in l for l in logs[i]) for i in (1, 2, 3)), 18),
          'SV2 elects new leader Node 3 after Node 4 stops')
    before = (ROOT / 'shared_counter.txt').read_text().strip()
    send(1, 'inc 0')
    check(until(lambda: any('Increment NOT performed' in l for l in logs[1]), 8),
          'SV3 refuses CS without Node 4 permission')
    check((ROOT / 'shared_counter.txt').read_text().strip() == before,
          'counter unchanged when permission incomplete')
    send(2, 'chat After-failure')
    check(until(lambda: any('[CHAT UI] Node 2: After-failure' in l for l in logs[3]), 8),
          'SV1 chat still works among remaining nodes')
    print(f'RESULT: {checks}/{checks} PASS', flush=True)
except Exception as e:
    print('FAIL', repr(e), flush=True)
    for i in range(1, 5):
        print(f'=== NODE {i} ===')
        print('\n'.join(logs[i][-35:]))
    raise
finally:
    for p in procs.values():
        if p.poll() is None:
            p.kill()
    for p in procs.values():
        p.wait(timeout=4)
