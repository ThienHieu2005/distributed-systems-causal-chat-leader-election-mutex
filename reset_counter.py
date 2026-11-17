"""Reset the shared file only before a test; do not use during active CS operations."""
from pathlib import Path
path = Path(__file__).resolve().parent / 'shared_counter.txt'
path.write_text('0\n', encoding='utf-8')
print(f'Reset {path} = 0')
