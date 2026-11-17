"""Clear demo shared document BEFORE starting peers (never while a peer is writing)."""
from pathlib import Path

board = Path(__file__).resolve().parent / 'shared_whiteboard.txt'
board.write_text('', encoding='utf-8')
print(f'Reset shared document: {board}')
