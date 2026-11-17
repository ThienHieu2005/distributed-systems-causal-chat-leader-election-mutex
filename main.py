"""Track D: khởi chạy một peer gồm cả ba module.

Ví dụ: python main.py --id 1
"""
import argparse
from app.peer_node import main
from modules.student1.causal_multicast import NODES

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Track D — 3 modules, one TCP listener per peer')
    parser.add_argument('--id', type=int, required=True, choices=list(NODES))
    parser.add_argument('--counter', default='shared_counter.txt')
    parser.add_argument('--board', default='shared_whiteboard.txt')
    args = parser.parse_args()
    main(args.id, args.counter, args.board)
