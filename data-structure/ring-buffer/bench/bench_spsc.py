"""Benchmark for the batch SPSC ring buffer in spsc.py.

Run: python3 bench/bench_spsc.py [-n N] [--size SIZE] [--batch B] [--repeat R]

Scenarios (ops/s counts individual items, not batches):
  insert          producer only, N items in batches of B, buffer never fills
  insert+emit     fill to capacity in batches of B, emit(), repeat
  spsc-threaded   producer inserts batches (waits for room via len()),
                  consumer emit()s until everything has arrived
  deque baseline  collections.deque doing the same work (extend / drain)
"""
import argparse
import os
import sys
import threading
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spsc import Data, RingBuffer  # noqa: E402


def timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result


def chunks(items, batch):
    return [items[i : i + batch] for i in range(0, len(items), batch)]


def bench_insert(n, size, batch):
    rb = RingBuffer(n + 1)
    batches = chunks([Data(i) for i in range(n)], batch)
    insert = rb.insert

    def run():
        for b in batches:
            insert(b)
        return 0

    return timed(run)


def bench_insert_emit(n, size, batch):
    rb = RingBuffer(size)
    cap = size - 1
    batch = min(batch, cap)
    fills = chunks([Data(i) for i in range(n)], cap)
    insert, emit = rb.insert, rb.emit

    def run():
        drained = 0
        for fill in fills:
            for b in chunks(fill, batch):
                insert(b)
            drained += len(emit())
        return drained

    return timed(run)


def bench_threaded(n, size, batch):
    rb = RingBuffer(size)
    cap = size - 1
    batches = chunks([Data(i) for i in range(n)], min(batch, cap))
    done = threading.Event()
    drained = [0]

    def producer():
        insert, length = rb.insert, rb.len
        for b in batches:
            need = len(b)
            while cap - length() < need:
                pass
            insert(b)
        done.set()

    def consumer():
        emit = rb.emit
        while True:
            got = emit()
            if got:
                drained[0] += len(got)
            elif done.is_set() and rb.isEmpty():
                return

    def run():
        drained[0] = 0
        done.clear()
        rb.empty()
        tp, tc = threading.Thread(target=producer), threading.Thread(target=consumer)
        tc.start()
        tp.start()
        tp.join()
        tc.join()
        return drained[0]

    return timed(run)


def bench_deque_insert(n, size, batch):
    dq = deque()
    batches = chunks([Data(i) for i in range(n)], batch)
    extend = dq.extend

    def run():
        for b in batches:
            extend(b)
        return 0

    return timed(run)


def bench_deque_insert_emit(n, size, batch):
    dq = deque()
    cap = size - 1
    batch = min(batch, cap)
    fills = chunks([Data(i) for i in range(n)], cap)
    extend = dq.extend

    def run():
        drained = 0
        for fill in fills:
            for b in chunks(fill, batch):
                extend(b)
            out = list(dq)
            dq.clear()
            drained += len(out)
        return drained

    return timed(run)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=1_000_000, help="items to insert")
    ap.add_argument("--size", type=int, default=1024, help="ring buffer size")
    ap.add_argument("--batch", type=int, default=64, help="items per insert() call")
    ap.add_argument("--repeat", type=int, default=3, help="take best of N runs")
    args = ap.parse_args()

    scenarios = [
        ("RingBuffer insert", bench_insert),
        ("RingBuffer insert+emit", bench_insert_emit),
        ("RingBuffer spsc-threaded", bench_threaded),
        ("deque insert", bench_deque_insert),
        ("deque insert+emit", bench_deque_insert_emit),
    ]

    print(
        f"n={args.n:,} size={args.size} batch={args.batch} best-of-{args.repeat}"
        f"  (Python {sys.version.split()[0]})"
    )
    print(f"{'scenario':28} {'time (s)':>9} {'items/s':>13} {'ns/item':>8} {'drained':>10}")
    for name, fn in scenarios:
        best, drained = min(
            (fn(args.n, args.size, args.batch) for _ in range(args.repeat)), key=lambda r: r[0]
        )
        ops = args.n / best
        drained_col = f"{drained:,}" if drained else "-"
        print(f"{name:28} {best:9.4f} {ops:13,.0f} {1e9 / ops:8.0f} {drained_col:>10}")


if __name__ == "__main__":
    main()
