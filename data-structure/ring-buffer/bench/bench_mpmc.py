"""Benchmark for the batch MPMC ring buffer in mpmc.py.

Run: python3 bench/bench_mpmc.py [-n N] [--size SIZE] [--batch B] [--repeat R]
                                 [--producers P] [--consumers C]

Scenarios (items/s counts individual items, not batches):
  insert          single producer, N items in batches of B, buffer never fills
                  (measures the cost of the uncontended insertLock)
  insert+emit     fill to capacity in batches of B, emit(), repeat
                  (single thread; both locks uncontended)
  spsc-threaded   1 producer / 1 consumer
  mpsc-threaded   P producers / 1 consumer
  spmc-threaded   1 producer / C consumers
  mpmc-threaded   P producers / C consumers
                  Producers split N evenly and retry their rejected suffix;
                  consumers emit() until everything has arrived.
  deque baseline  collections.deque doing the same work (extend / drain);
                  the threaded deque variant guards every call with one Lock
"""
import argparse
import os
import sys
import threading
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mpmc import Data, RingBuffer  # noqa: E402


def timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result


def chunks(items, batch):
    return [items[i : i + batch] for i in range(0, len(items), batch)]


def split(n, parts):
    base, extra = divmod(n, parts)
    return [base + (1 if i < extra else 0) for i in range(parts)]


def bench_insert(n, size, batch, producers, consumers):
    rb = RingBuffer(n + 1)
    batches = chunks([Data(i) for i in range(n)], batch)
    insert = rb.insert

    def run():
        for b in batches:
            insert(b)
        return 0

    return timed(run)


def bench_insert_emit(n, size, batch, producers, consumers):
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


def bench_threaded(n, size, batch, producers, consumers):
    rb = RingBuffer(size)
    batch = min(batch, size - 1)
    payloads = [[Data((pid, i)) for i in range(c)] for pid, c in enumerate(split(n, producers))]
    remaining = [producers]
    remaining_lock = threading.Lock()
    done = threading.Event()
    drained = [0] * consumers

    def producer(payload):
        insert = rb.insert
        pos, total = 0, len(payload)
        while pos < total:
            got = insert(payload[pos : pos + batch])
            if got:
                pos += got
            else:
                time.sleep(0)
        with remaining_lock:
            remaining[0] -= 1
            if remaining[0] == 0:
                done.set()

    def consumer(i):
        emit = rb.emit
        while True:
            got = emit()
            if got:
                drained[i] += len(got)
            elif done.is_set() and rb.isEmpty():
                return
            else:
                time.sleep(0)

    def run():
        for i in range(consumers):
            drained[i] = 0
        remaining[0] = producers
        done.clear()
        rb.empty()
        threads = [threading.Thread(target=producer, args=(p,)) for p in payloads]
        threads += [threading.Thread(target=consumer, args=(i,)) for i in range(consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return sum(drained)

    return timed(run)


def bench_deque_insert(n, size, batch, producers, consumers):
    dq = deque()
    batches = chunks([Data(i) for i in range(n)], batch)
    extend = dq.extend

    def run():
        for b in batches:
            extend(b)
        return 0

    return timed(run)


def bench_deque_insert_emit(n, size, batch, producers, consumers):
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


def bench_deque_threaded(n, size, batch, producers, consumers):
    """Bounded deque + one Lock around every call: the naive MPMC baseline."""
    dq = deque()
    lock = threading.Lock()
    cap = size - 1
    batch = min(batch, cap)
    payloads = [chunks([Data((pid, i)) for i in range(c)], batch) for pid, c in enumerate(split(n, producers))]
    remaining = [producers]
    remaining_lock = threading.Lock()
    done = threading.Event()
    drained = [0] * consumers

    def producer(batches):
        for b in batches:
            while True:
                with lock:
                    if len(dq) + len(b) <= cap:
                        dq.extend(b)
                        break
                time.sleep(0)
        with remaining_lock:
            remaining[0] -= 1
            if remaining[0] == 0:
                done.set()

    def consumer(i):
        while True:
            with lock:
                got = list(dq)
                dq.clear()
            if got:
                drained[i] += len(got)
            elif done.is_set() and not dq:
                return
            else:
                time.sleep(0)

    def run():
        for i in range(consumers):
            drained[i] = 0
        remaining[0] = producers
        done.clear()
        dq.clear()
        threads = [threading.Thread(target=producer, args=(p,)) for p in payloads]
        threads += [threading.Thread(target=consumer, args=(i,)) for i in range(consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return sum(drained)

    return timed(run)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=1_000_000, help="items to insert")
    ap.add_argument("--size", type=int, default=1024, help="ring buffer size")
    ap.add_argument("--batch", type=int, default=64, help="items per insert() call")
    ap.add_argument("--repeat", type=int, default=3, help="take best of N runs")
    ap.add_argument("--producers", type=int, default=4, help="producer threads for mp* scenarios")
    ap.add_argument("--consumers", type=int, default=4, help="consumer threads for *mc scenarios")
    args = ap.parse_args()
    P, C = args.producers, args.consumers

    def threaded(p, c):
        return lambda n, size, batch, _p, _c: bench_threaded(n, size, batch, p, c)

    scenarios = [
        ("RingBuffer insert", bench_insert),
        ("RingBuffer insert+emit", bench_insert_emit),
        ("RingBuffer spsc-threaded", threaded(1, 1)),
        (f"RingBuffer mpsc-threaded {P}p/1c", threaded(P, 1)),
        (f"RingBuffer spmc-threaded 1p/{C}c", threaded(1, C)),
        (f"RingBuffer mpmc-threaded {P}p/{C}c", threaded(P, C)),
        ("deque insert", bench_deque_insert),
        ("deque insert+emit", bench_deque_insert_emit),
        (f"deque+Lock mpmc-threaded {P}p/{C}c", bench_deque_threaded),
    ]

    print(
        f"n={args.n:,} size={args.size} batch={args.batch} producers={P} consumers={C}"
        f" best-of-{args.repeat}  (Python {sys.version.split()[0]})"
    )
    print(f"{'scenario':34} {'time (s)':>9} {'items/s':>13} {'ns/item':>8} {'drained':>10}")
    for name, fn in scenarios:
        best, drained = min(
            (fn(args.n, args.size, args.batch, P, C) for _ in range(args.repeat)),
            key=lambda r: r[0],
        )
        ops = args.n / best
        drained_col = f"{drained:,}" if drained else "-"
        print(f"{name:34} {best:9.4f} {ops:13,.0f} {1e9 / ops:8.0f} {drained_col:>10}")


if __name__ == "__main__":
    main()
