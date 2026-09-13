import random
import threading
import time
import unittest
from collections import deque
from typing import List


class Data:
    def __init__(self, price) -> None:
        self.price = price
        self.timestamp = int(time.time())


class RingBuffer:
    def __init__(self, size: int) -> None:
        self.ring = [None] * size
        self.size = size
        self.readPtr = 0
        self.writePtr = 0
        self.insertLock = threading.Lock()
        self.emitLock = threading.Lock()

    def insert(self, data: List[Data]) -> int:
        self.insertLock.acquire()
        room = self.size - self.len() - 1
        n = len(data)
        if n > room:
            n = room
        if n == 0:
            self.insertLock.release()

            return 0

        start = self.writePtr
        end = start + n
        if end <= self.size:
            self.ring[start:end] = data[:n]
        else:
            tail = self.size - start
            end -= self.size
            self.ring[start:] = data[:tail]
            self.ring[:end] = data[tail:n]

        nextWriteIdx = end
        if nextWriteIdx == self.size:
            nextWriteIdx = 0
        self.writePtr = nextWriteIdx

        self.insertLock.release()

        return n

    def emit(self) -> List[Data]:
        self.emitLock.acquire()
        length = self.len()
        if length == 0:
            self.emitLock.release()

            return []

        start = self.readPtr
        end = start + length
        if end <= self.size:
            data = self.ring[start:end]
            self.ring[start:end] = [None] * length
        else:
            tail = self.size - start
            end -= self.size
            data = self.ring[start:] + self.ring[:end]
            self.ring[start:] = [None] * tail
            self.ring[:end] = [None] * (length - tail)

        nextReadIdx = end
        if nextReadIdx == self.size:
            nextReadIdx = 0
        self.readPtr = nextReadIdx

        self.emitLock.release()

        return data

    def peek(self) -> Data:
        self.emitLock.acquire()
        if self.isEmpty():
            self.emitLock.release()

            return None

        data = self.ring[self.readPtr]
        self.emitLock.release()

        return data

    def empty(self):
        self.emitLock.acquire()
        self.insertLock.acquire()

        self.readPtr = 0
        self.writePtr = 0
        self.ring = [None] * self.size

        self.emitLock.release()
        self.insertLock.release()

        return

    def isEmpty(self) -> bool:
        return self.readPtr == self.writePtr

    def len(self) -> int:
        return (self.writePtr - self.readPtr) % self.size


# ---------------------------------------------------------------------------
# Tests. Run: python3 mpmc.py -v
#
# Expected semantics (bounded MPMC queue, batch API, reject-when-full):
#   - RingBuffer(size) holds up to `size - 1` unread items; one slot stays
#     free so that readPtr == writePtr unambiguously means "empty".
#   - insert(items) appends items in order until the buffer is full; the
#     accepted prefix is kept and the remaining items are dropped. Any number
#     of producer threads may call insert() concurrently; each call is atomic
#     and the buffer never exceeds capacity.
#   - emit() removes and returns every unread item in FIFO order ([] if empty).
#     Any number of consumer threads may call emit() concurrently; each
#     inserted item is returned by exactly one emit() call, never as None, and
#     within one consumer's stream items from the same producer keep the order
#     that producer inserted them.
#   - peek() returns the oldest unread item without consuming it, or None
#     when empty. It is linearizable with emit(): it never returns an item
#     that is not the head at some instant during the call.
#   - len() is the number of unread items; isEmpty() is len() == 0.
#   - empty() discards everything and resets to the initial state.
# ---------------------------------------------------------------------------


def capacity(size: int) -> int:
    return size - 1


def items(*ps) -> List[Data]:
    return [Data(p) for p in ps]


def prices(ds):
    return [d.price for d in ds]


class TestBasics(unittest.TestCase):
    def test_new_buffer_is_empty(self):
        rb = RingBuffer(4)
        self.assertTrue(rb.isEmpty())
        self.assertEqual(rb.len(), 0)
        self.assertEqual(rb.emit(), [])
        self.assertIsNone(rb.peek())

    def test_single_insert_then_emit(self):
        rb = RingBuffer(4)
        rb.insert(items(1))
        self.assertFalse(rb.isEmpty())
        self.assertEqual(rb.len(), 1)
        self.assertEqual(prices(rb.emit()), [1])
        self.assertTrue(rb.isEmpty())
        self.assertEqual(rb.emit(), [])

    def test_batch_insert_fifo_order(self):
        rb = RingBuffer(8)
        rb.insert(items(0, 1, 2))
        rb.insert(items(3, 4))
        self.assertEqual(rb.len(), 5)
        self.assertEqual(prices(rb.emit()), [0, 1, 2, 3, 4])

    def test_insert_empty_batch_is_noop(self):
        rb = RingBuffer(4)
        rb.insert(items(1))
        rb.insert([])
        self.assertEqual(rb.len(), 1)
        self.assertEqual(prices(rb.emit()), [1])

    def test_emit_on_empty_does_not_corrupt_state(self):
        rb = RingBuffer(4)
        self.assertEqual(rb.emit(), [])
        self.assertEqual(rb.emit(), [])
        rb.insert(items(7))
        self.assertEqual(prices(rb.emit()), [7])
        self.assertTrue(rb.isEmpty())

    def test_emit_clears_slots(self):
        rb = RingBuffer(4)
        rb.insert(items(1, 2))
        rb.emit()
        self.assertEqual(rb.ring, [None] * 4)

    def test_peek_does_not_consume(self):
        rb = RingBuffer(4)
        rb.insert(items(10, 20))
        self.assertEqual(rb.peek().price, 10)
        self.assertEqual(rb.peek().price, 10)
        self.assertEqual(prices(rb.emit()), [10, 20])
        self.assertIsNone(rb.peek())

    def test_empty_resets_buffer(self):
        rb = RingBuffer(4)
        rb.insert(items(1, 2))
        rb.empty()
        self.assertTrue(rb.isEmpty())
        self.assertEqual(rb.emit(), [])
        rb.insert(items(3))
        self.assertEqual(prices(rb.emit()), [3])

    def test_empty_restores_full_detection(self):
        size = 4
        rb = RingBuffer(size)
        rb.insert(items(1))
        rb.empty()
        rb.insert(items(*range(capacity(size))))
        rb.insert(items(99))  # full: must be rejected
        self.assertEqual(prices(rb.emit()), list(range(capacity(size))))

    def test_data_carries_timestamp(self):
        before = int(time.time())
        d = Data(1.5)
        self.assertEqual(d.price, 1.5)
        self.assertGreaterEqual(d.timestamp, before)


class TestCapacity(unittest.TestCase):
    def test_fill_to_capacity_in_one_batch(self):
        size = 4
        rb = RingBuffer(size)
        rb.insert(items(*range(capacity(size))))
        self.assertEqual(rb.len(), capacity(size))
        self.assertEqual(prices(rb.emit()), list(range(capacity(size))))

    def test_oversized_batch_keeps_prefix(self):
        size = 4
        rb = RingBuffer(size)
        rb.insert(items(0, 1, 2, 3, 4, 5))
        self.assertEqual(prices(rb.emit()), [0, 1, 2])

    def test_partial_batch_when_nearly_full(self):
        size = 4
        rb = RingBuffer(size)
        rb.insert(items(0, 1))
        rb.insert(items(2, 3, 4))  # only one slot left
        self.assertEqual(prices(rb.emit()), [0, 1, 2])

    def test_insert_when_full_is_rejected(self):
        size = 4
        rb = RingBuffer(size)
        rb.insert(items(*range(capacity(size))))
        rb.insert(items(99))
        self.assertEqual(rb.len(), capacity(size))
        self.assertEqual(rb.peek().price, 0)
        self.assertEqual(prices(rb.emit()), list(range(capacity(size))))

    def test_reject_then_accept_after_emit(self):
        rb = RingBuffer(3)
        rb.insert(items(0, 1, 2))  # 2 rejected
        self.assertEqual(prices(rb.emit()), [0, 1])
        rb.insert(items(3))
        self.assertEqual(prices(rb.emit()), [3])

    def test_size_one_holds_nothing(self):
        rb = RingBuffer(1)
        rb.insert(items(1))
        self.assertTrue(rb.isEmpty())
        self.assertEqual(rb.len(), 0)
        self.assertEqual(rb.emit(), [])


class TestWrapAround(unittest.TestCase):
    def test_pointers_wrap_after_drain(self):
        rb = RingBuffer(4)
        rb.insert(items(0, 1, 2))
        rb.emit()
        rb.insert(items(3, 4, 5))  # writes wrap around the end of the ring
        self.assertEqual(rb.len(), 3)
        self.assertEqual(prices(rb.emit()), [3, 4, 5])

    def test_partial_batch_across_wrap(self):
        rb = RingBuffer(4)
        rb.insert(items(0, 1))
        rb.emit()
        rb.insert(items(2, 3, 4, 5))  # slots 2, 3, 0 accepted; 5 rejected
        self.assertEqual(prices(rb.emit()), [2, 3, 4])

    def test_many_laps_alternating(self):
        rb = RingBuffer(3)
        for p in range(100):
            rb.insert(items(p))
            self.assertEqual(prices(rb.emit()), [p])
        self.assertTrue(rb.isEmpty())

    def test_many_laps_keeping_one_in_flight(self):
        rb = RingBuffer(3)
        rb.insert(items(0))
        for p in range(1, 100):
            rb.insert(items(p))
            self.assertEqual(rb.len(), 2)
            self.assertEqual(rb.peek().price, p - 1)
            self.assertEqual(prices(rb.emit()), [p - 1, p])
            rb.insert(items(p))  # keep one in flight for the next lap
        self.assertEqual(prices(rb.emit()), [99])

    def test_no_index_error_after_long_use(self):
        rb = RingBuffer(2)
        for p in range(50):
            rb.insert(items(p, p))
            rb.peek()
            rb.len()
            if p % 3 == 0:
                rb.emit()


class TestAgainstModel(unittest.TestCase):
    """Randomised batch insert/emit interleaving vs. a bounded deque model."""

    def test_random_interleaving_matches_model(self):
        rng = random.Random(1234)
        for size in (1, 2, 3, 7, 16):
            rb = RingBuffer(size)
            model = deque()
            counter = 0
            for _ in range(500):
                batch = []
                for _ in range(rng.randint(0, size + 2)):
                    batch.append(Data(counter))
                    if len(model) < capacity(size):
                        model.append(counter)
                    counter += 1
                rb.insert(batch)
                self.assertEqual(rb.len(), len(model), f"size={size} len mismatch")
                self.assertEqual(
                    rb.isEmpty(), not model, f"size={size} isEmpty mismatch"
                )
                if model:
                    self.assertEqual(
                        rb.peek().price, model[0], f"size={size} peek mismatch"
                    )
                if rng.random() < 0.6:
                    self.assertEqual(
                        prices(rb.emit()), list(model), f"size={size} emit mismatch"
                    )
                    model.clear()


class YieldingRing(list):
    """Drop-in for RingBuffer.ring that yields the GIL inside every slot
    read/write. On CPython >= 3.12 a thread switch can only happen at a
    function entry or a loop back-edge, and insert()/emit() have neither
    between reading a pointer and publishing it, so without this hook even an
    *unlocked* insert() passes every threaded test by accident."""

    def __getitem__(self, i):
        time.sleep(0)
        return list.__getitem__(self, i)

    def __setitem__(self, i, v):
        time.sleep(0)
        list.__setitem__(self, i, v)


def make_buffer(size: int) -> RingBuffer:
    rb = RingBuffer(size)
    rb.ring = YieldingRing(rb.ring)
    return rb


def run_producers_consumers(n_per_producer, size, batch, producers, consumers, timeout=30):
    """`producers` threads each insert `n_per_producer` items (price =
    (producer_id, seq)) in batches, retrying the rejected suffix until every
    item is accepted; `consumers` threads emit() until everything has arrived.
    Returns the per-consumer received lists and the per-producer accepted counts."""
    rb = make_buffer(size)
    received = [[] for _ in range(consumers)]
    accepted = [0] * producers
    remaining = [producers]
    remaining_lock = threading.Lock()
    done = threading.Event()
    deadline = time.perf_counter() + timeout

    def producer(pid):
        payload = [Data((pid, i)) for i in range(n_per_producer)]
        pos = 0
        while pos < n_per_producer and time.perf_counter() < deadline:
            n = rb.insert(payload[pos : pos + batch])
            accepted[pid] += n
            pos += n
            if n == 0:
                time.sleep(0)  # buffer full: yield so consumers can drain
        with remaining_lock:
            remaining[0] -= 1
            if remaining[0] == 0:
                done.set()

    def consumer(cid):
        while time.perf_counter() < deadline:
            got = rb.emit()
            if got:
                received[cid].extend(got)
            elif done.is_set() and rb.isEmpty():
                return
            else:
                time.sleep(0)

    threads = [threading.Thread(target=producer, args=(i,)) for i in range(producers)]
    threads += [threading.Thread(target=consumer, args=(i,)) for i in range(consumers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return received, accepted


class TestMultiProducerMultiConsumer(unittest.TestCase):

    def check_exactly_once_and_fifo(self, received, producers, n_per_producer):
        flat = [d for r in received for d in r]
        self.assertNotIn(None, flat, "emit() returned a None slot")
        got = prices(flat)
        expected = sorted((p, i) for p in range(producers) for i in range(n_per_producer))
        self.assertEqual(len(got), len(expected), "item count mismatch (lost or duplicated)")
        self.assertEqual(sorted(got), expected, "lost or duplicated items")
        for cid, r in enumerate(received):
            for pid in range(producers):
                seq = [i for (p, i) in prices(r) if p == pid]
                self.assertEqual(
                    seq, sorted(seq), f"consumer {cid} saw producer {pid} items out of order"
                )

    def test_spsc_preserves_order(self):
        received, accepted = run_producers_consumers(20_000, 32, 8, producers=1, consumers=1)
        self.assertEqual(accepted, [20_000])
        self.assertEqual(prices(received[0]), [(0, i) for i in range(20_000)])

    def test_mpsc_each_item_exactly_once(self):
        received, accepted = run_producers_consumers(5_000, 32, 8, producers=4, consumers=1)
        self.assertEqual(accepted, [5_000] * 4)
        self.check_exactly_once_and_fifo(received, 4, 5_000)
        # Single consumer: every producer's items are globally in order.
        for pid in range(4):
            self.assertEqual(
                [i for (p, i) in prices(received[0]) if p == pid], list(range(5_000))
            )

    def test_spmc_each_item_exactly_once(self):
        received, accepted = run_producers_consumers(20_000, 32, 8, producers=1, consumers=4)
        self.assertEqual(accepted, [20_000])
        self.check_exactly_once_and_fifo(received, 1, 20_000)

    def test_mpmc_each_item_exactly_once(self):
        received, accepted = run_producers_consumers(5_000, 32, 8, producers=4, consumers=4)
        self.assertEqual(accepted, [5_000] * 4)
        self.check_exactly_once_and_fifo(received, 4, 5_000)

    def test_mpmc_small_ring_heavy_contention(self):
        # Tiny ring: producers are rejected constantly, consumers race on every item.
        received, accepted = run_producers_consumers(1_000, 4, 2, producers=8, consumers=8)
        self.assertEqual(accepted, [1_000] * 8)
        self.check_exactly_once_and_fifo(received, 8, 1_000)

    def test_mpmc_large_ring_no_backpressure(self):
        n = 5_000
        received, accepted = run_producers_consumers(n, 4 * n + 1, 64, producers=4, consumers=4)
        self.assertEqual(accepted, [n] * 4)
        self.check_exactly_once_and_fifo(received, 4, n)

    def test_len_never_exceeds_capacity_under_contention(self):
        size = 16
        rb = make_buffer(size)
        stop = threading.Event()
        violations = [0]
        drained = [0]
        lock = threading.Lock()

        def producer(pid):
            i = 0
            while not stop.is_set():
                rb.insert(items((pid, i)))
                i += 1
                if rb.len() > capacity(size):
                    violations[0] += 1

        def consumer():
            while not stop.is_set():
                got = rb.emit()
                if got:
                    with lock:
                        drained[0] += len(got)
                if rb.len() > capacity(size):
                    violations[0] += 1
                time.sleep(0)

        threads = [threading.Thread(target=producer, args=(i,)) for i in range(4)]
        threads += [threading.Thread(target=consumer) for _ in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.5)
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(violations[0], 0, "len() exceeded capacity: a pointer overran the other")
        self.assertGreater(drained[0], 0)
        self.assertFalse(rb.insertLock.locked(), "insert() leaked insertLock")
        self.assertFalse(rb.emitLock.locked(), "emit() leaked emitLock")

    def test_concurrent_insert_on_static_buffer(self):
        """No consumer running: N producers race for room for exactly one batch;
        exactly one wins, the rest are rejected whole."""
        for _ in range(50):
            rb = make_buffer(9)
            results = []
            barrier = threading.Barrier(8)

            def producer(pid):
                barrier.wait()
                results.append((pid, rb.insert(items(*[(pid, i) for i in range(8)]))))

            threads = [threading.Thread(target=producer, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            winners = [pid for pid, n in results if n == 8]
            self.assertEqual(len(winners), 1, "more than one producer's batch was accepted")
            self.assertEqual(sum(n for _, n in results), 8)
            self.assertEqual(prices(rb.emit()), [(winners[0], i) for i in range(8)])

    def test_concurrent_emit_on_static_buffer(self):
        """No producer running: N consumers race on emit(); exactly one wins."""
        for _ in range(50):
            rb = make_buffer(64)
            rb.insert(items(*range(63)))
            results = []
            barrier = threading.Barrier(8)

            def consumer():
                barrier.wait()
                results.append(rb.emit())

            threads = [threading.Thread(target=consumer) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            non_empty = [r for r in results if r]
            self.assertEqual(len(non_empty), 1, "more than one consumer received the batch")
            self.assertEqual(prices(non_empty[0]), list(range(63)))
            self.assertTrue(rb.isEmpty())

    def test_peek_holds_lock_across_slot_read(self):
        """Deterministic: pause peek() at its slot read and check that a
        concurrent emit() cannot proceed until peek() returns. Without the
        lock held across the read, peek() can return None or a non-head item."""
        in_read, resume = threading.Event(), threading.Event()

        class PausingRing(list):
            def __getitem__(self, i):
                if isinstance(i, int):  # only peek() does a scalar read
                    in_read.set()
                    resume.wait()
                return list.__getitem__(self, i)

        rb = make_buffer(4)
        rb.ring = PausingRing(rb.ring)
        rb.insert(items("x"))
        out = []
        t_peek = threading.Thread(target=lambda: out.append(rb.peek()))
        t_peek.start()
        self.assertTrue(in_read.wait(5), "peek() never reached the slot read")
        emitted = []
        t_emit = threading.Thread(target=lambda: emitted.append(rb.emit()))
        t_emit.start()
        t_emit.join(0.2)
        self.assertTrue(t_emit.is_alive(), "emit() ran while peek() was reading the slot")
        resume.set()
        t_peek.join(5)
        t_emit.join(5)
        self.assertEqual(out[0].price, "x")
        self.assertEqual(prices(emitted[0]), ["x"])


if __name__ == "__main__":
    unittest.main()
