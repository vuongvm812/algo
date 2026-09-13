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
        self.lock = threading.Lock()
        self.readPtr = 0
        self.writePtr = 0

    def insert(self, data: List[Data]) -> int:
        room = self.size - self.len() - 1
        n = len(data)
        if n > room:
            n = room
        if n == 0:
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

        return n

    def emit(self) -> List[Data]:
        self.lock.acquire()
        length = self.len()
        if length == 0:
            self.lock.release()

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

        self.lock.release()

        return data

    def peek(self) -> Data:
        if self.isEmpty():
            return None

        return self.ring[self.readPtr]

    def empty(self):
        self.readPtr = 0
        self.writePtr = 0
        self.ring = [None] * self.size

        return

    def isEmpty(self) -> bool:
        return self.readPtr == self.writePtr

    def len(self) -> int:
        return (self.writePtr - self.readPtr) % self.size


# ---------------------------------------------------------------------------
# Tests. Run: python3 spmc.py -v
#
# Expected semantics (bounded SPMC queue, batch API, reject-when-full):
#   - RingBuffer(size) holds up to `size - 1` unread items; one slot stays
#     free so that readPtr == writePtr unambiguously means "empty".
#   - insert(items) appends items in order until the buffer is full; the
#     accepted prefix is kept and the remaining items are dropped. Exactly
#     one producer thread may call insert().
#   - emit() removes and returns every unread item in FIFO order ([] if empty).
#     Any number of consumer threads may call emit() concurrently; each
#     inserted item is returned by exactly one emit() call, never as None.
#   - peek() returns the oldest unread item without consuming it, or None
#     when empty (advisory only under concurrency).
#   - len() is the number of unread items; isEmpty() is len() == 0.
#   - empty() discards everything and resets to the initial state (only
#     while no consumer is active).
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


class TestLocking(unittest.TestCase):
    def test_emit_releases_lock(self):
        rb = RingBuffer(4)
        rb.emit()
        self.assertFalse(rb.lock.locked())
        rb.insert(items(1))
        rb.emit()
        self.assertFalse(rb.lock.locked())

    def test_emit_reads_length_under_lock(self):
        """The length snapshot must be taken while holding the lock. Taking it
        before acquiring the lock lets another consumer drain the buffer in
        between, after which this consumer copies `length` slots from the new
        readPtr, returns None entries and pushes readPtr past writePtr."""
        seen = []

        class Probe(RingBuffer):
            def len(self):
                seen.append(self.lock.locked())
                return RingBuffer.len(self)

        rb = Probe(4)
        rb.insert(items(1, 2))
        seen.clear()
        rb.emit()
        self.assertTrue(seen, "emit() did not call len()")
        self.assertTrue(all(seen), "emit() called len() without holding the lock")

    def test_emit_is_serialised_across_threads(self):
        """Hold the lock from outside; a concurrent emit() must block until it
        is released rather than reading the buffer."""
        rb = RingBuffer(8)
        rb.insert(items(1, 2, 3))
        result = []
        started = threading.Event()

        def consumer():
            started.set()
            result.append(prices(rb.emit()))

        with rb.lock:
            t = threading.Thread(target=consumer)
            t.start()
            started.wait(1)
            time.sleep(0.05)
            self.assertEqual(result, [], "emit() proceeded while the lock was held")
        t.join(1)
        self.assertEqual(result, [[1, 2, 3]])


def run_producer_consumers(n, size, batch, consumers, timeout=30):
    """One producer inserts `n` items in batches (waiting for room); `consumers`
    threads emit() until everything has arrived. Returns the per-consumer lists
    of received items and the number of items the producer inserted."""
    rb = RingBuffer(size)
    payload = items(*range(n))
    received = [[] for _ in range(consumers)]
    done = threading.Event()
    inserted = [0]
    deadline = time.perf_counter() + timeout

    def producer():
        for start in range(0, n, batch):
            chunk = payload[start : start + batch]
            while capacity(size) - rb.len() < len(chunk):
                if time.perf_counter() > deadline:
                    return
                time.sleep(0)  # yield the GIL so consumers can drain
            inserted[0] += rb.insert(chunk)
        done.set()

    def consumer(i):
        while time.perf_counter() < deadline:
            got = rb.emit()
            if got:
                received[i].extend(got)
            elif done.is_set() and rb.isEmpty():
                return
            else:
                time.sleep(0)  # yield the GIL so the producer can refill

    threads = [threading.Thread(target=consumer, args=(i,)) for i in range(consumers)]
    threads.append(threading.Thread(target=producer))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return received, inserted[0]


class TestSingleProducerMultiConsumer(unittest.TestCase):
    def check_exactly_once(self, received, n):
        flat = [d for r in received for d in r]
        self.assertNotIn(None, flat, "emit() returned a None slot")
        got = prices(flat)
        self.assertEqual(len(got), n, "item count mismatch (lost or duplicated)")
        self.assertEqual(sorted(got), list(range(n)), "lost or duplicated items")
        for i, r in enumerate(received):
            ps = prices(r)
            self.assertEqual(
                ps, sorted(ps), f"consumer {i} received items out of order"
            )

    def test_single_consumer_preserves_order(self):
        received, inserted = run_producer_consumers(20_000, 32, 8, consumers=1)
        self.assertEqual(inserted, 20_000)
        self.assertEqual(prices(received[0]), list(range(20_000)))

    def test_two_consumers_each_item_exactly_once(self):
        received, inserted = run_producer_consumers(20_000, 32, 8, consumers=2)
        self.assertEqual(inserted, 20_000)
        self.check_exactly_once(received, 20_000)

    def test_many_consumers_small_ring(self):
        # Tiny ring so consumers contend on nearly every item.
        received, inserted = run_producer_consumers(20_000, 4, 2, consumers=8)
        self.assertEqual(inserted, 20_000)
        self.check_exactly_once(received, 20_000)

    def test_many_consumers_large_ring_no_backpressure(self):
        n = 20_000
        received, inserted = run_producer_consumers(n, n + 1, 64, consumers=4)
        self.assertEqual(inserted, n)
        self.check_exactly_once(received, n)

    def test_len_never_exceeds_capacity_under_contention(self):
        size = 16
        rb = RingBuffer(size)
        stop = threading.Event()
        violations = [0]
        drained = []

        def producer():
            i = 0
            while not stop.is_set():
                rb.insert(items(i))
                i += 1

        def consumer():
            while not stop.is_set():
                got = rb.emit()
                if got:
                    drained.append(len(got))
                if rb.len() > capacity(size):
                    violations[0] += 1

        threads = [threading.Thread(target=consumer) for _ in range(4)]
        threads.append(threading.Thread(target=producer))
        for t in threads:
            t.start()
        time.sleep(0.5)
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(
            violations[0], 0, "len() exceeded capacity: readPtr overshot writePtr"
        )
        self.assertGreater(sum(drained), 0)
        self.assertFalse(rb.lock.locked())

    def test_concurrent_emit_on_static_buffer(self):
        """No producer running: N threads race on emit(); exactly one wins."""
        for _ in range(50):
            rb = RingBuffer(64)
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
            self.assertEqual(
                len(non_empty), 1, "more than one consumer received the batch"
            )
            self.assertEqual(prices(non_empty[0]), list(range(63)))
            self.assertTrue(rb.isEmpty())


if __name__ == "__main__":
    unittest.main()
