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
        length = self.len()
        if length == 0:
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
# Tests. Run: python3 spsc.py -v
#
# Expected semantics (bounded SPSC queue, batch API, reject-when-full):
#   - RingBuffer(size) holds up to `size - 1` unread items; one slot stays
#     free so that readPtr == writePtr unambiguously means "empty".
#   - insert(items) appends items in order until the buffer is full; the
#     accepted prefix is kept and the remaining items are dropped.
#   - emit() removes and returns every unread item in FIFO order ([] if empty).
#   - peek() returns the oldest unread item without consuming it, or None
#     when empty.
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


class TestSingleProducerSingleConsumer(unittest.TestCase):
    def test_threaded_producer_consumer_preserves_order(self):
        n = 20_000
        batch = 64
        rb = RingBuffer(n + 1)  # large enough that nothing is ever rejected
        payload = items(*range(n))
        received = []
        done = threading.Event()

        def producer():
            for start in range(0, n, batch):
                rb.insert(payload[start : start + batch])
            done.set()

        def consumer():
            deadline = time.perf_counter() + 10
            while len(received) < n and time.perf_counter() < deadline:
                got = rb.emit()
                if got:
                    received.extend(got)
                elif done.is_set() and rb.isEmpty():
                    break

        t_p = threading.Thread(target=producer)
        t_c = threading.Thread(target=consumer)
        t_c.start()
        t_p.start()
        t_p.join()
        t_c.join()

        self.assertEqual(len(received), n, "consumer did not receive every item")
        self.assertEqual(prices(received), list(range(n)), "order/duplicates broken")

    def test_threaded_with_backpressure(self):
        """Small ring; producer waits for room using len() before each batch."""
        n = 5_000
        batch = 8
        size = 32
        rb = RingBuffer(size)
        payload = items(*range(n))
        received = []
        done = threading.Event()
        deadline = time.perf_counter() + 10

        def producer():
            for start in range(0, n, batch):
                chunk = payload[start : start + batch]
                while capacity(size) - rb.len() < len(chunk):
                    if time.perf_counter() > deadline:
                        return
                rb.insert(chunk)
            done.set()

        def consumer():
            while len(received) < n and time.perf_counter() < deadline:
                got = rb.emit()
                if got:
                    received.extend(got)
                elif done.is_set() and rb.isEmpty():
                    break

        t_p = threading.Thread(target=producer)
        t_c = threading.Thread(target=consumer)
        t_c.start()
        t_p.start()
        t_p.join()
        t_c.join()

        self.assertEqual(len(received), n, "consumer did not receive every item")
        self.assertEqual(prices(received), list(range(n)), "order/duplicates broken")


if __name__ == "__main__":
    unittest.main()
