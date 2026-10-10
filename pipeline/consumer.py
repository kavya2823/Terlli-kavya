"""
ChronosMatch — Member 2: Order Pipeline
Consumer: reads order records from the mmap ring buffer and submits them
to the matching engine.

If a record is corrupt (checksum mismatch), it is counted and skipped;
the consumer keeps going so one bad record cannot stall the pipeline.
"""

import asyncio

from pipeline.format import unpack_order, CorruptRecord
from pipeline.ring_buffer import RingBuffer, RingBufferEmpty


class ConsumerStats:
    def __init__(self):
        self.received = 0
        self.corrupt = 0

    def __repr__(self):
        return f"ConsumerStats(received={self.received}, corrupt={self.corrupt})"


async def run_consumer(ring: RingBuffer, engine,
                       stop_event: asyncio.Event) -> ConsumerStats:
    stats = ConsumerStats()
    while not stop_event.is_set():
        if ring.empty():
            await asyncio.sleep(0)
            continue
        try:
            record = ring.read()
        except RingBufferEmpty:
            await asyncio.sleep(0)
            continue

        try:
            order = unpack_order(record)
        except CorruptRecord:
            stats.corrupt += 1
            continue

        engine.submit(order)
        stats.received += 1

    return stats


class NullEngine:
    """Placeholder engine used until the real matching engine is wired in."""
    def submit(self, order):
        return []


if __name__ == "__main__":
    import os
    import tempfile
    from pipeline.producer import run_producer

    async def _demo():
        fd, tmp = tempfile.mkstemp(suffix=".bin")
        os.close(fd)
        os.remove(tmp)

        ring = RingBuffer(tmp, capacity=1024)
        stop = asyncio.Event()
        engine = NullEngine()

        prod = asyncio.create_task(run_producer(ring, rate_per_sec=5000, stop_event=stop))
        cons = asyncio.create_task(run_consumer(ring, engine, stop_event=stop))

        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.gather(prod, cons)

        print("consumer demo finished")
        ring.close()
        os.remove(tmp)

    asyncio.run(_demo())
