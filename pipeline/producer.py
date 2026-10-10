"""
ChronosMatch — Member 2: Order Pipeline
Producer: generates orders and writes them into the mmap ring buffer.

Overflow policy: on full buffer, drops the newest order and counts it.
Swap to blocking behavior when correctness > throughput.
"""

import asyncio

from pipeline.format import pack_order
from pipeline.generator import generate_order
from pipeline.ring_buffer import RingBuffer, RingBufferFull


class ProducerStats:
    def __init__(self):
        self.sent = 0
        self.dropped = 0

    def __repr__(self):
        return f"ProducerStats(sent={self.sent}, dropped={self.dropped})"


async def run_producer(ring: RingBuffer, rate_per_sec: int,
                       stop_event: asyncio.Event) -> ProducerStats:
    interval = 1.0 / rate_per_sec
    seq = 0
    stats = ProducerStats()
    while not stop_event.is_set():
        order = await generate_order(order_id=seq, seq_num=seq)
        try:
            ring.write(pack_order(order))
            stats.sent += 1
        except RingBufferFull:
            stats.dropped += 1
        seq += 1
        await asyncio.sleep(interval)
    return stats


if __name__ == "__main__":
    import os
    import tempfile

    async def _demo():
        fd, tmp = tempfile.mkstemp(suffix=".bin")
        os.close(fd)
        os.remove(tmp)

        ring = RingBuffer(tmp, capacity=1024)
        stop = asyncio.Event()
        task = asyncio.create_task(run_producer(ring, rate_per_sec=1000, stop_event=stop))
        await asyncio.sleep(0.05)
        stop.set()
        stats = await task
        print(f"producer {stats}")
        ring.close()
        os.remove(tmp)

    asyncio.run(_demo())
