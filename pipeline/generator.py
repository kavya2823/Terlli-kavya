"""
ChronosMatch — Member 2: Order Pipeline
asyncio fake market-order generator.

Emits random limit orders at a target rate and pushes them into an out queue.
"""

import asyncio
import random

from pipeline.format import Order, BUY, SELL, LIMIT


async def generate_order(order_id: int, seq_num: int) -> Order:
    """Create a single random limit order."""
    return Order(
        order_id=order_id,
        price=random.randint(9900, 10100),
        quantity=random.randint(1, 100),
        side=random.choice([BUY, SELL]),
        type=LIMIT,
        flags=0,
        seq_num=seq_num,
    )


async def run_generator(out_queue: asyncio.Queue, rate_per_sec: int,
                        stop_event: asyncio.Event) -> int:
    """
    Continuously generate orders at `rate_per_sec` and push into `out_queue`.

    Yields to the event loop between orders so the consumer is not starved.
    Returns the total number of orders generated.
    """
    interval = 1.0 / rate_per_sec
    seq = 0
    while not stop_event.is_set():
        order = await generate_order(order_id=seq, seq_num=seq)
        await out_queue.put(order)
        seq += 1
        await asyncio.sleep(interval)
    return seq


if __name__ == "__main__":
    async def _demo():
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        stop = asyncio.Event()
        task = asyncio.create_task(run_generator(q, rate_per_sec=1000, stop_event=stop))
        await asyncio.sleep(0.1)
        stop.set()
        total = await task
        print(f"Generated {total} orders in ~100ms at 1000/sec")

    asyncio.run(_demo())
