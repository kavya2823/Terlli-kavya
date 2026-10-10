"""
ChronosMatch — Member 2: Order Pipeline
Binary wire format for orders.

Fixed 32-byte record, little-endian, integer ticks only (never floats).
Every record carries a crc32 checksum so corruption is detected on read.
"""

import struct
import zlib
from dataclasses import dataclass


# ----- Struct layout -----------------------------------------------------
#   q  order_id    (8)   unique, monotonic
#   q  price       (8)   integer ticks
#   i  quantity    (4)
#   B  side        (1)   0 = buy, 1 = sell
#   B  type        (1)   0 = limit, 1 = market
#   H  flags       (2)   reserved
#   I  seq_num     (4)   assigned by producer, monotonic
#   I  checksum    (4)   crc32 of the first 28 bytes
#
#   Total = 32 bytes
# -------------------------------------------------------------------------

ORDER_FMT = "<qqiBBHII"
ORDER_SIZE = struct.calcsize(ORDER_FMT)
assert ORDER_SIZE == 32, f"ORDER_FMT must be 32 bytes, got {ORDER_SIZE}"


# ----- Side / Type constants --------------------------------------------
BUY = 0
SELL = 1
LIMIT = 0
MARKET = 1


@dataclass(slots=True)
class Order:
    """The only type that crosses between the pipeline and the matching engine."""
    order_id: int
    price: int      # integer ticks
    quantity: int
    side: int       # 0 = buy, 1 = sell
    type: int       # 0 = limit, 1 = market
    flags: int = 0
    seq_num: int = 0


class CorruptRecord(Exception):
    """Raised when a record's checksum does not match its payload."""


# ----- Pack / Unpack -----------------------------------------------------

def pack_order(order: Order) -> bytes:
    """Serialize an Order into a fixed 32-byte record."""
    head = struct.pack(
        "<qqiBBH",
        order.order_id,
        order.price,
        order.quantity,
        order.side,
        order.type,
        order.flags,
    )
    checksum = zlib.crc32(head) & 0xFFFFFFFF
    return struct.pack(
        "<qqiBBHII",
        order.order_id,
        order.price,
        order.quantity,
        order.side,
        order.type,
        order.flags,
        order.seq_num,
        checksum,
    )


def unpack_order(record: bytes) -> Order:
    """Deserialize a 32-byte record back into an Order, verifying checksum."""
    if len(record) != ORDER_SIZE:
        raise CorruptRecord(f"record length {len(record)} != {ORDER_SIZE}")

    (order_id, price, quantity, side, otype, flags, seq_num, checksum) = \
        struct.unpack(ORDER_FMT, record)

    head = struct.pack("<qqiBBH", order_id, price, quantity, side, otype, flags)
    expected = zlib.crc32(head) & 0xFFFFFFFF
    if checksum != expected:
        raise CorruptRecord(
            f"checksum mismatch: got {checksum:#x}, expected {expected:#x}"
        )

    return Order(
        order_id=order_id,
        price=price,
        quantity=quantity,
        side=side,
        type=otype,
        flags=flags,
        seq_num=seq_num,
    )
