"""Round-trip and integrity tests for pipeline/format.py."""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.format import (
    Order, pack_order, unpack_order,
    ORDER_SIZE, CorruptRecord, BUY, SELL, LIMIT,
)


def test_order_size_is_32():
    assert ORDER_SIZE == 32


def test_roundtrip_single():
    o = Order(order_id=1, price=10050, quantity=10, side=BUY, type=LIMIT, seq_num=1)
    rec = pack_order(o)
    assert len(rec) == ORDER_SIZE
    assert unpack_order(rec) == o


def test_roundtrip_many():
    for i in range(10_000):
        o = Order(
            order_id=i,
            price=9900 + (i % 200),
            quantity=1 + (i % 100),
            side=i % 2,
            type=LIMIT,
            seq_num=i,
        )
        assert unpack_order(pack_order(o)) == o


def test_corruption_detected():
    o = Order(order_id=42, price=10100, quantity=5, side=BUY, type=LIMIT, seq_num=7)
    rec = bytearray(pack_order(o))
    rec[3] ^= 0xFF
    try:
        unpack_order(bytes(rec))
        assert False, "expected CorruptRecord"
    except CorruptRecord:
        pass


def test_wrong_length_rejected():
    try:
        unpack_order(b"\x00" * 31)
        assert False, "expected CorruptRecord"
    except CorruptRecord:
        pass
