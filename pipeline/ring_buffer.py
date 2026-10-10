"""
ChronosMatch — Member 2: Order Pipeline
Shared-memory ring buffer over mmap (single-producer / single-consumer).

Layout:
    [16-byte header: write_idx (8) | read_idx (8)]
    [slot 0][slot 1][slot 2]...[slot N-1]     (each slot = 32 bytes)

The buffer is backed by a file so producer and consumer can share it across
processes. Capacity must be a power of two so that index & (N-1) replaces
modulo.
"""

import mmap
import os
import struct

from pipeline.format import ORDER_SIZE


HEADER = struct.Struct("<QQ")     # write_idx, read_idx
HEADER_SIZE = HEADER.size         # 16 bytes


class RingBufferFull(Exception):
    pass


class RingBufferEmpty(Exception):
    pass


class RingBuffer:
    def __init__(self, path: str, capacity: int, create: bool = True):
        if capacity & (capacity - 1):
            raise ValueError("capacity must be a power of two")
        self.capacity = capacity
        self.path = path
        self.total_size = HEADER_SIZE + capacity * ORDER_SIZE

        if create:
            self.fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            os.ftruncate(self.fd, self
