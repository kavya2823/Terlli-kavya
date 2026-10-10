# ChronosMatch

**Project:** Zero-Copy High-Frequency Trading Engine
**Repo owner:** Terli Kavya
**Email:** terliikavya24@gmail.com
**Role:** Member 2 — The Order Pipeline
**Internship:** Infotact Solutions — Data Associate L1, Python Development
**Repo created:** 10 Oct 2026

---

## What This Repo Contains

My contribution as **Member 2** of the ChronosMatch team: the Order Pipeline
that delivers orders from a fake market generator to the matching engine
efficiently, reliably, and with zero loss, duplication, or corruption.

## Main Question

> Can we deliver orders to the engine efficiently and reliably?

## Components

| File | Purpose |
|------|---------|
| `pipeline/format.py` | 32-byte binary `struct` record + crc32 checksum |
| `pipeline/generator.py` | asyncio fake market-order generator |
| `pipeline/ring_buffer.py` | mmap-backed SPSC ring buffer |
| `pipeline/producer.py` | writes orders into the ring |
| `pipeline/consumer.py` | reads orders and submits them to the engine |
| `tests/test_format.py` | round-trip + corruption tests |

## Progress

- [x] Repo created 10 Oct 2026
- [ ] Binary order format
- [ ] asyncio order generator
- [ ] mmap ring buffer
- [ ] producer / consumer
- [ ] integrity test suite
- [ ] benchmark vs multiprocessing.Queue

## Timeline

- **10 Oct 2026** — repo created
- **27–30 Oct 2026** — mid-review
- **10 Nov 2026** — final submission
