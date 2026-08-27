# ringcast

A lock-free SPMC (single-producer, multiple-consumer) broadcast ring buffer for Rust. Aimed at low-latency work like market data distribution and real-time telemetry.

The sender never blocks and overwrites the oldest slot when the buffer is full. Every receiver independently reads all messages. If a receiver falls behind, it detects the overrun and jumps forward to the oldest available data instead of returning garbage.

## Quick start

```rust
use ringcast::RecvError;

// Create a channel. Capacity gets rounded up to a power of two.
let (tx, mut rx) = ringcast::bounded::<u64>(4096);

// More receivers via subscribe — they all see every message.
let mut rx2 = tx.subscribe();

// Producer side — send() never blocks, never fails.
tx.send(42);

// Consumer side
match rx.try_recv() {
    Ok(val) => { /* got it */ }
    Err(RecvError::Empty) => { /* nothing yet */ }
    Err(RecvError::Overrun { lost }) => { /* fell behind by `lost` items, repositioned */ }
    Err(RecvError::Timeout) => { /* only from recv_timeout/recv_deadline */ }
}
```

## What it does

- **Single producer, many consumers** — one `Sender` (not cloneable), unlimited `Receiver`s via `tx.subscribe()`.
- **Overwrite-on-full** — no backpressure, no blocking the sender. Slow consumers get an `Overrun` error with a count of lost items and are repositioned automatically.
- **Lock-free** — the entire hot path is atomic loads and stores. No mutexes, no `lock` prefixed instructions on x86. The only ordering constraint is a single `Release`/`Acquire` pair on the read-top counter.
- **Works with any `Copy` type** — overwrite detection uses the w_top pre/post bracket, which catches overwrites for all type sizes with no per-slot overhead.
- **Huge page allocation** — on Linux, the backing buffer uses 2MB huge pages by default (falls back to `mmap` with `MAP_POPULATE`, or to the system allocator on non-Linux platforms) to reduce TLB misses.
- **Batch operations** — `send_batch` / `try_recv_batch` / `recv_batch` amortize the atomic publish across N items.
- **Optional `mimalloc` feature** — enable the `mimalloc` Cargo feature to set `mimalloc` as the process's global allocator. Off by default so the library doesn't impose an allocator choice on consumers.

## Constraints

- `T: Copy` is required. No types with destructors, since overwrite-on-full has nowhere to run them.
- Capacity is fixed at creation time.
- Receivers spin-wait by default (tiered: immediate retry, then `pause`/`yield`). It's built for dedicated cores, not shared threadpools.

## Benchmarks

All benchmarks run with [Criterion](https://github.com/bheisler/criterion.rs) on the same machine, same build, same run. The comparison crates are:

- [crossbeam-channel](https://crates.io/crates/crossbeam-channel) — MPMC channel (not broadcast; simulated via N separate channels for fanout tests)
- [flume](https://crates.io/crates/flume) — MPMC channel (same simulation for fanout)
- [rtrb](https://crates.io/crates/rtrb) — lock-free SPSC ring buffer (not broadcast, single consumer only)
- [bus](https://crates.io/crates/bus) — SPMC broadcast ring

### Single-thread send+recv latency

How fast is a `send()` immediately followed by `try_recv()` on the same thread. Measures the raw overhead of the data structure, no cross-core transfer involved.

| Library | Latency |
|---|---|
| **ringcast** | **2.7 ns** |
| rtrb | 2.9 ns |
| crossbeam | 14.0 ns |
| flume | 14.2 ns |
| bus | 38.8 ns |

### Empty try_recv (miss)

How fast does `try_recv()` return when there's nothing to read. Relevant if you're spinning on it.

| Library | Latency |
|---|---|
| rtrb | 1.1 ns |
| **ringcast** | **1.1 ns** |
| bus | 8.1 ns |
| flume | 8.9 ns |
| crossbeam | 24.4 ns |

### Cross-thread round-trip latency

Ping-pong: sender writes a value, receiver reads it and acknowledges via an atomic. Measures the full cache-line transfer cost.

| Library | Latency |
|---|---|
| rtrb | 77 ns |
| crossbeam | 114 ns |
| **ringcast** | **117 ns** |
| flume | 206 ns |

rtrb wins here — it's a dedicated SPSC ring with no broadcast overhead. ringcast is competitive with crossbeam while also being a broadcast channel.

### SPSC throughput (10k messages, cross-thread)

Time to push 10k `u64` values through the channel with a receiver draining on another thread.

| Library | Time | Throughput |
|---|---|---|
| rtrb | 29 us | ~341M msgs/s |
| **ringcast** | **135 us** | **~74M msgs/s** |
| crossbeam | 179 us | ~56M msgs/s |
| flume | 482 us | ~21M msgs/s |

### SPSC throughput, batch of 16

Same test, but ringcast uses `send_batch()` with 16-element slices. The others don't have a native batch API so they send 16 items individually.

| Library | Time | Throughput |
|---|---|---|
| **ringcast** | **14 us** | **~718M msgs/s** |
| rtrb | 30 us | ~333M msgs/s |
| crossbeam | 163 us | ~61M msgs/s |
| flume | 478 us | ~21M msgs/s |

The batch path amortizes the atomic publish: one `Release` store per batch instead of per item.

### Broadcast fanout throughput (10k messages)

The actual use case: one sender, multiple receivers, all seeing every message. crossbeam and flume don't have native broadcast, so they're simulated with N separate channels (the sender loops and sends to each one). rtrb is SPSC-only so it's excluded.

| Receivers | ringcast | bus | crossbeam (sim) | flume (sim) |
|---|---|---|---|---|
| 1 | **149 us** | 865 us | 157 us | 460 us |
| 2 | **364 us** | 1.10 ms | 624 us | 2.68 ms |
| 4 | **622 us** | 1.47 ms | 1.49 ms | 3.98 ms |
| 8 | **881 us** | 1.73 ms | 2.62 ms | 8.40 ms |

Receivers share no mutable state with each other; each one reads a shared counter and its own local position. At 8 receivers it's roughly 2x bus and 3x simulated crossbeam.

### Disclaimer

These benchmarks were run on a single machine (WSL2, no core isolation, no `isolcpus`, no huge pages pre-allocated, stock CPU governor). They show relative performance between libraries under the same conditions, but the absolute numbers will differ on your hardware. For production latency work you'd want isolated cores, pinned threads, huge pages via `hugetlbfs`, and hyper-threading and turbo boost disabled.

Run the benchmarks yourself on your target hardware before trusting any of these numbers, mine included.

## API

### Creating a channel

```rust
// Simple — capacity only (rounded to next power of two)
let (tx, rx) = ringcast::bounded::<u64>(4096);

// Builder — for tuning allocation and spin behavior
let (tx, rx) = ringcast::builder::<u64>()
    .capacity(4096)
    .build();
```

### Sender

```rust
tx.send(item);                    // Single item. Never blocks.
tx.send_batch(&items);            // Batch. One atomic publish for N items.
let rx2 = tx.subscribe();        // New receiver, positioned at current write head.
```

### Receiver

```rust
rx.try_recv()                     // Non-blocking. Returns Ok(T) or Err(RecvError).
rx.recv()                         // Blocking (spin-wait).
rx.recv_timeout(duration)         // Blocking with timeout.
rx.recv_deadline(instant)         // Blocking with deadline.
rx.try_recv_batch(&mut buf)       // Non-blocking batch read.
rx.recv_batch(&mut buf)           // Blocking batch read.
rx.available()                    // Items available (lower bound).
rx.check_overrun()                // Check if lapped without consuming.
```

## License

MIT OR Apache-2.0
