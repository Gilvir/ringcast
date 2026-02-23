use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Measures the raw cost of transferring a cache line between two cores
/// via an AtomicU64 ping-pong. This is the hardware floor — ringcast
/// cannot be faster than this.
fn baseline_pingpong(c: &mut Criterion) {
    let ping = Arc::new(CacheLine::new());
    let pong = Arc::new(CacheLine::new());

    c.bench_function("atomic_pingpong_roundtrip", |b| {
        let ping_tx = Arc::clone(&ping);
        let pong_rx = Arc::clone(&pong);
        let ping_rx = Arc::clone(&ping);
        let pong_tx = Arc::clone(&pong);

        let handle = std::thread::spawn(move || {
            let mut expected = 1u64;
            loop {
                let val = ping_rx.val.load(Ordering::Acquire);
                if val == u64::MAX {
                    break;
                }
                if val != expected {
                    std::hint::spin_loop();
                    continue;
                }
                // Send pong
                pong_tx.val.store(expected, Ordering::Release);
                expected += 1;
            }
        });

        b.iter(|| {
            let seq = ping_tx.val.load(Ordering::Relaxed) + 1;
            ping_tx.val.store(seq, Ordering::Release);

            // Wait for pong
            while pong_rx.val.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
            black_box(seq);
        });

        // Signal thread to exit
        ping_tx.val.store(u64::MAX, Ordering::Release);
        handle.join().unwrap();

        // Reset for next iteration
        ping.val.store(0, Ordering::Relaxed);
        pong.val.store(0, Ordering::Relaxed);
    });
}

/// Cache-line-aligned atomic counter.
#[repr(align(64))]
struct CacheLine {
    val: AtomicU64,
}

impl CacheLine {
    fn new() -> Self {
        Self {
            val: AtomicU64::new(0),
        }
    }
}

criterion_group!(benches, baseline_pingpong);
criterion_main!(benches);
