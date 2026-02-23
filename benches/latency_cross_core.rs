use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const CORE_PAIRS: &[(&str, usize, usize)] = &[
    ("cores_0_1_sibling", 0, 1),
    ("cores_2_3_sibling", 2, 3),
    ("cores_1_3_cross", 1, 3),
];

fn pin_to_core(core_id: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core_id, &mut set);
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        assert_eq!(ret, 0, "failed to pin to core {core_id}");
    }
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

/// Raw AtomicU64 ping-pong (round-trip) per core pair.
/// Establishes the hardware floor per topology.
fn cross_core_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_core_pingpong");

    for &(label, sender_core, receiver_core) in CORE_PAIRS {
        group.bench_function(label, |b| {
            let ping = Arc::new(CacheLine::new());
            let pong = Arc::new(CacheLine::new());

            let ping_rx = Arc::clone(&ping);
            let pong_tx = Arc::clone(&pong);

            let handle = std::thread::spawn(move || {
                pin_to_core(receiver_core);
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
                    pong_tx.val.store(expected, Ordering::Release);
                    expected += 1;
                }
            });

            pin_to_core(sender_core);

            b.iter(|| {
                let seq = ping.val.load(Ordering::Relaxed) + 1;
                ping.val.store(seq, Ordering::Release);

                while pong.val.load(Ordering::Acquire) != seq {
                    std::hint::spin_loop();
                }
                black_box(seq);
            });

            ping.val.store(u64::MAX, Ordering::Release);
            handle.join().unwrap();

            ping.val.store(0, Ordering::Relaxed);
            pong.val.store(0, Ordering::Relaxed);
        });
    }

    group.finish();
}

/// ringcast send/recv (one-way) per core pair.
/// Sender thread pinned to sender_core runs flat-out;
/// receiver/bencher thread pinned to receiver_core calls rx.recv().
fn cross_core_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_core_latency");

    for &(label, sender_core, receiver_core) in CORE_PAIRS {
        group.bench_function(label, |b| {
            let (tx, mut rx) = ringcast::bounded::<u64>(4096);
            let running = Arc::new(AtomicBool::new(true));
            let running_clone = Arc::clone(&running);

            let handle = std::thread::spawn(move || {
                pin_to_core(sender_core);
                let mut seq = 0u64;
                while running_clone.load(Ordering::Relaxed) {
                    tx.send(seq);
                    seq = seq.wrapping_add(1);
                    std::hint::spin_loop();
                }
            });

            pin_to_core(receiver_core);

            b.iter(|| {
                let val = rx.recv();
                black_box(val);
            });

            running.store(false, Ordering::Relaxed);
            handle.join().unwrap();
        });
    }

    group.finish();
}

criterion_group!(benches, cross_core_pingpong, cross_core_latency);
criterion_main!(benches);
