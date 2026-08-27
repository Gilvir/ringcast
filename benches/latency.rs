use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn latency_send_recv(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency");

    group.bench_function("single_send_recv", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(4096);
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        let handle = std::thread::spawn(move || {
            let mut seq = 0u64;
            while running_clone.load(Ordering::Relaxed) {
                tx.send(seq);
                seq = seq.wrapping_add(1);
                std::hint::spin_loop();
            }
        });

        b.iter(|| {
            let val = rx.recv();
            black_box(val);
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("try_recv_hit", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(4096);

        for i in 0..100u64 {
            tx.send(i);
        }

        b.iter(|| {
            tx.send(black_box(42));
            let val = rx.try_recv();
            let _ = black_box(val);
        });
    });

    group.bench_function("try_recv_miss", |b| {
        let (_tx, mut rx) = ringcast::bounded::<u64>(4096);

        b.iter(|| {
            let val = rx.try_recv();
            let _ = black_box(val);
        });
    });

    group.finish();
}

fn latency_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_latency");

    for batch_size in [1, 4, 16, 64] {
        group.bench_function(format!("send_batch_{batch_size}"), |b| {
            let (tx, mut rx) = ringcast::bounded::<u64>(4096);
            let items: Vec<u64> = (0..batch_size).collect();
            let mut buf = vec![0u64; batch_size as usize];

            b.iter(|| {
                tx.send_batch(black_box(&items));
                let n = rx.try_recv_batch(&mut buf).unwrap();
                black_box(n);
            });
        });
    }

    group.finish();
}

const CORE_PAIRS: &[(&str, usize, usize)] = &[
    ("cores_0_1_sibling", 0, 1),
    ("cores_2_3_sibling", 2, 3),
    ("cores_1_3_cross", 1, 3),
];

fn pin_to_core(core_id: usize) {
    assert!(
        core_affinity::set_for_current(core_affinity::CoreId { id: core_id }),
        "failed to pin to core {core_id}",
    );
}

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

criterion_group!(
    benches,
    latency_send_recv,
    latency_batch,
    cross_core_pingpong,
    cross_core_latency
);
criterion_main!(benches);
