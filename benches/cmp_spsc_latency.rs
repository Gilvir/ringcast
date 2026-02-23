use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const CAPACITY: usize = 4096;

fn spsc_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_latency");

    // --- ringcast ---
    group.bench_function("ringcast", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        b.iter(|| {
            tx.send(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    // --- crossbeam-channel ---
    group.bench_function("crossbeam", |b| {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
        b.iter(|| {
            let _ = tx.try_send(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    // --- flume ---
    group.bench_function("flume", |b| {
        let (tx, rx) = flume::bounded::<u64>(CAPACITY);
        b.iter(|| {
            let _ = tx.try_send(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    // --- rtrb ---
    group.bench_function("rtrb", |b| {
        let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
        b.iter(|| {
            let _ = tx.push(black_box(42u64));
            black_box(rx.pop())
        });
    });

    group.finish();
}

fn spsc_latency_cross_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_latency_cross_thread");

    // --- ringcast ---
    group.bench_function("ringcast", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        let ack = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let ack_rx = ack.clone();
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(val) => {
                        ack_rx.store(val, Ordering::Release);
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }
        });

        let mut seq = 1u64;
        b.iter(|| {
            tx.send(black_box(seq));
            while ack.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
            seq += 1;
        });

        running.store(false, Ordering::Relaxed);
        tx.send(0); // unblock receiver
        handle.join().unwrap();
    });

    // --- crossbeam-channel ---
    group.bench_function("crossbeam", |b| {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
        let ack = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let ack_rx = ack.clone();
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(val) => {
                        ack_rx.store(val, Ordering::Release);
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }
        });

        let mut seq = 1u64;
        b.iter(|| {
            let _ = tx.try_send(black_box(seq));
            while ack.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
            seq += 1;
        });

        running.store(false, Ordering::Relaxed);
        let _ = tx.try_send(0);
        handle.join().unwrap();
    });

    // --- flume ---
    group.bench_function("flume", |b| {
        let (tx, rx) = flume::bounded::<u64>(CAPACITY);
        let ack = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let ack_rx = ack.clone();
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(val) => {
                        ack_rx.store(val, Ordering::Release);
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }
        });

        let mut seq = 1u64;
        b.iter(|| {
            let _ = tx.try_send(black_box(seq));
            while ack.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
            seq += 1;
        });

        running.store(false, Ordering::Relaxed);
        let _ = tx.try_send(0);
        handle.join().unwrap();
    });

    // --- rtrb ---
    group.bench_function("rtrb", |b| {
        let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
        let ack = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let ack_rx = ack.clone();
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.pop() {
                    Ok(val) => {
                        ack_rx.store(val, Ordering::Release);
                    }
                    Err(_) => std::hint::spin_loop(),
                }
            }
        });

        let mut seq = 1u64;
        b.iter(|| {
            while tx.push(black_box(seq)).is_err() {
                std::hint::spin_loop();
            }
            while ack.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
            seq += 1;
        });

        running.store(false, Ordering::Relaxed);
        let _ = tx.push(0);
        handle.join().unwrap();
    });

    group.finish();
}

criterion_group!(benches, spsc_latency, spsc_latency_cross_thread);
criterion_main!(benches);
