use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CAPACITY: usize = 65536;
const MSGS_PER_ITER: u64 = 10_000;
const BATCH_SIZE: usize = 16;

fn spsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("ringcast", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        b.iter(|| {
            for i in 0..MSGS_PER_ITER {
                tx.send(black_box(i));
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("crossbeam", |b| {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        b.iter(|| {
            for i in 0..MSGS_PER_ITER {
                let _ = tx.try_send(black_box(i));
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("flume", |b| {
        let (tx, rx) = flume::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        b.iter(|| {
            for i in 0..MSGS_PER_ITER {
                let _ = tx.try_send(black_box(i));
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("rtrb", |b| {
        let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.pop() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.pop().is_ok() {}
        });

        b.iter(|| {
            for i in 0..MSGS_PER_ITER {
                while tx.push(black_box(i)).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.finish();
}

fn spsc_throughput_batch_16(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput_batch_16");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("ringcast", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        let items: Vec<u64> = (0..BATCH_SIZE as u64).collect();
        b.iter(|| {
            for _ in 0..(MSGS_PER_ITER as usize / BATCH_SIZE) {
                tx.send_batch(black_box(&items));
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("crossbeam", |b| {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        b.iter(|| {
            for _ in 0..(MSGS_PER_ITER as usize / BATCH_SIZE) {
                for j in 0..BATCH_SIZE as u64 {
                    let _ = tx.try_send(black_box(j));
                }
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("flume", |b| {
        let (tx, rx) = flume::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        b.iter(|| {
            for _ in 0..(MSGS_PER_ITER as usize / BATCH_SIZE) {
                for j in 0..BATCH_SIZE as u64 {
                    let _ = tx.try_send(black_box(j));
                }
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("rtrb", |b| {
        let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        let handle = std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                match rx.pop() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.pop().is_ok() {}
        });

        b.iter(|| {
            for _ in 0..(MSGS_PER_ITER as usize / BATCH_SIZE) {
                for j in 0..BATCH_SIZE as u64 {
                    while tx.push(black_box(j)).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.finish();
}

criterion_group!(benches, spsc_throughput, spsc_throughput_batch_16);
criterion_main!(benches);
