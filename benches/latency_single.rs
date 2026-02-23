use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

fn latency_send_recv(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency");

    group.bench_function("single_send_recv", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(4096);
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        let handle = std::thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                tx.send(Instant::now().elapsed().as_nanos() as u64);
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

    group.bench_function("single_try_recv_hit", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(4096);

        // Pre-fill with data
        for i in 0..100u64 {
            tx.send(i);
        }

        b.iter(|| {
            // Send one to keep data available
            tx.send(black_box(42));
            let val = rx.try_recv();
            black_box(val);
        });
    });

    group.bench_function("single_try_recv_miss", |b| {
        let (_tx, mut rx) = ringcast::bounded::<u64>(4096);

        b.iter(|| {
            let val = rx.try_recv();
            black_box(val);
        });
    });

    group.finish();
}

fn latency_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_latency");

    for batch_size in [1, 4, 16, 64] {
        group.bench_function(format!("send_batch_{}", batch_size), |b| {
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

criterion_group!(benches, latency_send_recv, latency_batch);
criterion_main!(benches);
