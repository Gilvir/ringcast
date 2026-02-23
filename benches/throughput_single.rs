use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

fn throughput_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("send_only", |b| {
        let (tx, _rx) = ringcast::bounded::<u64>(65536);

        b.iter(|| {
            tx.send(black_box(42u64));
        });
    });

    group.bench_function("send_recv_pair", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(65536);
        let running = Arc::new(AtomicBool::new(true));
        let recv_count = Arc::new(AtomicU64::new(0));
        let running_clone = Arc::clone(&running);
        let recv_count_clone = Arc::clone(&recv_count);

        let handle = std::thread::spawn(move || {
            let mut count = 0u64;
            while running_clone.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => count += 1,
                    Err(_) => std::hint::spin_loop(),
                }
            }
            // Drain remaining
            while rx.try_recv().is_ok() {
                count += 1;
            }
            recv_count_clone.store(count, Ordering::Relaxed);
        });

        b.iter(|| {
            tx.send(black_box(42u64));
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("send_batch_16", |b| {
        let (tx, _rx) = ringcast::bounded::<u64>(65536);
        let items: Vec<u64> = (0..16).collect();

        b.iter(|| {
            tx.send_batch(black_box(&items));
        });
    });

    group.finish();
}

criterion_group!(benches, throughput_single);
criterion_main!(benches);
