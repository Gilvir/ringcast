use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const CAPACITY: usize = 65536;
const MSGS_PER_ITER: u64 = 10_000;

fn throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("send_only", |b| {
        let (tx, _rx) = ringcast::bounded::<u64>(CAPACITY);

        b.iter(|| {
            tx.send(black_box(42u64));
        });
    });

    group.bench_function("send_recv_pair", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        let handle = std::thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(_) => std::hint::spin_loop(),
                }
            }
            while rx.try_recv().is_ok() {}
        });

        b.iter(|| {
            tx.send(black_box(42u64));
        });

        running.store(false, Ordering::Relaxed);
        handle.join().unwrap();
    });

    group.bench_function("send_batch_16", |b| {
        let (tx, _rx) = ringcast::bounded::<u64>(CAPACITY);
        let items: Vec<u64> = (0..16).collect();

        b.iter(|| {
            tx.send_batch(black_box(&items));
        });
    });

    group.finish();
}

fn fanout_throughput(c: &mut Criterion) {
    for n in [1usize, 2, 4, 8] {
        let mut group = c.benchmark_group(format!("fanout_throughput_{n}rx"));
        group.measurement_time(Duration::from_secs(5));

        group.bench_function("ringcast", |b| {
            let (tx, rx) = ringcast::bounded::<u64>(CAPACITY);
            let mut rxs = vec![rx];
            for _ in 1..n {
                rxs.push(tx.subscribe());
            }

            let running = Arc::new(AtomicBool::new(true));
            let handles: Vec<_> = rxs
                .into_iter()
                .map(|mut rx| {
                    let r = running.clone();
                    std::thread::spawn(move || {
                        while r.load(Ordering::Relaxed) {
                            match rx.try_recv() {
                                Ok(_) => {}
                                Err(_) => std::hint::spin_loop(),
                            }
                        }
                        while rx.try_recv().is_ok() {}
                    })
                })
                .collect();

            b.iter(|| {
                for i in 0..MSGS_PER_ITER {
                    tx.send(black_box(i));
                }
            });

            running.store(false, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }
        });

        group.finish();
    }
}

criterion_group!(benches, throughput, fanout_throughput);
criterion_main!(benches);
