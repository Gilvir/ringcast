use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CAPACITY: usize = 65536;
const MSGS_PER_ITER: u64 = 10_000;

fn shutdown(running: &Arc<AtomicBool>, handles: Vec<std::thread::JoinHandle<()>>) {
    running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}

fn fanout_throughput(c: &mut Criterion) {
    for n in [1usize, 2, 4, 8] {
        let mut group = c.benchmark_group(format!("fanout_throughput_{n}rx"));
        group.measurement_time(Duration::from_secs(5));

        group.bench_function("ringcast", |b| {
            let (tx, rx) = ringcast::bounded::<u64>(CAPACITY);
            let mut rxs: Vec<ringcast::Receiver<u64>> = vec![rx];
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

            shutdown(&running, handles);
        });

        group.bench_function("bus", |b| {
            let mut bus_tx = bus::Bus::<u64>::new(CAPACITY);
            let rxs: Vec<_> = (0..n).map(|_| bus_tx.add_rx()).collect();

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
                    let _ = bus_tx.try_broadcast(black_box(i));
                }
            });

            shutdown(&running, handles);
        });

        group.bench_function("crossbeam_simulated", |b| {
            let mut txs = Vec::with_capacity(n);
            let mut rxs = Vec::with_capacity(n);
            for _ in 0..n {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
                txs.push(tx);
                rxs.push(rx);
            }

            let running = Arc::new(AtomicBool::new(true));
            let handles: Vec<_> = rxs
                .into_iter()
                .map(|rx| {
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
                    let val = black_box(i);
                    for tx in &txs {
                        let _ = tx.try_send(val);
                    }
                }
            });

            shutdown(&running, handles);
        });

        group.bench_function("flume_simulated", |b| {
            let mut txs = Vec::with_capacity(n);
            let mut rxs = Vec::with_capacity(n);
            for _ in 0..n {
                let (tx, rx) = flume::bounded::<u64>(CAPACITY);
                txs.push(tx);
                rxs.push(rx);
            }

            let running = Arc::new(AtomicBool::new(true));
            let handles: Vec<_> = rxs
                .into_iter()
                .map(|rx| {
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
                    let val = black_box(i);
                    for tx in &txs {
                        let _ = tx.try_send(val);
                    }
                }
            });

            shutdown(&running, handles);
        });

        group.finish();
    }
}

criterion_group!(benches, fanout_throughput);
criterion_main!(benches);
