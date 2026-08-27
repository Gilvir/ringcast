use criterion::{black_box, criterion_group, criterion_main, Criterion};

const CAPACITY: usize = 4096;

fn try_recv_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_recv_hit");

    group.bench_function("ringcast", |b| {
        let (tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        b.iter(|| {
            tx.send(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    group.bench_function("crossbeam", |b| {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
        b.iter(|| {
            let _ = tx.try_send(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    group.bench_function("flume", |b| {
        let (tx, rx) = flume::bounded::<u64>(CAPACITY);
        b.iter(|| {
            let _ = tx.try_send(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    group.bench_function("rtrb", |b| {
        let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
        b.iter(|| {
            let _ = tx.push(black_box(42u64));
            black_box(rx.pop())
        });
    });

    group.bench_function("bus", |b| {
        let mut bus_tx = bus::Bus::<u64>::new(CAPACITY);
        let mut rx = bus_tx.add_rx();
        b.iter(|| {
            let _ = bus_tx.try_broadcast(black_box(42u64));
            black_box(rx.try_recv())
        });
    });

    group.finish();
}

fn try_recv_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_recv_miss");

    group.bench_function("ringcast", |b| {
        let (_tx, mut rx) = ringcast::bounded::<u64>(CAPACITY);
        b.iter(|| black_box(rx.try_recv()));
    });

    group.bench_function("crossbeam", |b| {
        let (_tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
        b.iter(|| black_box(rx.try_recv()));
    });

    group.bench_function("flume", |b| {
        let (_tx, rx) = flume::bounded::<u64>(CAPACITY);
        b.iter(|| black_box(rx.try_recv()));
    });

    group.bench_function("rtrb", |b| {
        let (_tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
        b.iter(|| black_box(rx.pop()));
    });

    group.bench_function("bus", |b| {
        let mut bus_tx = bus::Bus::<u64>::new(CAPACITY);
        let mut rx = bus_tx.add_rx();
        b.iter(|| black_box(rx.try_recv()));
        let _ = bus_tx;
    });

    group.finish();
}

criterion_group!(benches, try_recv_hit, try_recv_miss);
criterion_main!(benches);
