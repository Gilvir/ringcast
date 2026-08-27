use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[test]
fn sender_receiver_separate_threads() {
    let (tx, mut rx) = ringcast::bounded::<u64>(8192);
    let n = 10_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    let mut received = Vec::with_capacity(n as usize);
    loop {
        match rx.try_recv() {
            Ok(val) => {
                received.push(val);
                if val == n - 1 {
                    break;
                }
            }
            Err(ringcast::RecvError::Empty) => std::hint::spin_loop(),
            Err(ringcast::RecvError::Overrun { .. }) => {}
            Err(ringcast::RecvError::Timeout) => unreachable!(),
        }
    }

    handle.join().unwrap();

    // Verify monotonically increasing
    for window in received.windows(2) {
        assert!(
            window[1] > window[0],
            "non-monotonic: {} then {}",
            window[0],
            window[1]
        );
    }
}

#[test]
fn recv_blocking_cross_thread() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    let sender = thread::spawn(move || {
        // Small delay to let receiver start spinning
        thread::sleep(std::time::Duration::from_millis(10));
        tx.send(123);
        running_clone.store(false, Ordering::Relaxed);
    });

    let val = rx.recv();
    assert_eq!(val, 123);

    sender.join().unwrap();
}

#[test]
fn batch_send_recv_cross_thread() {
    let (tx, mut rx) = ringcast::bounded::<u64>(1 << 14); // 16384 >> 8000 total items
    let n_batches = 1000;
    let batch_size = 8;

    let handle = thread::spawn(move || {
        let items: Vec<u64> = (0..batch_size).collect();
        for _ in 0..n_batches {
            tx.send_batch(&items);
        }
    });

    let mut total_received = 0usize;
    let expected = n_batches * batch_size as usize;
    let mut buf = [0u64; 32];
    while total_received < expected {
        match rx.try_recv_batch(&mut buf) {
            Ok(n) => total_received += n,
            Err(ringcast::RecvError::Overrun { .. }) => {}
            Err(ringcast::RecvError::Empty) => std::hint::spin_loop(),
            _ => unreachable!(),
        }
    }

    handle.join().unwrap();
    assert_eq!(total_received, expected);
}

fn collect_monotonic(mut rx: ringcast::Receiver<u64>, last_value: u64) -> Vec<u64> {
    let mut received = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(val) => {
                received.push(val);
                if val == last_value {
                    break;
                }
            }
            Err(ringcast::RecvError::Empty) => std::hint::spin_loop(),
            Err(ringcast::RecvError::Overrun { .. }) => {}
            Err(ringcast::RecvError::Timeout) => unreachable!(),
        }
    }
    received
}

#[test]
fn concurrent_multiple_receivers_separate_threads() {
    let (tx, rx1) = ringcast::bounded::<u64>(8192);
    let rx2 = tx.subscribe();
    let rx3 = tx.subscribe();
    let n = 50_000u64;

    let sender = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    let h1 = thread::spawn(move || collect_monotonic(rx1, n - 1));
    let h2 = thread::spawn(move || collect_monotonic(rx2, n - 1));
    let h3 = thread::spawn(move || collect_monotonic(rx3, n - 1));

    sender.join().unwrap();
    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();
    let r3 = h3.join().unwrap();

    // All should have received the last item
    assert_eq!(*r1.last().unwrap(), n - 1);
    assert_eq!(*r2.last().unwrap(), n - 1);
    assert_eq!(*r3.last().unwrap(), n - 1);

    // All sequences should be monotonically increasing
    for (name, received) in [("rx1", &r1), ("rx2", &r2), ("rx3", &r3)] {
        for window in received.windows(2) {
            assert!(
                window[1] > window[0],
                "{}: non-monotonic: {} then {}",
                name,
                window[0],
                window[1]
            );
        }
    }
}

#[test]
fn concurrent_batch_overrun_recovery() {
    let (tx, rx) = ringcast::bounded::<u64>(16);
    let n = 100_000u64;

    let sender = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    let mut rx = rx;
    let mut buf = [0u64; 8];
    let mut overrun_count = 0usize;
    let mut recv_count = 0usize;
    let mut seen_last = false;

    loop {
        match rx.try_recv_batch(&mut buf) {
            Err(ringcast::RecvError::Empty) => {
                if seen_last {
                    break;
                }
                std::hint::spin_loop();
            }
            Ok(count) => {
                recv_count += count;
                for item in &buf[..count] {
                    if *item == n - 1 {
                        seen_last = true;
                    }
                }
            }
            Err(ringcast::RecvError::Overrun { .. }) => {
                overrun_count += 1;
            }
            _ => unreachable!(),
        }
    }

    sender.join().unwrap();
    assert!(overrun_count > 0, "expected overruns with small buffer");
    assert!(recv_count > 0, "should have received some items");
}

#[test]
fn concurrent_recv_batch_ordering() {
    let (tx, rx) = ringcast::bounded::<u64>(1 << 17);
    let n = 100_000u64;

    let sender = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    let mut rx = rx;
    let mut buf = [0u64; 64];
    let mut expected = 0u64;
    while expected < n {
        match rx.try_recv_batch(&mut buf) {
            Err(ringcast::RecvError::Empty) => std::hint::spin_loop(),
            Ok(count) => {
                for item in &buf[..count] {
                    assert_eq!(*item, expected, "out of order at position {expected}");
                    expected += 1;
                }
            }
            Err(ringcast::RecvError::Overrun { .. }) => {
                panic!("unexpected overrun with large buffer");
            }
            _ => unreachable!(),
        }
    }

    sender.join().unwrap();
    assert_eq!(expected, n);
}

#[test]
fn recv_timeout_cross_thread() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);

    let sender = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(20));
        tx.send(999);
    });

    let result = rx.recv_timeout(std::time::Duration::from_secs(5));
    assert_eq!(result, Ok(999));
    sender.join().unwrap();
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LargeConcurrent {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

#[test]
fn concurrent_large_batch_correctness() {
    let (tx, rx) = ringcast::bounded::<LargeConcurrent>(4096);
    let n = 50_000u64;

    let handle = thread::spawn(move || {
        for batch_start in (0..n).step_by(8) {
            let batch: Vec<LargeConcurrent> = (0..8u64.min(n - batch_start))
                .map(|j| {
                    let i = batch_start + j;
                    LargeConcurrent {
                        a: i,
                        b: i.wrapping_mul(7),
                        c: i.wrapping_mul(13),
                        d: i.wrapping_mul(31),
                    }
                })
                .collect();
            tx.send_batch(&batch);
        }
    });

    let mut rx = rx;
    let mut buf = vec![
        LargeConcurrent {
            a: 0,
            b: 0,
            c: 0,
            d: 0,
        };
        64
    ];
    let mut seen_last = false;

    loop {
        match rx.try_recv_batch(&mut buf) {
            Err(ringcast::RecvError::Empty) => {
                if seen_last {
                    break;
                }
                std::hint::spin_loop();
            }
            Ok(count) => {
                for item in &buf[..count] {
                    assert_eq!(
                        item.b,
                        item.a.wrapping_mul(7),
                        "torn read on b at a={}",
                        item.a
                    );
                    assert_eq!(
                        item.c,
                        item.a.wrapping_mul(13),
                        "torn read on c at a={}",
                        item.a
                    );
                    assert_eq!(
                        item.d,
                        item.a.wrapping_mul(31),
                        "torn read on d at a={}",
                        item.a
                    );
                    if item.a == n - 1 {
                        seen_last = true;
                    }
                }
            }
            Err(ringcast::RecvError::Overrun { .. }) => {}
            _ => unreachable!(),
        }
    }

    handle.join().unwrap();
    assert!(seen_last);
}

#[test]
fn stress_large_type_many_items() {
    let (tx, rx) = ringcast::bounded::<LargeConcurrent>(8192);
    let n = 500_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(LargeConcurrent {
                a: i,
                b: i.wrapping_mul(7),
                c: i.wrapping_mul(13),
                d: i.wrapping_mul(31),
            });
        }
    });

    let mut rx = rx;
    let mut count = 0u64;
    loop {
        match rx.try_recv() {
            Ok(val) => {
                assert_eq!(val.b, val.a.wrapping_mul(7), "torn at a={}", val.a);
                assert_eq!(val.c, val.a.wrapping_mul(13), "torn at a={}", val.a);
                assert_eq!(val.d, val.a.wrapping_mul(31), "torn at a={}", val.a);
                count += 1;
                if val.a == n - 1 {
                    break;
                }
            }
            Err(ringcast::RecvError::Empty) => std::hint::spin_loop(),
            Err(ringcast::RecvError::Overrun { .. }) => {}
            Err(ringcast::RecvError::Timeout) => unreachable!(),
        }
    }

    handle.join().unwrap();
    assert!(count > 0);
}
