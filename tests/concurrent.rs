use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
fn broadcast_multiple_receivers() {
    let (tx, mut rx1) = ringcast::bounded::<u64>(4096);
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();
    let n = 10_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    let collect = |rx: &mut ringcast::Receiver<u64>| -> Vec<u64> {
        let mut received = Vec::new();
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
        received
    };

    // rx1 started at position 0, rx2 and rx3 subscribed at the same point
    let r1 = collect(&mut rx1);
    handle.join().unwrap();

    // rx2 and rx3 may have missed earlier items if sender was fast
    let r2 = collect(&mut rx2);
    let r3 = collect(&mut rx3);

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
fn overrun_under_load() {
    // Small buffer, fast sender, slow receiver → guaranteed overruns
    let (tx, mut rx) = ringcast::bounded::<u64>(16);
    let n = 100_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    handle.join().unwrap();

    // Receiver is very late — should see overrun
    let mut overrun_count = 0;
    let mut recv_count = 0;
    loop {
        match rx.try_recv() {
            Ok(_) => recv_count += 1,
            Err(ringcast::RecvError::Overrun { .. }) => {
                overrun_count += 1;
            }
            Err(ringcast::RecvError::Empty) => break,
            _ => unreachable!(),
        }
    }

    assert!(overrun_count > 0, "expected at least one overrun");
    assert!(recv_count > 0, "expected to receive some items");
    assert!(
        recv_count <= 16,
        "should not receive more than capacity items after overrun"
    );
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
    let (tx, mut rx) = ringcast::bounded::<u64>(1024);
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

#[test]
fn stress_many_items() {
    // Use a large buffer to avoid overruns, so we can verify ordering
    let (tx, mut rx) = ringcast::bounded::<u64>(1 << 20); // 1M slots
    let n = 1_000_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(i);
        }
    });

    let mut count = 0u64;
    loop {
        match rx.try_recv() {
            Ok(val) => {
                // Each value should equal its expected position
                assert_eq!(val, count, "unexpected value at position {}", count);
                count += 1;
                if count == n {
                    break;
                }
            }
            Err(ringcast::RecvError::Empty) => std::hint::spin_loop(),
            Err(ringcast::RecvError::Overrun { .. }) => {
                panic!("unexpected overrun with large buffer");
            }
            Err(ringcast::RecvError::Timeout) => unreachable!(),
        }
    }

    handle.join().unwrap();
    assert_eq!(count, n);
}
