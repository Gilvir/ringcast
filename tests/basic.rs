use ringcast::RecvError;

#[test]
fn send_recv_single() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    tx.send(42);
    assert_eq!(rx.try_recv(), Ok(42));
}

#[test]
fn send_recv_multiple() {
    let (tx, mut rx) = ringcast::bounded::<u64>(8);
    for i in 0..5u64 {
        tx.send(i);
    }
    for i in 0..5u64 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn empty_on_no_data() {
    let (_tx, mut rx) = ringcast::bounded::<u64>(4);
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn overrun_detection() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4); // rounds to 4
    // Fill buffer and overflow
    for i in 0..8u64 {
        tx.send(i);
    }
    // Receiver should detect overrun (lapped by 4 items)
    match rx.try_recv() {
        Err(RecvError::Overrun { lost }) => {
            assert_eq!(lost, 4, "should have lost 4 items");
        }
        other => panic!("expected Overrun, got {:?}", other),
    }
    // After overrun recovery, should be able to read remaining data
    assert_eq!(rx.try_recv(), Ok(4));
    assert_eq!(rx.try_recv(), Ok(5));
    assert_eq!(rx.try_recv(), Ok(6));
    assert_eq!(rx.try_recv(), Ok(7));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn overrun_recovery_continues() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    // Overrun the receiver
    for i in 0..10u64 {
        tx.send(i);
    }
    // First recv detects overrun
    let err = rx.try_recv().unwrap_err();
    assert!(matches!(err, RecvError::Overrun { .. }));

    // Next recv should succeed with the oldest available data
    let val = rx.try_recv().unwrap();
    assert!(val >= 6, "should read from repositioned point, got {}", val);
}

#[test]
fn subscribe_creates_new_receiver() {
    let (tx, mut rx1) = ringcast::bounded::<u64>(8);
    tx.send(1);
    tx.send(2);

    // Subscribe after sending — new receiver starts at current w_top
    let mut rx2 = tx.subscribe();

    tx.send(3);
    tx.send(4);

    // rx1 sees everything from the start
    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.try_recv(), Ok(2));
    assert_eq!(rx1.try_recv(), Ok(3));
    assert_eq!(rx1.try_recv(), Ok(4));

    // rx2 only sees items after subscribe
    assert_eq!(rx2.try_recv(), Ok(3));
    assert_eq!(rx2.try_recv(), Ok(4));
    assert_eq!(rx2.try_recv(), Err(RecvError::Empty));
}

#[test]
fn batch_send_recv() {
    let (tx, mut rx) = ringcast::bounded::<u64>(16);
    let items: Vec<u64> = (0..5).collect();
    let sent = tx.send_batch(&items);
    assert_eq!(sent, 5);

    let mut buf = [0u64; 8];
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf[..5], &[0, 1, 2, 3, 4]);
}

#[test]
fn batch_send_empty() {
    let (tx, _rx) = ringcast::bounded::<u64>(4);
    let sent = tx.send_batch(&[]);
    assert_eq!(sent, 0);
}

#[test]
fn batch_recv_partial() {
    let (tx, mut rx) = ringcast::bounded::<u64>(16);
    for i in 0..10u64 {
        tx.send(i);
    }

    let mut buf = [0u64; 3];
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(buf, [0, 1, 2]);

    // More data should still be available
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(buf, [3, 4, 5]);
}

#[test]
fn capacity_one() {
    let (tx, mut rx) = ringcast::bounded::<u64>(1);
    tx.send(42);
    assert_eq!(rx.try_recv(), Ok(42));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));

    // Overwrite the single slot
    tx.send(99);
    assert_eq!(rx.try_recv(), Ok(99));
}

#[test]
fn capacity_rounds_to_power_of_two() {
    let (tx, mut rx) = ringcast::bounded::<u64>(3);
    // Should round to 4
    for i in 0..4u64 {
        tx.send(i);
    }
    for i in 0..4u64 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn check_overrun_no_overrun() {
    let (tx, mut rx) = ringcast::bounded::<u64>(8);
    tx.send(1);
    assert_eq!(rx.check_overrun(), None);
}

#[test]
fn check_overrun_detects_overrun() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    let lost = rx.check_overrun();
    assert!(lost.is_some());
    assert_eq!(lost.unwrap(), 4);
}

#[test]
fn available_count() {
    let (tx, rx) = ringcast::bounded::<u64>(8);
    assert_eq!(rx.available(), 0);
    tx.send(1);
    tx.send(2);
    tx.send(3);
    assert_eq!(rx.available(), 3);
}

#[test]
fn recv_timeout_returns_on_data() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    tx.send(42);
    let result = rx.recv_timeout(std::time::Duration::from_millis(100));
    assert_eq!(result, Ok(42));
}

#[test]
fn recv_timeout_times_out() {
    let (_tx, mut rx) = ringcast::bounded::<u64>(4);
    let result = rx.recv_timeout(std::time::Duration::from_millis(1));
    assert_eq!(result, Err(RecvError::Timeout));
}

#[test]
fn various_copy_types() {
    // u8
    let (tx, mut rx) = ringcast::bounded::<u8>(4);
    tx.send(255u8);
    assert_eq!(rx.try_recv(), Ok(255u8));

    // i32
    let (tx, mut rx) = ringcast::bounded::<i32>(4);
    tx.send(-42i32);
    assert_eq!(rx.try_recv(), Ok(-42i32));

    // f64
    let (tx, mut rx) = ringcast::bounded::<f64>(4);
    tx.send(3.14f64);
    assert_eq!(rx.try_recv(), Ok(3.14f64));

    // bool
    let (tx, mut rx) = ringcast::bounded::<bool>(4);
    tx.send(true);
    assert_eq!(rx.try_recv(), Ok(true));
}

#[test]
fn wrap_around_ring() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    // Send and receive more items than capacity to wrap around
    for round in 0..10u64 {
        let base = round * 4;
        for i in 0..4u64 {
            tx.send(base + i);
        }
        for i in 0..4u64 {
            assert_eq!(rx.try_recv(), Ok(base + i));
        }
    }
}

#[test]
fn multiple_subscribers() {
    let (tx, mut rx1) = ringcast::bounded::<u64>(8);
    let mut rx2 = tx.subscribe();
    let mut rx3 = tx.subscribe();

    tx.send(1);
    tx.send(2);

    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx2.try_recv(), Ok(1));
    assert_eq!(rx3.try_recv(), Ok(1));

    assert_eq!(rx1.try_recv(), Ok(2));
    assert_eq!(rx2.try_recv(), Ok(2));
    assert_eq!(rx3.try_recv(), Ok(2));
}

#[test]
fn batch_send_unroll_paths() {
    let (tx, mut rx) = ringcast::bounded::<u64>(16);

    // Test each unroll path (1, 2, 3, 4, and > 4)
    for batch_size in 1..=6 {
        let items: Vec<u64> = (0..batch_size).collect();
        tx.send_batch(&items);
        for i in 0..batch_size {
            assert_eq!(rx.try_recv(), Ok(i));
        }
    }
}

#[test]
fn builder_api() {
    let (tx, mut rx) = ringcast::builder::<u64>()
        .capacity(8)
        .spin_iterations(32)
        .allow_yield(true)
        .build();

    tx.send(42);
    assert_eq!(rx.try_recv(), Ok(42));
}

#[test]
#[should_panic(expected = "capacity must be set")]
fn builder_no_capacity_panics() {
    let _ = ringcast::builder::<u64>().build();
}

#[test]
#[should_panic(expected = "capacity must be > 0")]
fn zero_capacity_panics() {
    let _ = ringcast::bounded::<u64>(0);
}
