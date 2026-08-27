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
    for i in 0..8u64 {
        tx.send(i);
    }
    // Receiver should detect overrun (lapped by 4 items)
    match rx.try_recv() {
        Err(RecvError::Overrun { lost }) => {
            assert_eq!(lost, 4, "should have lost 4 items");
        }
        other => panic!("expected Overrun, got {other:?}"),
    }
    // After overrun recovery, should be able to read remaining data
    assert_eq!(rx.try_recv(), Ok(4));
    assert_eq!(rx.try_recv(), Ok(5));
    assert_eq!(rx.try_recv(), Ok(6));
    assert_eq!(rx.try_recv(), Ok(7));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
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
    tx.send(2.72f64);
    assert_eq!(rx.try_recv(), Ok(2.72f64));

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

#[test]
fn capacity_one_overrun() {
    let (tx, mut rx) = ringcast::bounded::<u64>(1);
    tx.send(10);
    tx.send(20);
    // w_top=2, local_pos=0, 2 > 1 → overrun, lost=1
    match rx.try_recv() {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 1),
        other => panic!("expected Overrun {{ lost: 1 }}, got {other:?}"),
    }
    assert_eq!(rx.try_recv(), Ok(20));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn try_recv_batch_empty_buffer() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    tx.send(42);
    let mut buf: [u64; 0] = [];
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 0);
    // Data should still be available
    assert_eq!(rx.try_recv(), Ok(42));
}

#[test]
fn recv_batch_empty_buffer() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    tx.send(42);
    let mut buf: [u64; 0] = [];
    let n = rx.recv_batch(&mut buf);
    assert_eq!(n, 0);
    // Data should still be available
    assert_eq!(rx.try_recv(), Ok(42));
}

#[test]
fn try_recv_batch_overrun() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    let mut buf = [0u64; 8];
    // First batch should detect overrun
    match rx.try_recv_batch(&mut buf) {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 4),
        other => panic!("expected Overrun, got {other:?}"),
    }
    // After repositioning, read surviving data
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 4);
    assert_eq!(&buf[..4], &[4, 5, 6, 7]);
}

#[test]
fn recv_batch_overrun_recovery() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    // recv_batch handles overrun internally and returns valid data
    let mut buf = [0u64; 4];
    let n = rx.recv_batch(&mut buf);
    assert_eq!(n, 4);
    assert_eq!(buf, [4, 5, 6, 7]);
}

#[test]
fn batch_send_wraps_around_ring() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    // Advance write position to near end of ring
    for i in 0..3u64 {
        tx.send(i);
    }
    for _ in 0..3 {
        rx.try_recv().unwrap();
    }
    // w_top=3, local_pos=3. Batch wraps: positions 3, 4(→slot 0), 5(→slot 1)
    let sent = tx.send_batch(&[10, 11, 12]);
    assert_eq!(sent, 3);
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Ok(11));
    assert_eq!(rx.try_recv(), Ok(12));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn batch_recv_wraps_around_ring() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    // Fill and partially consume to position receiver near ring end
    for i in 0..4u64 {
        tx.send(i);
    }
    for _ in 0..3 {
        rx.try_recv().unwrap();
    }
    // local_pos=3. Send 3 more at positions 4,5,6 (slots 0,1,2)
    for i in 4..7u64 {
        tx.send(i);
    }
    // Batch read crosses ring boundary: positions 3,4,5,6 → slots 3,0,1,2
    let mut buf = [0u64; 4];
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 4);
    assert_eq!(buf, [3, 4, 5, 6]);
}

#[test]
fn send_batch_returns_len() {
    let (tx, _rx) = ringcast::bounded::<u64>(16);
    assert_eq!(tx.send_batch(&[1, 2, 3]), 3);
    assert_eq!(tx.send_batch(&[1]), 1);
    assert_eq!(tx.send_batch(&[1, 2, 3, 4, 5, 6, 7, 8]), 8);
    assert_eq!(tx.send_batch(&[]), 0);
}

#[test]
fn send_batch_exceeding_capacity() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    let items: Vec<u64> = (0..6).collect();
    tx.send_batch(&items);
    // w_top=6, local_pos=0, 6 > 4 → overrun, new_pos=2, lost=2
    match rx.try_recv() {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 2),
        other => panic!("expected Overrun {{ lost: 2 }}, got {other:?}"),
    }
    // Surviving items: 2, 3, 4, 5
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Ok(4));
    assert_eq!(rx.try_recv(), Ok(5));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn capacity_one_batch() {
    let (tx, mut rx) = ringcast::bounded::<u64>(1);
    tx.send_batch(&[10, 20]);
    // w_top=2, local_pos=0, 2 > 1 → overrun, lost=1
    match rx.try_recv() {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 1),
        other => panic!("expected Overrun {{ lost: 1 }}, got {other:?}"),
    }
    assert_eq!(rx.try_recv(), Ok(20));
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn recv_deadline_returns_overrun_immediately() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    let start = std::time::Instant::now();
    let deadline = start + std::time::Duration::from_secs(5);
    let result = rx.recv_deadline(deadline);
    let elapsed = start.elapsed();
    match result {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 4),
        other => panic!("expected Overrun, got {other:?}"),
    }
    assert!(
        elapsed < std::time::Duration::from_millis(100),
        "overrun should return immediately, took {elapsed:?}"
    );
}

#[test]
fn recv_timeout_returns_overrun_immediately() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    let start = std::time::Instant::now();
    let result = rx.recv_timeout(std::time::Duration::from_secs(5));
    let elapsed = start.elapsed();
    match result {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 4),
        other => panic!("expected Overrun, got {other:?}"),
    }
    assert!(
        elapsed < std::time::Duration::from_millis(100),
        "overrun should return immediately, took {elapsed:?}"
    );
}

#[test]
fn recv_deadline_returns_immediately_on_data() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    tx.send(42);
    let start = std::time::Instant::now();
    let deadline = start + std::time::Duration::from_secs(5);
    let result = rx.recv_deadline(deadline);
    let elapsed = start.elapsed();
    assert_eq!(result, Ok(42));
    assert!(
        elapsed < std::time::Duration::from_millis(100),
        "should return immediately when data available, took {elapsed:?}"
    );
}

#[test]
fn recv_deadline_timeout() {
    let (_tx, mut rx) = ringcast::bounded::<u64>(4);
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(10);
    let result = rx.recv_deadline(deadline);
    assert_eq!(result, Err(RecvError::Timeout));
}

#[test]
fn subscribe_after_overrun() {
    let (tx, _rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    // New subscriber starts at w_top — should see Empty, not Overrun
    let mut rx2 = tx.subscribe();
    assert_eq!(rx2.try_recv(), Err(RecvError::Empty));
}

#[test]
fn available_after_overrun() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    let lost = rx.check_overrun().unwrap();
    assert_eq!(lost, 4);
    // After repositioning, 4 items are available (full capacity)
    assert_eq!(rx.available(), 4);
}

#[test]
fn available_decreases_after_recv() {
    let (tx, mut rx) = ringcast::bounded::<u64>(8);
    for i in 0..5u64 {
        tx.send(i);
    }
    assert_eq!(rx.available(), 5);
    rx.try_recv().unwrap();
    assert_eq!(rx.available(), 4);
    rx.try_recv().unwrap();
    assert_eq!(rx.available(), 3);
}

#[test]
fn check_overrun_idempotent_when_no_overrun() {
    let (tx, mut rx) = ringcast::bounded::<u64>(8);
    tx.send(1);
    tx.send(2);
    assert_eq!(rx.check_overrun(), None);
    assert_eq!(rx.check_overrun(), None);
    // Position undisturbed — can still read both items
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn check_overrun_then_recv() {
    let (tx, mut rx) = ringcast::bounded::<u64>(4);
    for i in 0..8u64 {
        tx.send(i);
    }
    let lost = rx.check_overrun().unwrap();
    assert_eq!(lost, 4);
    // After repositioning, try_recv gets oldest surviving item
    assert_eq!(rx.try_recv(), Ok(4));
}

#[test]
fn multiple_subscribers_independent_positions() {
    let (tx, mut rx1) = ringcast::bounded::<u64>(16);
    tx.send(0);
    tx.send(1);

    let mut rx2 = tx.subscribe(); // starts at position 2

    tx.send(2);
    tx.send(3);

    let mut rx3 = tx.subscribe(); // starts at position 4

    tx.send(4);
    tx.send(5);

    // rx1 sees everything from position 0
    assert_eq!(rx1.try_recv(), Ok(0));
    assert_eq!(rx1.try_recv(), Ok(1));
    assert_eq!(rx1.try_recv(), Ok(2));
    assert_eq!(rx1.try_recv(), Ok(3));
    assert_eq!(rx1.try_recv(), Ok(4));
    assert_eq!(rx1.try_recv(), Ok(5));

    // rx2 sees items from position 2 onward
    assert_eq!(rx2.try_recv(), Ok(2));
    assert_eq!(rx2.try_recv(), Ok(3));
    assert_eq!(rx2.try_recv(), Ok(4));
    assert_eq!(rx2.try_recv(), Ok(5));

    // rx3 sees items from position 4 onward
    assert_eq!(rx3.try_recv(), Ok(4));
    assert_eq!(rx3.try_recv(), Ok(5));

    assert_eq!(rx1.try_recv(), Err(RecvError::Empty));
    assert_eq!(rx2.try_recv(), Err(RecvError::Empty));
    assert_eq!(rx3.try_recv(), Err(RecvError::Empty));
}
