use ringcast::RecvError;

#[derive(Debug, Clone, Copy, PartialEq)]
struct LargeStruct {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

fn make_large(i: u64) -> LargeStruct {
    LargeStruct {
        a: i,
        b: i.wrapping_mul(7),
        c: i.wrapping_mul(13),
        d: i.wrapping_mul(31),
    }
}

fn check_large(val: &LargeStruct) {
    assert_eq!(
        val.b,
        val.a.wrapping_mul(7),
        "inconsistent b at a={}",
        val.a
    );
    assert_eq!(
        val.c,
        val.a.wrapping_mul(13),
        "inconsistent c at a={}",
        val.a
    );
    assert_eq!(
        val.d,
        val.a.wrapping_mul(31),
        "inconsistent d at a={}",
        val.a
    );
}

#[test]
fn large_send_recv_single() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    let item = LargeStruct {
        a: 1,
        b: 2,
        c: 3,
        d: 4,
    };
    tx.send(item);
    assert_eq!(rx.try_recv(), Ok(item));
}

#[test]
fn large_batch_send_wraps_around() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    // Advance position to near end
    for i in 0..3u64 {
        tx.send(make_large(i));
    }
    for _ in 0..3 {
        rx.try_recv().unwrap();
    }
    // Batch write wraps: positions 3, 4(→slot 0), 5(→slot 1)
    let items: Vec<LargeStruct> = (10..13).map(make_large).collect();
    tx.send_batch(&items);
    for i in 10..13u64 {
        let val = rx.try_recv().unwrap();
        assert_eq!(val.a, i);
        check_large(&val);
    }
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn large_try_recv_batch_overrun() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    for i in 0..8u64 {
        tx.send(make_large(i));
    }
    let mut buf = vec![make_large(0); 8];
    match rx.try_recv_batch(&mut buf) {
        Err(RecvError::Overrun { lost }) => assert_eq!(lost, 4),
        other => panic!("expected Overrun, got {other:?}"),
    }
    // After repositioning, read surviving data
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 4);
    for (i, item) in buf.iter().enumerate().take(4) {
        assert_eq!(item.a, (i + 4) as u64);
        check_large(item);
    }
}

#[test]
fn large_batch_recv_partial() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(16);
    for i in 0..10u64 {
        tx.send(make_large(i));
    }
    let mut buf = vec![make_large(0); 3];
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 3);
    for (idx, item) in buf[..3].iter().enumerate() {
        assert_eq!(item.a, idx as u64);
        check_large(item);
    }
    // More data still available
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 3);
    for (idx, item) in buf[..3].iter().enumerate() {
        assert_eq!(item.a, (idx + 3) as u64);
        check_large(item);
    }
}

/// Concurrent send/recv with large types — verifies no torn reads under contention.
#[test]
fn large_concurrent_correctness() {
    use std::thread;

    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4096);
    let n = 10_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(make_large(i));
        }
    });

    let mut count = 0u64;
    loop {
        match rx.try_recv() {
            Ok(val) => {
                check_large(&val);
                count += 1;
                if val.a == n - 1 {
                    break;
                }
            }
            Err(RecvError::Empty) => std::hint::spin_loop(),
            Err(RecvError::Overrun { .. }) => {}
            Err(RecvError::Timeout) => unreachable!(),
        }
    }

    handle.join().unwrap();
    assert!(count > 0);
}
