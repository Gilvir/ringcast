use ringcast::RecvError;

#[derive(Debug, Clone, Copy, PartialEq)]
struct LargeStruct {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
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
fn large_send_recv_multiple() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(8);
    for i in 0..5u64 {
        tx.send(LargeStruct {
            a: i,
            b: i * 10,
            c: i * 100,
            d: i * 1000,
        });
    }
    for i in 0..5u64 {
        let expected = LargeStruct {
            a: i,
            b: i * 10,
            c: i * 100,
            d: i * 1000,
        };
        assert_eq!(rx.try_recv(), Ok(expected));
    }
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn large_empty() {
    let (_tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    assert_eq!(rx.try_recv(), Err(RecvError::Empty));
}

#[test]
fn large_overrun() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    for i in 0..8u64 {
        tx.send(LargeStruct {
            a: i,
            b: 0,
            c: 0,
            d: 0,
        });
    }

    match rx.try_recv() {
        Err(RecvError::Overrun { lost }) => {
            assert_eq!(lost, 4);
        }
        other => panic!("expected Overrun, got {:?}", other),
    }

    // Should be able to read after recovery
    let val = rx.try_recv().unwrap();
    assert_eq!(val.a, 4);
}

#[test]
fn large_subscribe() {
    let (tx, mut rx1) = ringcast::bounded::<LargeStruct>(8);
    tx.send(LargeStruct {
        a: 1,
        b: 0,
        c: 0,
        d: 0,
    });

    let mut rx2 = tx.subscribe();

    tx.send(LargeStruct {
        a: 2,
        b: 0,
        c: 0,
        d: 0,
    });

    assert_eq!(rx1.try_recv().unwrap().a, 1);
    assert_eq!(rx1.try_recv().unwrap().a, 2);

    assert_eq!(rx2.try_recv().unwrap().a, 2);
    assert_eq!(rx2.try_recv(), Err(RecvError::Empty));
}

#[test]
fn large_batch_send_recv() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(16);
    let items: Vec<LargeStruct> = (0..5)
        .map(|i| LargeStruct {
            a: i,
            b: i * 10,
            c: i * 100,
            d: i * 1000,
        })
        .collect();
    let sent = tx.send_batch(&items);
    assert_eq!(sent, 5);

    let mut buf = vec![
        LargeStruct {
            a: 0,
            b: 0,
            c: 0,
            d: 0
        };
        8
    ];
    let n = rx.try_recv_batch(&mut buf).unwrap();
    assert_eq!(n, 5);
    for i in 0..5 {
        assert_eq!(buf[i], items[i]);
    }
}

#[test]
fn large_available() {
    let (tx, rx) = ringcast::bounded::<LargeStruct>(8);
    assert_eq!(rx.available(), 0);
    tx.send(LargeStruct {
        a: 1,
        b: 0,
        c: 0,
        d: 0,
    });
    tx.send(LargeStruct {
        a: 2,
        b: 0,
        c: 0,
        d: 0,
    });
    assert_eq!(rx.available(), 2);
}

#[test]
fn large_check_overrun() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    for i in 0..8u64 {
        tx.send(LargeStruct {
            a: i,
            b: 0,
            c: 0,
            d: 0,
        });
    }
    let lost = rx.check_overrun();
    assert!(lost.is_some());
    assert_eq!(lost.unwrap(), 4);
}

#[test]
fn large_concurrent_correctness() {
    use std::thread;

    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4096);
    let n = 10_000u64;

    let handle = thread::spawn(move || {
        for i in 0..n {
            tx.send(LargeStruct {
                a: i,
                b: i.wrapping_mul(7),
                c: i.wrapping_mul(13),
                d: i.wrapping_mul(31),
            });
        }
    });

    let mut count = 0u64;
    loop {
        match rx.try_recv() {
            Ok(val) => {
                // Verify internal consistency — no torn reads
                assert_eq!(val.b, val.a.wrapping_mul(7), "torn read on b at a={}", val.a);
                assert_eq!(val.c, val.a.wrapping_mul(13), "torn read on c at a={}", val.a);
                assert_eq!(val.d, val.a.wrapping_mul(31), "torn read on d at a={}", val.a);
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

#[test]
fn large_recv_timeout() {
    let (tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    tx.send(LargeStruct {
        a: 42,
        b: 0,
        c: 0,
        d: 0,
    });
    let result = rx.recv_timeout(std::time::Duration::from_millis(100));
    assert!(result.is_ok());
    assert_eq!(result.unwrap().a, 42);
}

#[test]
fn large_recv_timeout_expires() {
    let (_tx, mut rx) = ringcast::bounded::<LargeStruct>(4);
    let result = rx.recv_timeout(std::time::Duration::from_millis(1));
    assert_eq!(result, Err(RecvError::Timeout));
}
