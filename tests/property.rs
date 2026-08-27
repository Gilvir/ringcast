use proptest::prelude::*;
use ringcast::RecvError;

#[derive(Debug, Clone, Copy, PartialEq)]
struct Quad {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

impl Quad {
    fn new(seed: u64) -> Self {
        Self {
            a: seed,
            b: seed ^ 0xDEAD_BEEF_CAFE_BABE,
            c: seed.wrapping_mul(6_364_136_223_846_793_005),
            d: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
        }
    }

    fn is_consistent(&self) -> bool {
        self.b == self.a ^ 0xDEAD_BEEF_CAFE_BABE
            && self.c == self.a.wrapping_mul(6_364_136_223_846_793_005)
            && self.d == self.a.wrapping_add(0x9E37_79B9_7F4A_7C15)
    }
}

/// Drain all available items, panicking on overrun.
fn drain_strict<T: Copy>(rx: &mut ringcast::Receiver<T>) -> Vec<T> {
    let mut out = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(item) => out.push(item),
            Err(RecvError::Empty) => return out,
            Err(e) => panic!("unexpected error during drain_strict: {e:?}"),
        }
    }
}

/// Drain all available items, skipping overruns.
fn drain_all<T: Copy>(rx: &mut ringcast::Receiver<T>) -> Vec<T> {
    let mut out = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(item) => out.push(item),
            Err(RecvError::Empty) => return out,
            Err(RecvError::Overrun { .. }) => continue,
            Err(RecvError::Timeout) => unreachable!(),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Everything sent is received in FIFO order when no overrun occurs.
    #[test]
    fn roundtrip_no_overrun(cap_exp in 0u32..=12, count in 0usize..=4096) {
        let cap = 1usize << cap_exp;
        let count = count.min(cap);

        let (tx, mut rx) = ringcast::bounded::<u64>(cap);
        for i in 0..count as u64 {
            tx.send(i);
        }

        let received = drain_strict(&mut rx);
        prop_assert_eq!(received.len(), count);
        for (i, &val) in received.iter().enumerate() {
            prop_assert_eq!(val, i as u64);
        }
        prop_assert_eq!(rx.try_recv(), Err(RecvError::Empty));
    }

    /// First recv after overrun reports correct loss count, and surviving
    /// tail is the last `cap` items in order.
    #[test]
    fn overrun_reports_correct_loss(cap_exp in 1u32..=10, extra in 1usize..=4096) {
        let cap = 1usize << cap_exp;
        let total = cap + extra;

        let (tx, mut rx) = ringcast::bounded::<u64>(cap);
        for i in 0..total as u64 {
            tx.send(i);
        }

        match rx.try_recv() {
            Err(RecvError::Overrun { lost }) => {
                prop_assert_eq!(lost, extra);
            }
            other => prop_assert!(false, "expected Overrun, got {:?}", other),
        }

        // After repositioning, drain surviving items
        let received = drain_strict(&mut rx);
        prop_assert_eq!(received.len(), cap);
        let first_surviving = total - cap;
        for (i, &val) in received.iter().enumerate() {
            prop_assert_eq!(val, (first_surviving + i) as u64);
        }
    }

    /// send_batch + try_recv_batch yields the same sequence as one-by-one.
    #[test]
    fn batch_equivalence(
        items in prop::collection::vec(any::<u64>(), 0..=512),
        cap_exp in 3u32..=12,
    ) {
        let cap = 1usize << cap_exp;
        let items: Vec<u64> = items.into_iter().take(cap).collect();

        // Path A: batch
        let (tx_a, mut rx_a) = ringcast::bounded::<u64>(cap);
        if !items.is_empty() {
            tx_a.send_batch(&items);
        }
        let mut buf_a = vec![0u64; items.len().max(1)];
        let n_a = if items.is_empty() {
            0
        } else {
            rx_a.try_recv_batch(&mut buf_a).unwrap_or(0)
        };

        // Path B: one-by-one
        let (tx_b, mut rx_b) = ringcast::bounded::<u64>(cap);
        for &item in &items {
            tx_b.send(item);
        }
        let received_b = drain_strict(&mut rx_b);

        prop_assert_eq!(&buf_a[..n_a], &received_b[..]);
    }

    /// Every received Quad has consistent fields (no torn reads).
    #[test]
    fn no_torn_reads(
        seeds in prop::collection::vec(any::<u64>(), 1..=2048),
        cap_exp in 2u32..=10,
    ) {
        let cap = 1usize << cap_exp;
        let (tx, mut rx) = ringcast::bounded::<Quad>(cap);

        for &seed in &seeds {
            tx.send(Quad::new(seed));
        }

        let received = drain_all(&mut rx);
        for item in &received {
            prop_assert!(item.is_consistent(), "torn read detected: {:?}", item);
        }
    }

    /// N receivers from subscribe() each independently see the same sequence.
    #[test]
    fn multi_receiver_independence(
        count in 1usize..=1024,
        n_receivers in 2usize..=6,
        cap_exp in 3u32..=10,
    ) {
        let cap = 1usize << cap_exp;
        let count = count.min(cap);

        let (tx, mut rx0) = ringcast::bounded::<u64>(cap);
        let mut receivers: Vec<_> = (1..n_receivers).map(|_| tx.subscribe()).collect();

        for i in 0..count as u64 {
            tx.send(i);
        }

        let expected: Vec<u64> = (0..count as u64).collect();
        let got0 = drain_strict(&mut rx0);
        prop_assert_eq!(&got0, &expected);

        for (idx, rx) in receivers.iter_mut().enumerate() {
            let got = drain_strict(rx);
            prop_assert_eq!(&got, &expected, "receiver {} diverged", idx + 1);
        }
    }

    /// Data survives crossing ring boundaries at arbitrary positions.
    #[test]
    fn wrap_around_integrity(
        cap_exp in 1u32..=8,
        pre_fill in 0usize..=256,
        payload in prop::collection::vec(any::<u64>(), 1..=512),
    ) {
        let cap = 1usize << cap_exp;

        let (tx, mut rx) = ringcast::bounded::<u64>(cap);

        // Pre-fill to shift ring position
        for i in 0..pre_fill as u64 {
            tx.send(i);
        }
        // Drain pre-fill (may overrun, that's fine)
        drain_all(&mut rx);

        for &val in &payload {
            tx.send(val);
        }

        if payload.len() <= cap {
            // No overrun expected
            let received = drain_strict(&mut rx);
            prop_assert_eq!(&received, &payload);
        } else {
            // Overrun expected — surviving tail should be last `cap` items
            match rx.try_recv() {
                Err(RecvError::Overrun { .. }) => {}
                other => prop_assert!(false, "expected Overrun, got {:?}", other),
            }
            let received = drain_strict(&mut rx);
            let tail = &payload[payload.len() - cap..];
            prop_assert_eq!(&received, tail);
        }
    }
}

// Concurrent torn-read check — fewer cases since we spawn threads.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn concurrent_no_torn_reads(count in 1000usize..=50_000, cap_exp in 4u32..=10) {
        let cap = 1usize << cap_exp;
        let (tx, mut rx) = ringcast::bounded::<Quad>(cap);

        let handle = std::thread::spawn(move || {
            for i in 0..count as u64 {
                tx.send(Quad::new(i));
            }
        });

        let mut torn = false;
        let mut received = 0usize;
        loop {
            match rx.try_recv() {
                Ok(item) => {
                    if !item.is_consistent() {
                        torn = true;
                        break;
                    }
                    received += 1;
                }
                Err(RecvError::Empty) => {
                    if received > 0 && handle.is_finished() {
                        // Drain remaining
                        loop {
                            match rx.try_recv() {
                                Ok(item) => {
                                    if !item.is_consistent() {
                                        torn = true;
                                    }
                                    received += 1;
                                }
                                Err(RecvError::Empty) => break,
                                Err(RecvError::Overrun { .. }) => continue,
                                Err(RecvError::Timeout) => unreachable!(),
                            }
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
                Err(RecvError::Overrun { .. }) => continue,
                Err(RecvError::Timeout) => unreachable!(),
            }
        }

        handle.join().unwrap();
        prop_assert!(!torn, "torn read detected under concurrency");
        prop_assert!(received > 0, "should have received at least one item");
    }
}
