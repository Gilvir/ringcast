//! Broadcasting a large struct across threads.
//!
//! Demonstrates that ringcast works with types larger than 8 bytes
//! using the same w_top bracket for overwrite detection.

use std::thread;
use std::time::Duration;

use ringcast::RecvError;

/// A market quote — 32 bytes, well beyond the 8-byte "naturally atomic" threshold.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
struct Quote {
    symbol_id: u32,
    flags: u32,
    bid: f64,
    ask: f64,
    volume: u64,
}

fn main() {
    let (tx, mut rx1) = ringcast::bounded::<Quote>(1024);
    let mut rx2 = tx.subscribe();

    let producer = thread::spawn(move || {
        for i in 0..100u64 {
            let price = 100.0 + (i as f64) * 0.01;
            tx.send(Quote {
                symbol_id: 42,
                flags: 0,
                bid: price,
                ask: price + 0.02,
                volume: 1000 + i,
            });
        }
        println!("[producer] sent 100 quotes");
    });

    // Fast consumer — reads everything.
    let consumer1 = thread::spawn(move || {
        let mut count = 0u64;
        loop {
            match rx1.recv_timeout(Duration::from_millis(50)) {
                Ok(q) => {
                    count += 1;
                    if count == 1 || count == 100 {
                        println!(
                            "[consumer-1] quote #{count}: bid={:.2} ask={:.2} vol={}",
                            q.bid, q.ask, q.volume
                        );
                    }
                }
                Err(RecvError::Overrun { lost }) => {
                    println!("[consumer-1] overrun, lost {lost}");
                }
                Err(RecvError::Timeout) => break,
                Err(RecvError::Empty) => unreachable!(),
            }
        }
        println!("[consumer-1] received {count} quotes total");
    });

    // Batch consumer — reads in chunks of 16.
    let consumer2 = thread::spawn(move || {
        let mut buf = [Quote {
            symbol_id: 0,
            flags: 0,
            bid: 0.0,
            ask: 0.0,
            volume: 0,
        }; 16];
        let mut total = 0u64;
        loop {
            match rx2.recv_timeout(Duration::from_millis(50)) {
                Ok(_first) => {
                    total += 1;
                    // Drain any remaining into a batch.
                    match rx2.try_recv_batch(&mut buf) {
                        Ok(n) => total += n as u64,
                        Err(RecvError::Overrun { .. }) => {}
                        Err(RecvError::Empty) => {}
                        Err(RecvError::Timeout) => unreachable!(),
                    }
                }
                Err(RecvError::Overrun { lost }) => {
                    println!("[consumer-2] overrun, lost {lost}");
                }
                Err(RecvError::Timeout) => break,
                Err(RecvError::Empty) => unreachable!(),
            }
        }
        println!("[consumer-2] received {total} quotes total (batch)");
    });

    producer.join().unwrap();
    consumer1.join().unwrap();
    consumer2.join().unwrap();
}
