//! Multi-consumer broadcast with overrun handling.
//!
//! A producer thread sends a stream of ticks. Three receiver threads consume
//! them — one is deliberately slow so it triggers overrun detection.

use std::thread;
use std::time::Duration;

use ringcast::RecvError;

const TICK_COUNT: u64 = 500;

fn main() {
    // Small capacity so the slow receiver gets lapped.
    let (tx, mut rx_fast1) = ringcast::bounded::<u64>(450);
    let mut rx_fast2 = tx.subscribe();
    let mut rx_slow = tx.subscribe();

    let producer = thread::spawn(move || {
        for tick in 0..TICK_COUNT {
            tx.send(tick);
        }
        println!("[producer] sent {TICK_COUNT} ticks");
    });

    let fast1 = thread::spawn(move || {
        let mut count = 0u64;
        loop {
            match rx_fast1.recv_timeout(Duration::from_millis(100)) {
                Ok(_) => count += 1,
                Err(RecvError::Overrun { lost }) => {
                    println!("[fast-1]  overrun, lost {lost}");
                }
                Err(RecvError::Timeout) => break,
                Err(RecvError::Empty) => unreachable!(),
            }
        }
        println!("[fast-1]  received {count}");
    });

    let fast2 = thread::spawn(move || {
        let mut count = 0u64;
        loop {
            match rx_fast2.recv_timeout(Duration::from_millis(100)) {
                Ok(_) => count += 1,
                Err(RecvError::Overrun { lost }) => {
                    println!("[fast-2]  overrun, lost {lost}");
                }
                Err(RecvError::Timeout) => break,
                Err(RecvError::Empty) => unreachable!(),
            }
        }
        println!("[fast-2]  received {count}");
    });

    let slow = thread::spawn(move || {
        let mut count = 0u64;
        let mut total_lost = 0usize;
        loop {
            match rx_slow.recv_timeout(Duration::from_millis(100)) {
                Ok(_) => {
                    count += 1;
                    // Simulate slow processing every 10 items.
                    if count % 10 == 0 {
                        thread::sleep(Duration::from_micros(5000));
                    }
                }
                Err(RecvError::Overrun { lost }) => {
                    total_lost += lost;
                }
                Err(RecvError::Timeout) => break,
                Err(RecvError::Empty) => unreachable!(),
            }
        }
        println!("[slow]    received {count}, lost {total_lost} to overrun");
    });

    producer.join().unwrap();
    fast1.join().unwrap();
    fast2.join().unwrap();
    slow.join().unwrap();
}
