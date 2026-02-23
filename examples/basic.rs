//! Minimal single-producer / single-consumer example.

fn main() {
    let (tx, mut rx) = ringcast::bounded::<u32>(16);

    // Send a few items
    for i in 1..=5 {
        tx.send(i);
    }

    // Receive them all
    loop {
        match rx.try_recv() {
            Ok(val) => println!("received: {val}"),
            Err(ringcast::RecvError::Empty) => {
                println!("channel empty, done");
                break;
            }
            Err(ringcast::RecvError::Overrun { lost }) => {
                println!("overrun: lost {lost} items");
            }
            Err(ringcast::RecvError::Timeout) => unreachable!(),
        }
    }
}
