mod receiver;
mod ring;
mod sender;

pub use receiver::Receiver;
pub use sender::Sender;

use crate::alloc::HugePageAllocator;
use ring::LargeRingBuf;
use std::sync::Arc;

/// Create a bounded broadcast channel for large types (`size_of::<T>() > 8`).
///
/// Uses per-slot sequence counters for tear detection. Same API as the
/// standard variant but with ~5-10ns additional latency per operation.
///
/// # Panics
/// Panics if `capacity` is 0.
pub fn bounded<T: Copy>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let ring = Arc::new(LargeRingBuf::new(
        capacity,
        Box::new(HugePageAllocator::new()),
    ));

    let sender = Sender::new(Arc::clone(&ring), 64, false);
    let receiver = Receiver::new(ring, 0, 64, false);

    (sender, receiver)
}
