use std::marker::PhantomData;
use std::sync::Arc;

use crate::alloc::{Allocator, HugePageAllocator};
use crate::receiver::Receiver;
use crate::ring::RingBuf;
use crate::sender::Sender;

pub struct Builder<T: Copy> {
    capacity: Option<usize>,
    allocator: Option<Box<dyn Allocator>>,
    spin_iterations: usize,
    allow_yield: bool,
    _marker: PhantomData<T>,
}

impl<T: Copy> Builder<T> {
    pub fn new() -> Self {
        Self {
            capacity: None,
            allocator: None,
            spin_iterations: 64,
            allow_yield: false,
            _marker: PhantomData,
        }
    }

    /// Set the ring buffer capacity. Required. Must be > 0.
    /// Non-power-of-two values are rounded up to the next power of two.
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Set a custom allocator. Default: `HugePageAllocator`.
    pub fn allocator(mut self, allocator: impl Allocator + 'static) -> Self {
        self.allocator = Some(Box::new(allocator));
        self
    }

    /// Set the number of spin-pause iterations before escalation.
    /// Default: 64.
    pub fn spin_iterations(mut self, iterations: usize) -> Self {
        self.spin_iterations = iterations;
        self
    }

    /// Allow `thread::yield_now()` as a last-resort backoff.
    /// Default: false.
    pub fn allow_yield(mut self, allow: bool) -> Self {
        self.allow_yield = allow;
        self
    }

    /// Build the channel, returning `(Sender, Receiver)`.
    ///
    /// # Panics
    /// Panics if capacity is not set or is zero.
    pub fn build(self) -> (Sender<T>, Receiver<T>) {
        let capacity = self.capacity.expect("ringcast: capacity must be set");
        assert!(capacity > 0, "ringcast: capacity must be > 0");

        let allocator = self
            .allocator
            .unwrap_or_else(|| Box::new(HugePageAllocator::new()));

        let ring = Arc::new(RingBuf::new(capacity, allocator));

        let sender = Sender::new(Arc::clone(&ring), self.spin_iterations, self.allow_yield);
        let receiver = Receiver::new(ring, 0, self.spin_iterations, self.allow_yield);

        (sender, receiver)
    }
}

impl<T: Copy> Default for Builder<T> {
    fn default() -> Self {
        Self::new()
    }
}
