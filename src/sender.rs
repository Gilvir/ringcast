use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::receiver::Receiver;
use crate::ring::RingBuf;

/// Single-threaded sender. `!Clone`, `!Sync`, `Send`.
///
/// Moving to a thread transfers exclusive ownership.
pub struct Sender<T: Copy> {
    ring: Arc<RingBuf<T>>,
    spin_iterations: usize,
    allow_yield: bool,
    // !Sync, !Clone
    _not_sync: PhantomData<*const ()>,
}

// Safety: Sender can be moved to another thread, but not shared.
unsafe impl<T: Copy + Send> Send for Sender<T> {}

impl<T: Copy> Sender<T> {
    pub(crate) fn new(
        ring: Arc<RingBuf<T>>,
        spin_iterations: usize,
        allow_yield: bool,
    ) -> Self {
        Self {
            ring,
            spin_iterations,
            allow_yield,
            _not_sync: PhantomData,
        }
    }

    /// Send a single item. Never blocks. Overwrites oldest slot if full.
    ///
    /// Two-phase publish protocol (design §4):
    /// 1. `w_top.load(Relaxed)` — read current position (sender-local)
    /// 2. `w_top.store(pos + 1, Relaxed)` — advance write head
    /// 3. `write_item(pos, item)` — write data (bare write or seqlock)
    /// 4. `r_top.store(pos + 1, Release)` — publish to receivers
    #[inline(always)]
    pub fn send(&self, item: T) {
        let pos = self.ring.w_top().load(Ordering::Relaxed);
        self.ring.w_top().store(pos.wrapping_add(1), Ordering::Relaxed);

        unsafe {
            self.ring.write_item(pos, item);
        }

        self.ring.r_top().store(pos.wrapping_add(1), Ordering::Release);
    }

    /// Send a batch of items. Returns the number of items sent (always == items.len()).
    ///
    /// Amortizes the atomic `r_top` publish across N items.
    /// For small types: manually unrolled for N <= 4, prefetch loop for N >= 5.
    /// For large types: simple loop (seqlock per-write makes unrolling less beneficial).
    #[inline]
    pub fn send_batch(&self, items: &[T]) -> usize {
        let n = items.len();
        if n == 0 {
            return 0;
        }

        let start = self.ring.w_top().load(Ordering::Relaxed);
        self.ring
            .w_top()
            .store(start.wrapping_add(n as u64), Ordering::Relaxed);

        if const { std::mem::size_of::<T>() <= 8 } {
            unsafe {
                match n {
                    1 => {
                        self.ring.write_item(start, items[0]);
                    }
                    2 => {
                        self.ring.write_item(start, items[0]);
                        self.ring.write_item(start.wrapping_add(1), items[1]);
                    }
                    3 => {
                        self.ring.write_item(start, items[0]);
                        self.ring.write_item(start.wrapping_add(1), items[1]);
                        self.ring.write_item(start.wrapping_add(2), items[2]);
                    }
                    4 => {
                        self.ring.write_item(start, items[0]);
                        self.ring.write_item(start.wrapping_add(1), items[1]);
                        self.ring.write_item(start.wrapping_add(2), items[2]);
                        self.ring.write_item(start.wrapping_add(3), items[3]);
                    }
                    _ => {
                        for i in 0..n {
                            let pos = start.wrapping_add(i as u64);
                            if i + 1 < n {
                                crate::hint::prefetch_read(
                                    self.ring.slot_ptr_raw(pos.wrapping_add(1)),
                                );
                            }
                            self.ring.write_item(pos, items[i]);
                        }
                    }
                }
            }
        } else {
            for (i, item) in items.iter().enumerate() {
                unsafe {
                    self.ring.write_item(start.wrapping_add(i as u64), *item);
                }
            }
        }

        self.ring
            .r_top()
            .store(start.wrapping_add(n as u64), Ordering::Release);

        n
    }

    /// Create a new receiver positioned at the current write head.
    /// The new receiver will see all items sent after this call.
    pub fn subscribe(&self) -> Receiver<T> {
        let pos = self.ring.w_top().load(Ordering::Relaxed);
        Receiver::new(
            Arc::clone(&self.ring),
            pos,
            self.spin_iterations,
            self.allow_yield,
        )
    }
}
