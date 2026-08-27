use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::receiver::Receiver;
use crate::ring::RingBuf;

/// Single-threaded sender. `!Clone`, `!Sync`, `Send`.
///
/// Moving to a thread transfers exclusive ownership.
///
/// Hot fields are cached on cache line 1 to eliminate pointer indirection
/// through `Arc<RingBuf<T>>` on the fast path.
#[repr(C, align(64))]
pub struct Sender<T: Copy> {
    // Cache line 1: hot
    w_top_ptr: *const AtomicU64,
    r_top_ptr: *const AtomicU64,
    _pad_hot: [u8; 48],
    // Cache line 2: cold
    ring: Arc<RingBuf<T>>,
    spin_iterations: usize,
    allow_yield: bool,
    _not_sync: PhantomData<*const ()>,
}

// Safety: Sender can be moved to another thread, but not shared.
unsafe impl<T: Copy + Send> Send for Sender<T> {}

impl<T: Copy> Sender<T> {
    pub(crate) fn new(ring: Arc<RingBuf<T>>, spin_iterations: usize, allow_yield: bool) -> Self {
        // Safety: pointers remain valid for the lifetime of `ring: Arc<RingBuf<T>>`
        // which is held in the same struct.
        let w_top_ptr: *const AtomicU64 = ring.w_top();
        let r_top_ptr: *const AtomicU64 = ring.r_top();

        Self {
            w_top_ptr,
            r_top_ptr,
            _pad_hot: [0; 48],
            ring,
            spin_iterations,
            allow_yield,
            _not_sync: PhantomData,
        }
    }

    /// Send a single item. Never blocks. Overwrites oldest slot if full.
    ///
    /// Two-phase publish:
    /// 1. Advance `w_top` (Relaxed) — reserves the slot
    /// 2. Write data
    /// 3. Advance `r_top` (Release) — makes it visible to receivers
    #[inline(always)]
    pub fn send(&self, item: T) {
        let w_top = unsafe { &*self.w_top_ptr };
        let r_top = unsafe { &*self.r_top_ptr };

        let pos = w_top.load(Ordering::Relaxed);
        w_top.store(pos.wrapping_add(1), Ordering::Relaxed);

        // Prefetch next write slot for Modified MESI state while writing current slot.
        // This overlaps the RFO (Read For Ownership) latency with the current write.
        unsafe {
            crate::hint::prefetch_write(self.ring.slot_ptr_raw(pos.wrapping_add(1)));
        }

        unsafe {
            self.ring.write_item(pos, item);
        }

        r_top.store(pos.wrapping_add(1), Ordering::Release);
    }

    /// Send a batch of items. Returns the number of items sent (always == items.len()).
    ///
    /// Amortizes the atomic `r_top` publish across N items.
    /// Manually unrolled for N <= 4, prefetch loop for N >= 5.
    #[inline]
    pub fn send_batch(&self, items: &[T]) -> usize {
        let n = items.len();
        if n == 0 {
            return 0;
        }

        let w_top = unsafe { &*self.w_top_ptr };
        let r_top = unsafe { &*self.r_top_ptr };

        let start = w_top.load(Ordering::Relaxed);
        w_top.store(start.wrapping_add(n as u64), Ordering::Relaxed);

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
                    for (i, &item) in items.iter().enumerate() {
                        let pos = start.wrapping_add(i as u64);
                        if i + 1 < n {
                            crate::hint::prefetch_write(
                                self.ring.slot_ptr_raw(pos.wrapping_add(1)),
                            );
                        }
                        self.ring.write_item(pos, item);
                    }
                }
            }
        }

        r_top.store(start.wrapping_add(n as u64), Ordering::Release);

        n
    }

    /// Create a new receiver positioned at the current write head.
    /// The new receiver will see all items sent after this call.
    pub fn subscribe(&self) -> Receiver<T> {
        let pos = unsafe { &*self.w_top_ptr }.load(Ordering::Relaxed);
        Receiver::new(
            Arc::clone(&self.ring),
            pos,
            self.spin_iterations,
            self.allow_yield,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_line_is_cache_aligned() {
        assert_eq!(align_of::<Sender<u64>>() % 64, 0);
        assert_eq!(align_of::<Sender<[u8; 128]>>() % 64, 0);
    }
}
