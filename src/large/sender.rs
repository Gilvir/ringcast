use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::receiver::Receiver;
use super::ring::LargeRingBuf;

/// Single-threaded sender for large types. `!Clone`, `!Sync`, `Send`.
pub struct Sender<T: Copy> {
    ring: Arc<LargeRingBuf<T>>,
    spin_iterations: usize,
    allow_yield: bool,
    _not_sync: PhantomData<*const ()>,
}

unsafe impl<T: Copy + Send> Send for Sender<T> {}

impl<T: Copy> Sender<T> {
    pub(crate) fn new(
        ring: Arc<LargeRingBuf<T>>,
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
    /// Write protocol:
    /// 1. `w_top.load(Relaxed)`
    /// 2. `w_top.store(pos + 1, Relaxed)` — advance write head
    /// 3. `slot.seq_pre.store(pos, Release)` — begin write marker
    /// 4. `ptr::write(slot.data, item)` — write data
    /// 5. `slot.seq_post.store(pos, Release)` — end write marker
    /// 6. `r_top.store(pos + 1, Release)` — publish to receivers
    #[inline(always)]
    pub fn send(&self, item: T) {
        let pos = self.ring.w_top().load(Ordering::Relaxed);
        self.ring
            .w_top()
            .store(pos.wrapping_add(1), Ordering::Relaxed);

        unsafe {
            self.ring.write_slot(pos, item);
        }

        self.ring
            .r_top()
            .store(pos.wrapping_add(1), Ordering::Release);
    }

    /// Send a batch of items. Returns items.len().
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

        for (i, item) in items.iter().enumerate() {
            unsafe {
                self.ring.write_slot(start.wrapping_add(i as u64), *item);
            }
        }

        self.ring
            .r_top()
            .store(start.wrapping_add(n as u64), Ordering::Release);

        n
    }

    /// Create a new receiver positioned at the current write head.
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
