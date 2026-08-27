use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::hint::{likely, spin_pause, unlikely};
use crate::ring::RingBuf;

/// Errors returned by receiver operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// No data available.
    Empty,
    /// Receiver fell behind the sender by more than capacity.
    /// The receiver has been repositioned to the oldest available slot.
    Overrun { lost: usize },
    /// A timeout or deadline expired before data became available.
    Timeout,
}

/// Single-owner broadcast receiver.
///
/// `!Clone`, `!Sync`, `Send`. Each receiver independently tracks its read position.
/// Uses `&mut self` to enforce single-thread usage at compile time.
///
/// Hot fields are cached on cache line 1 to eliminate pointer indirection
/// through `Arc<RingBuf<T>>` on the fast path.
#[repr(C, align(64))]
pub struct Receiver<T: Copy> {
    // Cache line 1: hot (accessed every try_recv)
    local_position: u64,
    r_top_ptr: *const AtomicU64,
    w_top_ptr: *const AtomicU64,
    data_ptr: *const u8,
    mask: u64,
    capacity: u64,
    _pad_hot: [u8; 16],
    // Cache line 2: cold
    ring: Arc<RingBuf<T>>,
    spin_iterations: usize,
    allow_yield: bool,
    _not_sync: PhantomData<*const ()>,
}

// Safety: Receiver can be moved to another thread (Send), but cannot be
// shared across threads (!Sync enforced by PhantomData<*const ()>).
unsafe impl<T: Copy + Send> Send for Receiver<T> {}

impl<T: Copy> Receiver<T> {
    pub(crate) fn new(
        ring: Arc<RingBuf<T>>,
        position: u64,
        spin_iterations: usize,
        allow_yield: bool,
    ) -> Self {
        // Safety: pointers remain valid for the lifetime of `ring: Arc<RingBuf<T>>`
        // which is held in the same struct.
        let r_top_ptr: *const AtomicU64 = ring.r_top();
        let w_top_ptr: *const AtomicU64 = ring.w_top();
        let data_ptr = ring.data_ptr();
        let mask = ring.mask();
        let capacity = ring.capacity() as u64;

        Self {
            local_position: position,
            r_top_ptr,
            w_top_ptr,
            data_ptr,
            mask,
            capacity,
            _pad_hot: [0; 16],
            ring,
            spin_iterations,
            allow_yield,
            _not_sync: PhantomData,
        }
    }

    /// Non-blocking receive. Returns immediately.
    ///
    /// Steps: load `r_top`, load `w_top`, check overrun, check empty, read,
    /// post-read `w_top` recheck to detect overwrites during the read.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        // Load r_top before w_top — ordering matters for correct overrun detection
        let r_top = unsafe { &*self.r_top_ptr }.load(Ordering::Acquire);
        let w_top = unsafe { &*self.w_top_ptr }.load(Ordering::Acquire);

        let local_pos = self.local_position;

        if unlikely(w_top.wrapping_sub(local_pos) > self.capacity) {
            let new_pos = w_top.wrapping_sub(self.capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }

        if likely(local_pos >= r_top) {
            return Err(RecvError::Empty);
        }

        let item = if const { std::mem::size_of::<T>() <= 8 } {
            // Small types are naturally atomic: read directly from the cached
            // data pointer, then re-check w_top below to catch a lapping write.
            let idx = (local_pos & self.mask) as usize;
            unsafe { std::ptr::read((self.data_ptr as *const T).add(idx)) }
        } else {
            // Large types use the per-slot seqlock in the ring: a concurrent or
            // lapping write is reported as `None` and retried by the caller.
            match unsafe { self.ring.read_item(local_pos) } {
                Some(item) => item,
                None => return Err(RecvError::Empty),
            }
        };

        // Post-read validation: re-check w_top to detect if the sender overwrote
        // our slot between the pre-read check and the actual read. Without this,
        // a fast sender can race past the pre-read w_top snapshot, overwrite
        // this slot with a newer value, and cause non-monotonic reads.
        // Cost: ~1ns L1 cache hit in the common case (w_top line is already cached).
        let w_top_post = unsafe { &*self.w_top_ptr }.load(Ordering::Acquire);
        if unlikely(w_top_post.wrapping_sub(local_pos) > self.capacity) {
            let new_pos = w_top_post.wrapping_sub(self.capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }

        self.local_position = local_pos.wrapping_add(1);
        Ok(item)
    }

    /// Blocking receive. Spins until data is available using tiered backoff.
    #[inline]
    pub fn recv(&mut self) -> T {
        if let Ok(item) = self.try_recv() {
            return item;
        }

        self.recv_slow()
    }

    /// Optimized spin loop: polls only r_top instead of full try_recv state machine.
    #[inline(never)]
    fn recv_slow(&mut self) -> T {
        let local_pos = self.local_position;
        let mut spin_count = 0u32;

        loop {
            let r_top = unsafe { &*self.r_top_ptr }.load(Ordering::Acquire);
            if r_top > local_pos {
                break;
            }
            if (spin_count as usize) < self.spin_iterations {
                spin_pause();
            } else if self.allow_yield {
                std::thread::yield_now();
            } else {
                spin_pause();
            }
            spin_count = spin_count.wrapping_add(1);
        }

        match self.try_recv() {
            Ok(item) => {
                unsafe {
                    crate::hint::prefetch_read(self.ring.slot_ptr_raw(self.local_position));
                }
                item
            }
            Err(RecvError::Overrun { .. }) | Err(RecvError::Empty) => self.recv_slow(),
            Err(RecvError::Timeout) => unreachable!(),
        }
    }

    /// Blocking receive with timeout.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvError> {
        self.recv_deadline(Instant::now() + timeout)
    }

    /// Blocking receive with absolute deadline.
    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<T, RecvError> {
        match self.try_recv() {
            Ok(item) => return Ok(item),
            Err(RecvError::Overrun { lost }) => return Err(RecvError::Overrun { lost }),
            Err(RecvError::Empty) => {}
            Err(RecvError::Timeout) => unreachable!(),
        }

        let local_pos = self.local_position;
        let mut spin_count = 0u32;

        loop {
            let r_top = unsafe { &*self.r_top_ptr }.load(Ordering::Acquire);
            if r_top > local_pos {
                break;
            }
            if Instant::now() >= deadline {
                return Err(RecvError::Timeout);
            }
            if (spin_count as usize) < self.spin_iterations {
                spin_pause();
            } else if self.allow_yield {
                std::thread::yield_now();
            } else {
                spin_pause();
            }
            spin_count = spin_count.wrapping_add(1);
        }

        match self.try_recv() {
            Ok(item) => Ok(item),
            Err(RecvError::Overrun { lost }) => Err(RecvError::Overrun { lost }),
            Err(RecvError::Empty) => {
                // Rare: overrun happened between r_top check and try_recv.
                // Fall back to deadline loop.
                self.recv_deadline_slow(deadline)
            }
            Err(RecvError::Timeout) => unreachable!(),
        }
    }

    #[inline(never)]
    fn recv_deadline_slow(&mut self, deadline: Instant) -> Result<T, RecvError> {
        let mut spin_count = 0u32;
        loop {
            match self.try_recv() {
                Ok(item) => return Ok(item),
                Err(RecvError::Overrun { lost }) => return Err(RecvError::Overrun { lost }),
                Err(RecvError::Empty) => {
                    if Instant::now() >= deadline {
                        return Err(RecvError::Timeout);
                    }
                    if (spin_count as usize) < self.spin_iterations {
                        spin_pause();
                    } else if self.allow_yield {
                        std::thread::yield_now();
                    } else {
                        spin_pause();
                    }
                    spin_count = spin_count.wrapping_add(1);
                }
                Err(RecvError::Timeout) => unreachable!(),
            }
        }
    }

    /// Non-blocking batch receive. Fills `buf` with as many available items as fit.
    ///
    /// Returns:
    /// - `Ok(count)` — number of items written to `buf`. `count > 0`, except
    ///   `Ok(0)` when `buf` itself is empty.
    /// - `Err(RecvError::Empty)` — no data currently available in the channel.
    /// - `Err(RecvError::Overrun { lost })` — the receiver was lapped; it has
    ///   been repositioned to the oldest surviving item.
    #[inline]
    pub fn try_recv_batch(&mut self, buf: &mut [T]) -> Result<usize, RecvError> {
        if buf.is_empty() {
            return Ok(0);
        }

        let r_top = unsafe { &*self.r_top_ptr }.load(Ordering::Acquire);
        let w_top = unsafe { &*self.w_top_ptr }.load(Ordering::Acquire);

        let local_pos = self.local_position;

        if unlikely(w_top.wrapping_sub(local_pos) > self.capacity) {
            let new_pos = w_top.wrapping_sub(self.capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }

        if local_pos >= r_top {
            return Err(RecvError::Empty);
        }

        let available = r_top.wrapping_sub(local_pos) as usize;
        let count = available.min(buf.len());

        let read = if const { std::mem::size_of::<T>() <= 8 } {
            let base = self.data_ptr as *const T;
            let mask = self.mask;
            for (i, slot) in buf[..count].iter_mut().enumerate() {
                unsafe {
                    let pos = local_pos.wrapping_add(i as u64);
                    // Prefetch next slot
                    if i + 1 < count {
                        let next_idx = (pos.wrapping_add(1) & mask) as usize;
                        crate::hint::prefetch_read(base.add(next_idx));
                    }
                    let idx = (pos & mask) as usize;
                    *slot = std::ptr::read(base.add(idx));
                }
            }
            count
        } else {
            // Large types: seqlock read per slot, stop at the first slot that is
            // being written or has been lapped.
            let mut actual = 0usize;
            for slot in buf[..count].iter_mut() {
                let pos = local_pos.wrapping_add(actual as u64);
                match unsafe { self.ring.read_item(pos) } {
                    Some(item) => {
                        *slot = item;
                        actual += 1;
                    }
                    None => break,
                }
            }
            actual
        };

        if read == 0 {
            // Large-type seqlock reported the first slot as busy/lapped.
            return Err(RecvError::Empty);
        }

        // Post-read validation: re-check w_top to detect if the sender
        // overwrote any of our slots during the batch read.
        let w_top_post = unsafe { &*self.w_top_ptr }.load(Ordering::Acquire);
        if unlikely(w_top_post.wrapping_sub(local_pos) > self.capacity) {
            let new_pos = w_top_post.wrapping_sub(self.capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }
        self.local_position = local_pos.wrapping_add(read as u64);
        Ok(read)
    }

    /// Blocking batch receive. Spins until at least one item is available,
    /// then drains up to `buf.len()` items.
    pub fn recv_batch(&mut self, buf: &mut [T]) -> usize {
        if buf.is_empty() {
            return 0;
        }

        let mut spin_count = 0u32;
        loop {
            match self.try_recv_batch(buf) {
                Err(RecvError::Empty) => {
                    if (spin_count as usize) < self.spin_iterations {
                        spin_pause();
                    } else if self.allow_yield {
                        std::thread::yield_now();
                    } else {
                        spin_pause();
                    }
                    spin_count = spin_count.wrapping_add(1);
                }
                Ok(n) => return n,
                Err(RecvError::Overrun { .. }) => {
                    // Repositioned — try again immediately
                    continue;
                }
                _ => unreachable!(),
            }
        }
    }

    /// Check if the receiver has been lapped without consuming data.
    /// Returns `Some(items_lost)` if overrun occurred, `None` otherwise.
    /// Repositions the receiver if overrun is detected.
    pub fn check_overrun(&mut self) -> Option<usize> {
        let w_top = unsafe { &*self.w_top_ptr }.load(Ordering::Acquire);
        let local_pos = self.local_position;

        if w_top.wrapping_sub(local_pos) > self.capacity {
            let new_pos = w_top.wrapping_sub(self.capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            Some(lost)
        } else {
            None
        }
    }

    /// Returns the number of items available to read without blocking.
    /// This is a lower bound — more items may be published between the call and the read.
    pub fn available(&self) -> usize {
        let r_top = unsafe { &*self.r_top_ptr }.load(Ordering::Acquire);
        let local_pos = self.local_position;
        if r_top > local_pos {
            r_top.wrapping_sub(local_pos) as usize
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_line_is_cache_aligned() {
        assert_eq!(std::mem::align_of::<Receiver<u64>>() % 64, 0);
        assert_eq!(std::mem::align_of::<Receiver<[u8; 128]>>() % 64, 0);
    }
}
