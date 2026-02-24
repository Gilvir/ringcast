use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
    Overrun {
        /// The number of items that were overwritten and lost.
        lost: usize,
    },
    /// A timeout or deadline expired before data became available.
    Timeout,
}

/// Single-owner broadcast receiver.
///
/// `!Clone`, `!Sync`, `Send`. Each receiver independently tracks its read position.
/// Uses `&mut self` to enforce single-thread usage at compile time.
pub struct Receiver<T: Copy> {
    ring: Arc<RingBuf<T>>,
    local_position: u64,
    spin_iterations: usize,
    allow_yield: bool,
    // !Sync: prevent sharing across threads
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
        Self {
            ring,
            local_position: position,
            spin_iterations,
            allow_yield,
            _not_sync: PhantomData,
        }
    }

    /// Non-blocking receive. Returns immediately.
    ///
    /// # State machine (from design §5):
    /// 1. Load `r_top` (Acquire) FIRST
    /// 2. Load `w_top` (Acquire) SECOND
    /// 3. Check overrun: `w_top - local_pos > capacity` -> snap forward, return `Overrun`
    /// 4. Check empty: `local_pos >= r_top` -> return `Empty`
    /// 5. Read slot, advance position, return `Ok(item)`
    ///
    /// For large types, a tear during read is treated as `Empty` (retry later).
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        // MUST load r_top before w_top — see design §5 load ordering rationale
        let r_top = self.ring.r_top().load(Ordering::Acquire);
        let w_top = self.ring.w_top().load(Ordering::Acquire);

        let local_pos = self.local_position;
        let capacity = self.ring.capacity() as u64;

        // Check overrun: sender has lapped us
        if unlikely(w_top.wrapping_sub(local_pos) > capacity) {
            let new_pos = w_top.wrapping_sub(capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }

        // Check empty: no new data published
        if likely(local_pos >= r_top) {
            return Err(RecvError::Empty);
        }

        // Read the slot and advance
        unsafe {
            match self.ring.read_item(local_pos) {
                Some(item) => {
                    self.local_position = local_pos.wrapping_add(1);
                    Ok(item)
                }
                None => {
                    // Tear detected — the sender is currently writing this slot.
                    // Treat as if the data is not yet available.
                    Err(RecvError::Empty)
                }
            }
        }
    }

    /// Blocking receive. Spins until data is available using tiered backoff.
    #[inline]
    pub fn recv(&mut self) -> T {
        // Fast path: try immediately
        if let Ok(item) = self.try_recv() {
            return item;
        }

        self.recv_slow()
    }

    #[inline(never)]
    fn recv_slow(&mut self) -> T {
        let mut spin_count = 0u32;
        loop {
            match self.try_recv() {
                Ok(item) => return item,
                Err(RecvError::Overrun { .. }) => {
                    // Repositioned — try again immediately
                    continue;
                }
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
                Err(RecvError::Timeout) => unreachable!(),
            }
        }
    }

    /// Blocking receive with timeout.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvError> {
        self.recv_deadline(Instant::now() + timeout)
    }

    /// Blocking receive with absolute deadline.
    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<T, RecvError> {
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

    /// Non-blocking batch receive. Fills buffer with available items.
    /// Returns `Ok(count)` or `Err(RecvError::Overrun)` if lapped.
    ///
    /// For small types: reads all available items with prefetch (no tears possible).
    /// For large types: reads until a tear is detected, then stops.
    #[inline]
    pub fn try_recv_batch(&mut self, buf: &mut [T]) -> Result<usize, RecvError> {
        if buf.is_empty() {
            return Ok(0);
        }

        let r_top = self.ring.r_top().load(Ordering::Acquire);
        let w_top = self.ring.w_top().load(Ordering::Acquire);

        let local_pos = self.local_position;
        let capacity = self.ring.capacity() as u64;

        // Check overrun
        if unlikely(w_top.wrapping_sub(local_pos) > capacity) {
            let new_pos = w_top.wrapping_sub(capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }

        // Check empty
        if local_pos >= r_top {
            return Ok(0);
        }

        // Read as many items as available, up to buf.len()
        let available = r_top.wrapping_sub(local_pos) as usize;
        let count = available.min(buf.len());

        if const { std::mem::size_of::<T>() <= 8 } {
            // Small path: no tears possible, read with prefetch
            for i in 0..count {
                unsafe {
                    let pos = local_pos.wrapping_add(i as u64);
                    // Prefetch next slot
                    if i + 1 < count {
                        crate::hint::prefetch_read(
                            self.ring.slot_ptr_raw(pos.wrapping_add(1)),
                        );
                    }
                    // Safety: small T always returns Some
                    buf[i] = self.ring.read_item(pos).unwrap_unchecked();
                }
            }
            self.local_position = local_pos.wrapping_add(count as u64);
            Ok(count)
        } else {
            // Large path: stop on tear
            let mut actual = 0;
            for i in 0..count {
                let pos = local_pos.wrapping_add(i as u64);
                unsafe {
                    match self.ring.read_item(pos) {
                        Some(item) => {
                            buf[actual] = item;
                            actual += 1;
                        }
                        None => break, // Tear — stop batch here
                    }
                }
            }
            self.local_position = local_pos.wrapping_add(actual as u64);
            Ok(actual)
        }
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
                Ok(0) => {
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
        let w_top = self.ring.w_top().load(Ordering::Acquire);
        let capacity = self.ring.capacity() as u64;
        let local_pos = self.local_position;

        if w_top.wrapping_sub(local_pos) > capacity {
            let new_pos = w_top.wrapping_sub(capacity);
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
        let r_top = self.ring.r_top().load(Ordering::Acquire);
        let local_pos = self.local_position;
        if r_top > local_pos {
            r_top.wrapping_sub(local_pos) as usize
        } else {
            0
        }
    }
}
