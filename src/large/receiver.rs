use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::hint::{likely, spin_pause, unlikely};
use crate::RecvError;

use super::ring::LargeRingBuf;

/// Single-owner broadcast receiver for large types.
pub struct Receiver<T: Copy> {
    ring: Arc<LargeRingBuf<T>>,
    local_position: u64,
    spin_iterations: usize,
    allow_yield: bool,
    _not_sync: PhantomData<*const ()>,
}

unsafe impl<T: Copy + Send> Send for Receiver<T> {}

impl<T: Copy> Receiver<T> {
    pub(crate) fn new(
        ring: Arc<LargeRingBuf<T>>,
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

    /// Non-blocking receive with tear detection.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
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
        if likely(local_pos >= r_top) {
            return Err(RecvError::Empty);
        }

        // Read with tear detection — retry on tear
        unsafe {
            match self.ring.read_slot(local_pos) {
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

    /// Blocking receive.
    #[inline]
    pub fn recv(&mut self) -> T {
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
                Err(RecvError::Overrun { .. }) => continue,
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

    /// Non-blocking batch receive.
    #[inline]
    pub fn try_recv_batch(&mut self, buf: &mut [T]) -> Result<usize, RecvError> {
        if buf.is_empty() {
            return Ok(0);
        }

        let r_top = self.ring.r_top().load(Ordering::Acquire);
        let w_top = self.ring.w_top().load(Ordering::Acquire);

        let local_pos = self.local_position;
        let capacity = self.ring.capacity() as u64;

        if unlikely(w_top.wrapping_sub(local_pos) > capacity) {
            let new_pos = w_top.wrapping_sub(capacity);
            let lost = new_pos.wrapping_sub(local_pos) as usize;
            self.local_position = new_pos;
            return Err(RecvError::Overrun { lost });
        }

        if local_pos >= r_top {
            return Ok(0);
        }

        let available = r_top.wrapping_sub(local_pos) as usize;
        let max_count = available.min(buf.len());
        let mut count = 0;

        for i in 0..max_count {
            let pos = local_pos.wrapping_add(i as u64);
            unsafe {
                match self.ring.read_slot(pos) {
                    Some(item) => {
                        buf[count] = item;
                        count += 1;
                    }
                    None => break, // Tear — stop batch here
                }
            }
        }

        self.local_position = local_pos.wrapping_add(count as u64);
        Ok(count)
    }

    /// Blocking batch receive.
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
                Err(RecvError::Overrun { .. }) => continue,
                _ => unreachable!(),
            }
        }
    }

    /// Check if the receiver has been lapped.
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

    /// Returns the number of items available to read.
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
