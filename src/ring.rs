use std::alloc::Layout;
use std::sync::atomic::AtomicU64;

use crate::alloc::Allocator;

/// Core ring buffer shared between sender and receivers via `Arc`.
///
/// Cache-line layout (64-byte alignment):
/// - Line 1 (0-63):   data_ptr, capacity, mask, padding  (read-only after init)
/// - Line 2 (64-127): w_top (sender writes, receiver reads for overrun)
/// - Line 3 (128-191): r_top (sender writes to publish, receiver polls)
#[repr(C, align(64))]
pub struct RingBuf<T: Copy> {
    // --- Cache line 1: read-only after init ---
    data_ptr: *mut T,
    capacity: usize,
    mask: u64,
    _pad0: [u8; 40],

    // --- Cache line 2: w_top ---
    w_top: AtomicU64,
    _pad1: [u8; 56],

    // --- Cache line 3: r_top ---
    r_top: AtomicU64,
    _pad2: [u8; 56],

    // --- Metadata (not on hot path) ---
    layout: Layout,
    alloc: Box<dyn Allocator>,
}

// Safety: RingBuf is shared via Arc between sender and receivers.
// All mutable state is accessed through atomics. T: Send is required
// because data is transferred between threads.
unsafe impl<T: Copy + Send> Send for RingBuf<T> {}
unsafe impl<T: Copy + Send> Sync for RingBuf<T> {}

impl<T: Copy> RingBuf<T> {
    pub fn new(capacity: usize, alloc: Box<dyn Allocator>) -> Self {
        const {
            assert!(
                size_of::<T>() <= 8,
                "ringcast requires size_of::<T>() <= 8 for atomic read/write guarantee. Use ringcast::large::bounded() for larger types."
            );
        }
        assert!(capacity > 0, "ringcast: capacity must be > 0");

        // Round up to next power of two
        let capacity = capacity.next_power_of_two();
        let mask = (capacity as u64).wrapping_sub(1);

        let layout = Layout::array::<T>(capacity).expect("ringcast: layout overflow");

        let data_ptr = alloc.alloc(layout) as *mut T;
        assert!(
            !data_ptr.is_null(),
            "ringcast: allocation failed for {} elements",
            capacity
        );

        Self {
            data_ptr,
            capacity,
            mask,
            _pad0: [0; 40],
            w_top: AtomicU64::new(0),
            _pad1: [0; 56],
            r_top: AtomicU64::new(0),
            _pad2: [0; 56],
            layout,
            alloc,
        }
    }

    #[inline(always)]
    pub fn w_top(&self) -> &AtomicU64 {
        &self.w_top
    }

    #[inline(always)]
    pub fn r_top(&self) -> &AtomicU64 {
        &self.r_top
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn mask(&self) -> u64 {
        self.mask
    }

    #[inline(always)]
    pub unsafe fn slot_ptr(&self, position: u64) -> *mut T {
        unsafe { self.data_ptr.add((position & self.mask) as usize) }
    }
}

impl<T: Copy> Drop for RingBuf<T> {
    fn drop(&mut self) {
        unsafe {
            self.alloc.dealloc(self.data_ptr as *mut u8, self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_line_layout() {
        use std::mem;
        // Verify the struct is 64-byte aligned
        assert_eq!(mem::align_of::<RingBuf<u64>>() % 64, 0);

        // Verify field offsets for cache line separation
        // w_top should be at offset 64 (cache line 2)
        let offset_w_top = memoffset(|r: &RingBuf<u64>| &r.w_top);
        assert_eq!(offset_w_top, 64, "w_top should be at cache line 2");

        // r_top should be at offset 128 (cache line 3)
        let offset_r_top = memoffset(|r: &RingBuf<u64>| &r.r_top);
        assert_eq!(offset_r_top, 128, "r_top should be at cache line 3");
    }

    fn memoffset<T: Copy, F, R>(f: F) -> usize
    where
        F: Fn(&RingBuf<T>) -> &R,
    {
        // Create a temporary RingBuf to measure offsets
        let ring = RingBuf::<T>::new(1, Box::new(crate::alloc::HugePageAllocator::new()));
        let base = &ring as *const _ as usize;
        let field = f(&ring) as *const _ as usize;
        field - base
    }

    #[test]
    fn test_power_of_two_rounding() {
        let ring = RingBuf::<u64>::new(3, Box::new(crate::alloc::HugePageAllocator::new()));
        assert_eq!(ring.capacity(), 4);
        assert_eq!(ring.mask(), 3);

        let ring = RingBuf::<u64>::new(1, Box::new(crate::alloc::HugePageAllocator::new()));
        assert_eq!(ring.capacity(), 1);
        assert_eq!(ring.mask(), 0);
    }
}
