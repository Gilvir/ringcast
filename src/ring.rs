use std::alloc::Layout;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;

use crate::alloc::Allocator;

/// Core ring buffer shared between sender and receivers via `Arc`.
///
/// Cache-line layout (64-byte alignment):
/// - Line 1 (0-63):   data_ptr, capacity, mask, padding  (read-only after init)
/// - Line 2 (64-127): w_top (sender writes, receiver reads for overrun)
/// - Line 3 (128-191): r_top (sender writes to publish, receiver polls)
///
/// Overwrite detection relies on the w_top pre/post bracket in the receiver:
/// a torn read requires the sender to lap the receiver (advancing w_top by
/// >= capacity), which the bracket catches for all type sizes.
#[repr(C, align(64))]
pub struct RingBuf<T: Copy> {
    data_ptr: *mut u8,
    capacity: usize,
    mask: u64,
    _pad0: [u8; 40],

    w_top: AtomicU64,
    _pad1: [u8; 56],

    r_top: AtomicU64,
    _pad2: [u8; 56],

    _marker: PhantomData<T>,
}

// Safety: RingBuf is shared via Arc between sender and receivers.
// All mutable state is accessed through atomics. T: Send is required
// because data is transferred between threads.
unsafe impl<T: Copy + Send> Send for RingBuf<T> {}
unsafe impl<T: Copy + Send> Sync for RingBuf<T> {}

impl<T: Copy> RingBuf<T> {
    pub fn new(capacity: usize, alloc: Box<dyn Allocator>) -> Self {
        assert!(capacity > 0, "ringcast: capacity must be > 0");

        let capacity = capacity.next_power_of_two();
        let mask = (capacity as u64).wrapping_sub(1);

        let layout = Layout::array::<T>(capacity).expect("ringcast: layout overflow");
        let data_ptr = alloc.alloc(layout);
        assert!(
            !data_ptr.is_null(),
            "ringcast: allocation failed for {capacity} elements",
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
            _marker: PhantomData,
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
    pub fn mask(&self) -> u64 {
        self.mask
    }

    #[inline(always)]
    pub fn data_ptr(&self) -> *const u8 {
        self.data_ptr
    }

    #[inline(always)]
    pub unsafe fn write_item(&self, position: u64, item: T) {
        let idx = (position & self.mask) as usize;
        unsafe {
            std::ptr::write((self.data_ptr as *mut T).add(idx), item);
        }
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub unsafe fn read_item(&self, position: u64) -> T {
        let idx = (position & self.mask) as usize;
        unsafe { std::ptr::read((self.data_ptr as *const T).add(idx)) }
    }

    /// For prefetch hints.
    #[inline(always)]
    pub unsafe fn slot_ptr_raw(&self, position: u64) -> *const u8 {
        let idx = (position & self.mask) as usize;
        unsafe { (self.data_ptr as *const T).add(idx) as *const u8 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_line_layout() {
        use std::mem;
        assert_eq!(mem::align_of::<RingBuf<u64>>() % 64, 0);

        let offset_w_top = memoffset(|r: &RingBuf<u64>| &r.w_top);
        assert_eq!(offset_w_top, 64, "w_top should be at cache line 2");

        let offset_r_top = memoffset(|r: &RingBuf<u64>| &r.r_top);
        assert_eq!(offset_r_top, 128, "r_top should be at cache line 3");
    }

    fn memoffset<T: Copy, F, R>(f: F) -> usize
    where
        F: Fn(&RingBuf<T>) -> &R,
    {
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
