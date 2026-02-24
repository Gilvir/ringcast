use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::alloc::Allocator;

/// Per-slot layout for large types with tear detection.
///
/// ```text
/// +----------+--------------------+----------+
/// | seq_pre  |       data: T      | seq_post |
/// | (u64)    |                    | (u64)    |
/// +----------+--------------------+----------+
/// ```
#[repr(C)]
pub(crate) struct Slot<T> {
    seq_pre: AtomicU64,
    data: UnsafeCell<T>,
    seq_post: AtomicU64,
}

impl<T: Copy> Slot<T> {
    fn init() -> Self {
        Self {
            seq_pre: AtomicU64::new(0),
            data: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            seq_post: AtomicU64::new(0),
        }
    }
}

/// Core ring buffer shared between sender and receivers via `Arc`.
///
/// Cache-line layout (64-byte alignment):
/// - Line 1 (0-63):   data_ptr, capacity, mask, padding  (read-only after init)
/// - Line 2 (64-127): w_top (sender writes, receiver reads for overrun)
/// - Line 3 (128-191): r_top (sender writes to publish, receiver polls)
///
/// For `size_of::<T>() <= 8`, data slots are bare `T` values (naturally atomic
/// on x86-64/AArch64). For larger types, each slot is wrapped in a `Slot<T>`
/// with per-slot sequence counters for tear detection. The path is selected at
/// compile time via `const { size_of::<T>() <= 8 }` — zero overhead for the
/// small-type path.
#[repr(C, align(64))]
pub struct RingBuf<T: Copy> {
    // --- Cache line 1: read-only after init ---
    data_ptr: *mut u8,
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

        // Round up to next power of two
        let capacity = capacity.next_power_of_two();
        let mask = (capacity as u64).wrapping_sub(1);

        let (data_ptr, layout) = if const { std::mem::size_of::<T>() <= 8 } {
            let layout = Layout::array::<T>(capacity).expect("ringcast: layout overflow");
            let data_ptr = alloc.alloc(layout);
            assert!(
                !data_ptr.is_null(),
                "ringcast: allocation failed for {} elements",
                capacity
            );
            (data_ptr, layout)
        } else {
            let layout =
                Layout::array::<Slot<T>>(capacity).expect("ringcast: layout overflow");
            let data_ptr = alloc.alloc(layout);
            assert!(
                !data_ptr.is_null(),
                "ringcast: allocation failed for {} slots",
                capacity
            );
            // Initialize all slots with zeroed sequence counters
            for i in 0..capacity {
                unsafe {
                    std::ptr::write((data_ptr as *mut Slot<T>).add(i), Slot::init());
                }
            }
            (data_ptr, layout)
        };

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
    #[allow(dead_code)]
    pub fn mask(&self) -> u64 {
        self.mask
    }

    /// Write an item to the ring at the given position.
    ///
    /// For small types (`size_of::<T>() <= 8`): bare `ptr::write` (naturally atomic).
    /// For large types: seqlock protocol (seq_pre -> write -> seq_post).
    #[inline(always)]
    pub unsafe fn write_item(&self, position: u64, item: T) {
        let idx = (position & self.mask) as usize;
        if const { std::mem::size_of::<T>() <= 8 } {
            unsafe {
                std::ptr::write((self.data_ptr as *mut T).add(idx), item);
            }
        } else {
            unsafe {
                let slot = &*(self.data_ptr as *mut Slot<T>).add(idx);
                slot.seq_pre.store(position, Ordering::Release);
                std::ptr::write(slot.data.get(), item);
                slot.seq_post.store(position, Ordering::Release);
            }
        }
    }

    /// Read an item from the ring at the given position.
    ///
    /// For small types (`size_of::<T>() <= 8`): bare `ptr::read`, always returns `Some`.
    /// For large types: seqlock read with tear detection. Returns `None` on tear.
    #[inline(always)]
    pub unsafe fn read_item(&self, position: u64) -> Option<T> {
        let idx = (position & self.mask) as usize;
        if const { std::mem::size_of::<T>() <= 8 } {
            Some(unsafe { std::ptr::read((self.data_ptr as *const T).add(idx)) })
        } else {
            unsafe {
                let slot = &*(self.data_ptr as *const Slot<T>).add(idx);
                let pre = slot.seq_pre.load(Ordering::Acquire);
                let item = std::ptr::read(slot.data.get());
                let post = slot.seq_post.load(Ordering::Acquire);
                if pre == post && pre == position {
                    Some(item)
                } else {
                    None
                }
            }
        }
    }

    /// Raw pointer to the slot at the given position, for prefetch hints.
    #[inline(always)]
    pub unsafe fn slot_ptr_raw(&self, position: u64) -> *const u8 {
        let idx = (position & self.mask) as usize;
        if const { std::mem::size_of::<T>() <= 8 } {
            unsafe { (self.data_ptr as *const T).add(idx) as *const u8 }
        } else {
            unsafe { (self.data_ptr as *const Slot<T>).add(idx) as *const u8 }
        }
    }
}

impl<T: Copy> Drop for RingBuf<T> {
    fn drop(&mut self) {
        unsafe {
            self.alloc.dealloc(self.data_ptr, self.layout);
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
