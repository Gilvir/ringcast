use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::alloc::Allocator;

/// Per-slot layout for large types with tear detection.
///
/// ```text
/// ┌──────────┬────────────────────┬──────────┐
/// │ seq_pre  │       data: T      │ seq_post │
/// │ (u64)    │                    │ (u64)    │
/// └──────────┴────────────────────┴──────────┘
/// ```
#[repr(C)]
pub struct Slot<T> {
    pub seq_pre: AtomicU64,
    pub data: UnsafeCell<T>,
    pub seq_post: AtomicU64,
}

impl<T> Slot<T> {
    fn init() -> Self
    where
        T: Copy,
    {
        Self {
            seq_pre: AtomicU64::new(0),
            data: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            seq_post: AtomicU64::new(0),
        }
    }
}

/// Ring buffer for large types (`size_of::<T>() > 8`).
///
/// Uses per-slot sequence counters for tear detection instead of
/// relying on naturally atomic reads/writes.
#[repr(C, align(64))]
pub struct LargeRingBuf<T: Copy> {
    // --- Cache line 1: read-only after init ---
    data_ptr: *mut Slot<T>,
    capacity: usize,
    mask: u64,
    _pad0: [u8; 40],

    // --- Cache line 2: w_top ---
    w_top: AtomicU64,
    _pad1: [u8; 56],

    // --- Cache line 3: r_top ---
    r_top: AtomicU64,
    _pad2: [u8; 56],

    // --- Metadata ---
    layout: Layout,
    alloc: Box<dyn Allocator>,
}

unsafe impl<T: Copy + Send> Send for LargeRingBuf<T> {}
unsafe impl<T: Copy + Send> Sync for LargeRingBuf<T> {}

impl<T: Copy> LargeRingBuf<T> {
    pub fn new(capacity: usize, alloc: Box<dyn Allocator>) -> Self {
        assert!(capacity > 0, "ringcast::large: capacity must be > 0");

        let capacity = capacity.next_power_of_two();
        let mask = (capacity as u64).wrapping_sub(1);

        let layout = Layout::array::<Slot<T>>(capacity).expect("ringcast::large: layout overflow");

        let data_ptr = alloc.alloc(layout) as *mut Slot<T>;
        assert!(
            !data_ptr.is_null(),
            "ringcast::large: allocation failed for {} slots",
            capacity
        );

        // Initialize all slots
        for i in 0..capacity {
            unsafe {
                std::ptr::write(data_ptr.add(i), Slot::init());
            }
        }

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
    pub unsafe fn slot(&self, position: u64) -> &Slot<T> {
        unsafe { &*self.data_ptr.add((position & self.mask) as usize) }
    }

    /// Write protocol: seq_pre(Release), data write, seq_post(Release)
    #[inline(always)]
    pub unsafe fn write_slot(&self, position: u64, item: T) {
        let slot = unsafe { self.slot(position) };
        slot.seq_pre.store(position, Ordering::Release);
        unsafe { std::ptr::write(slot.data.get(), item) };
        slot.seq_post.store(position, Ordering::Release);
    }

    /// Read protocol: load seq_pre(Acquire), read data, load seq_post(Acquire), validate
    ///
    /// Returns `Some(item)` if the read was consistent, `None` if a tear was detected.
    #[inline(always)]
    pub unsafe fn read_slot(&self, position: u64) -> Option<T> {
        let slot = unsafe { self.slot(position) };
        let pre = slot.seq_pre.load(Ordering::Acquire);
        let item = unsafe { std::ptr::read(slot.data.get()) };
        let post = slot.seq_post.load(Ordering::Acquire);

        if pre == post && pre == position {
            Some(item)
        } else {
            None
        }
    }
}

impl<T: Copy> Drop for LargeRingBuf<T> {
    fn drop(&mut self) {
        unsafe {
            self.alloc.dealloc(self.data_ptr as *mut u8, self.layout);
        }
    }
}
