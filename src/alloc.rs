use std::alloc::Layout;

/// # Safety
///
/// Implementations must return properly aligned, zeroed memory from `alloc`,
/// and correctly free it in `dealloc`. The returned pointer must be valid for
/// the given layout.
pub unsafe trait Allocator: Send + Sync {
    /// Allocate memory for `layout`, returning a properly aligned, zeroed pointer.
    fn alloc(&self, layout: Layout) -> *mut u8;

    /// # Safety
    ///
    /// `ptr` must have been returned by a prior call to `alloc` with the same layout.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout);
}

/// Tries Linux huge pages first (falling back to regular `mmap`); on non-Linux
/// platforms or under Miri, falls back to the system allocator.
pub struct HugePageAllocator;

impl HugePageAllocator {
    /// Create a new `HugePageAllocator`.
    pub fn new() -> Self {
        HugePageAllocator
    }
}

impl Default for HugePageAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(target_os = "linux", not(miri)))]
unsafe impl Allocator for HugePageAllocator {
    fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                layout.size(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
                -1,
                0,
            )
        };

        if ptr != libc::MAP_FAILED {
            return ptr as *mut u8;
        }

        // Fallback: regular mmap with MAP_POPULATE to pre-fault pages
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                layout.size(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            panic!(
                "mmap failed to allocate {} bytes: {}",
                layout.size(),
                std::io::Error::last_os_error()
            );
        }

        ptr as *mut u8
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let ret = unsafe { libc::munmap(ptr as *mut libc::c_void, layout.size()) };
        debug_assert_eq!(ret, 0, "munmap failed");
    }
}

#[cfg(any(miri, not(target_os = "linux")))]
unsafe impl Allocator for HugePageAllocator {
    fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { std::alloc::alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { std::alloc::dealloc(ptr, layout) };
    }
}
