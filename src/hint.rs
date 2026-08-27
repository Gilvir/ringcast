#[inline(never)]
#[cold]
fn cold_path() {}

#[inline(always)]
pub fn likely(b: bool) -> bool {
    if !b {
        cold_path();
    }
    b
}

#[inline(always)]
pub fn unlikely(b: bool) -> bool {
    if b {
        cold_path();
    }
    b
}

#[inline(always)]
pub fn spin_pause() {
    #[cfg(target_arch = "x86_64")]
    core::arch::x86_64::_mm_pause();
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("isb sy");
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        core::hint::spin_loop();
    }
}

#[inline(always)]
pub unsafe fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = ptr;
    }
}

#[inline(always)]
pub unsafe fn prefetch_write(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::asm!("prefetchw [{}]", in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pstl1keep, [{x}]", x = in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}
