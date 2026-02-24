pub mod alloc;
mod builder;
mod hint;
mod receiver;
mod ring;
mod sender;

pub use builder::Builder;
pub use receiver::RecvError;
pub use receiver::Receiver;
pub use sender::Sender;

/// Create a bounded broadcast channel with the given capacity.
///
/// Returns `(Sender<T>, Receiver<T>)`. Additional receivers can be
/// created via `sender.subscribe()`.
///
/// Capacity is rounded up to the next power of two. Uses `HugePageAllocator`
/// by default.
///
/// For `size_of::<T>() <= 8`, data slots use naturally atomic reads/writes.
/// For larger types, per-slot sequence counters provide tear detection with
/// ~5-10ns additional latency per operation. The path is selected at compile
/// time — zero overhead for small types.
///
/// # Panics
/// Panics if `capacity` is 0.
pub fn bounded<T: Copy>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    Builder::new().capacity(capacity).build()
}

/// Create a builder for advanced channel configuration.
pub fn builder<T: Copy>() -> Builder<T> {
    Builder::new()
}
