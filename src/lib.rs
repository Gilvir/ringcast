pub mod alloc;
mod builder;
mod hint;
pub mod large;
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
/// # Panics
/// Panics if `capacity` is 0.
pub fn bounded<T: Copy>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    Builder::new().capacity(capacity).build()
}

/// Create a builder for advanced channel configuration.
pub fn builder<T: Copy>() -> Builder<T> {
    Builder::new()
}
