//! Lock-free SPMC broadcast ring buffer.
//!
//! A single [`Sender`] publishes messages that are received by all [`Receiver`]s.
//! Slow receivers that fall behind are detected (not blocked on), and the lost
//! count is reported via [`RecvError::Overrun`].
//!
//! # Quick start
//!
//! ```
//! let (tx, mut rx) = ringcast::bounded::<u64>(1024);
//! let mut rx2 = tx.subscribe();
//!
//! tx.send(42);
//!
//! assert_eq!(rx.try_recv(), Ok(42));
//! assert_eq!(rx2.try_recv(), Ok(42));
//! ```
//!
//! Use [`bounded`] for simple construction or [`Builder`] for advanced
//! configuration (spin iterations, yield policy, custom allocator).

pub mod alloc;
mod builder;
mod hint;
mod receiver;
mod ring;
mod sender;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub use builder::Builder;
pub use receiver::Receiver;
pub use receiver::RecvError;
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
