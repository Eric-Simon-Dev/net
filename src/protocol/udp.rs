//! Protocols over UDP.

mod header;
mod sliding_window;

#[allow(unused)]
pub use header::{DecodeError as DecodeHeaderError, Header};
pub use sliding_window::SlidingWindow;

pub const MAX_PACKET_SIZE: usize = 1024;
