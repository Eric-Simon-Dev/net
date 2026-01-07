//! Protocols over UDP.

mod header;
mod sliding_window;

pub use header::Header;
pub use sliding_window::SlidingWindow;

pub const MAX_PACKET_SIZE: usize = 1024;
