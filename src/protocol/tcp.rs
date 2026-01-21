//! Protocols over TCP.

mod header;

pub use header::{DecodeError as DecodeHeaderError, EncodeError as EncodeHeaderError, Header};

pub(super) use header::MAX_PAYLOAD_LENGTH;
