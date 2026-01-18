//! Protocols over TCP.

mod header;

pub use header::{CreateError as CreateHeaderError, DecodeError as DecodeHeaderError, Header};

pub(super) use header::MAX_PAYLOAD_LENGTH;
