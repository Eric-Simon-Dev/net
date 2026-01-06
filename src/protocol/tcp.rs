//! Protocols over TCP.

mod header;

pub use header::{CreateError as HeaderCreateError, DecodeError as HeaderDecodeError, Header};

pub(super) use header::MAX_PAYLOAD_LENGTH;
