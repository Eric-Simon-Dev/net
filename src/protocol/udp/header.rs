use std::fmt;

use bytes::{BufMut, BytesMut};

mod classic {
    pub const VARIANT: u8 = 0;
    pub const LEN: usize = 10;

    // ---- Wire format ----
    pub const CHANNEL_INDEX: usize = 1;
    pub const SEQ_RANGE: std::ops::Range<usize> = 2..10;
}

/// Always first byte.
const VARIANT_INDEX: usize = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Header {
    /// On wire:
    /// - byte 0 : variant (`0`)
    /// - byte 1 : channel
    /// - byte 2..10 : sequence number (`u64`, big-endian)
    Classic { channel: u8, seq: u64 },
}

impl Header {
    /// Appends this header to the end of `buf`.
    ///
    /// This function encodes the header in its on-wire representation and
    /// grows the buffer accordingly.
    pub fn put_into(&self, buf: &mut BytesMut) {
        match *self {
            Header::Classic { channel, seq } => {
                let mut header = [0; classic::LEN];
                header[VARIANT_INDEX] = classic::VARIANT;
                header[classic::CHANNEL_INDEX] = channel;
                header[classic::SEQ_RANGE].copy_from_slice(&seq.to_be_bytes());
                buf.put(&header[..]);
            }
        }
    }

    /// Splits and decodes a header from the front of `buf`.
    ///
    /// On success, the header is removed from `buf` and returned.
    /// The remaining bytes in `buf` represent the payload.
    ///
    /// # Errors
    /// - [`HeaderError::Empty`] if the buffer is empty.
    /// - [`HeaderError::Invalid`] if the buffer does not contain a complete
    ///   header for the recognized variant.
    /// - [`HeaderError::Unknown`] if the header variant is unknown.
    pub fn split_from(buf: &mut BytesMut) -> Result<Self, HeaderDecodingError> {
        if buf.is_empty() {
            return Err(HeaderDecodingError::EmptyBuffer);
        }

        match buf[VARIANT_INDEX] {
            classic::VARIANT => {
                if buf.len() < classic::LEN {
                    return Err(HeaderDecodingError::Invalid);
                }

                let header = buf.split_to(classic::LEN);

                let channel = header[classic::CHANNEL_INDEX];
                let seq = u64::from_be_bytes(header[classic::SEQ_RANGE].try_into().unwrap());

                Ok(Header::Classic { channel, seq })
            }
            variant => Err(HeaderDecodingError::Unknown(variant)),
        }
    }
}

#[derive(Debug)]
pub enum HeaderDecodingError {
    /// The buffer is empty.
    EmptyBuffer,
    /// The header variant is recognized but the data is invalid or incomplete.
    Invalid,
    /// The header variant is unknown.
    Unknown(u8),
}

impl fmt::Display for HeaderDecodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeaderDecodingError::EmptyBuffer => write!(f, "buffer is empty"),
            HeaderDecodingError::Invalid => write!(f, "invalid or incomplete header"),
            HeaderDecodingError::Unknown(v) => write!(f, "unknown header variant {}", v),
        }
    }
}

impl std::error::Error for HeaderDecodingError {}
