use bytes::{BufMut, BytesMut};
use thiserror::Error;

/// Index of the header variant byte for all on-wire representations.
///
/// This byte is always stored first and determines how the rest of the
/// header should be interpreted.
const VARIANT_INDEX: usize = 0;

mod classic {
    pub const VARIANT: u8 = 0;

    // ---- Wire format ----
    pub const CHANNEL_INDEX: usize = 1;
    pub const SEQ_RANGE: std::ops::Range<usize> = 2..10;
    pub const LEN: usize = 10;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Header {
    /// # Wire format
    /// ```text
    /// byte 0      : VARIANT
    /// byte 1      : channel
    /// byte 2..10  : sequence number (u64, big-endian)
    /// ```
    Classic { channel: u8, seq: u64 },
}

impl Header {
    pub fn channel(&self) -> u8 {
        match *self {
            Header::Classic { channel, .. } => channel,
        }
    }

    pub fn seq(&self) -> u64 {
        match *self {
            Header::Classic { seq, .. } => seq,
        }
    }

    /// Encodes and appends this header to the end of `buf`.
    pub fn put_into(&self, buf: &mut BytesMut) {
        match *self {
            Header::Classic { channel, seq } => {
                // ---- Encoding ----
                let mut header_bytes = [0; classic::LEN];
                header_bytes[VARIANT_INDEX] = classic::VARIANT;
                header_bytes[classic::CHANNEL_INDEX] = channel;
                header_bytes[classic::SEQ_RANGE].copy_from_slice(&seq.to_be_bytes());

                // Append encoded header.
                buf.put(&header_bytes[..]);
            }
        }
    }

    /// Decodes and removes a header from the front of `buf`.
    ///
    /// # Errors
    /// - [`DecodeError::BufferTooSmall`] if the buffer does not
    ///   contain enough bytes to decode a complete header.
    /// - [`DecodeError::UnknownVariant`] if the header variant
    ///   byte is not recognized.
    pub fn split_from(buf: &mut BytesMut) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Err(DecodeError::BufferTooSmall);
        }

        match buf[VARIANT_INDEX] {
            classic::VARIANT => {
                if buf.len() < classic::LEN {
                    return Err(DecodeError::BufferTooSmall);
                }

                // Remove encoded header.
                let header_bytes = buf.split_to(classic::LEN);

                // ---- Decoding ----
                let header = Header::Classic {
                    channel: header_bytes[classic::CHANNEL_INDEX],
                    seq: u64::from_be_bytes(header_bytes[classic::SEQ_RANGE].try_into().unwrap()),
                };

                Ok(header)
            }
            variant => Err(DecodeError::UnknownVariant(variant)),
        }
    }
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("buffer does not contain enough bytes to decode a complete header")]
    BufferTooSmall,

    #[error("unrecognized header variant: {0}")]
    UnknownVariant(u8),
}
