use bytes::{BufMut, BytesMut};
use thiserror::Error;

/// # Wire format
/// ```text
/// byte 0..4 : payload length (u32, big-endian)
/// byte 4    : channel
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub payload_length: usize,
    pub channel: u8,
}

// ---- Invariants ----
pub const MAX_PAYLOAD_LENGTH: usize = 1_048_576; // 1 MiB
const _: () = assert!(MAX_PAYLOAD_LENGTH <= u32::MAX as usize);

// ---- Wire format ----
const PAYLOAD_LENGTH_RANGE: std::ops::Range<usize> = 0..4;
const CHANNEL_INDEX: usize = 4;
const LEN: usize = 5;

// =============================================================================
// Encode
// =============================================================================

impl Header {
    /// Returns header wire size.
    pub fn encode_into(&self, buf: &mut BytesMut) -> Result<usize, EncodeError> {
        if self.payload_length > MAX_PAYLOAD_LENGTH {
            return Err(EncodeError::PayloadTooBig);
        }

        let mut encoded_header = [0u8; LEN];
        let payload_length = self.payload_length as u32;
        encoded_header[PAYLOAD_LENGTH_RANGE].copy_from_slice(&payload_length.to_be_bytes());
        encoded_header[CHANNEL_INDEX] = self.channel;

        buf.put(&encoded_header[..]);

        Ok(LEN)
    }
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("payload too big")]
    PayloadTooBig,
}

// =============================================================================
// Decode
// =============================================================================

impl Header {
    /// Returns header and its wire size.
    pub fn decode_from(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < LEN {
            return Err(DecodeError::BufferTooSmall);
        }

        let payload_length = u32::from_be_bytes(buf[PAYLOAD_LENGTH_RANGE].try_into().unwrap());
        let payload_length = payload_length as usize;
        if payload_length > MAX_PAYLOAD_LENGTH {
            return Err(DecodeError::PayloadTooBig);
        }

        let channel = buf[CHANNEL_INDEX];

        let header = Self {
            payload_length,
            channel,
        };

        Ok((header, LEN))
    }
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("buffer too small")]
    BufferTooSmall,

    #[error("payload too big")]
    PayloadTooBig,
}
