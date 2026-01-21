use bytes::{BufMut, BytesMut};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub seq: u64,
    pub channel: u8,
}

// ---- Wire format ----
const SEQ_RANGE: std::ops::Range<usize> = 0..8;
const CHANNEL_INDEX: usize = 8;
const LEN: usize = 9;

// =============================================================================
// Encode
// =============================================================================

impl Header {
    /// Returns header wire size.
    pub fn encode_into(&self, buf: &mut BytesMut) -> usize {
        let mut encoded_header = [0; LEN];
        encoded_header[SEQ_RANGE].copy_from_slice(&self.seq.to_be_bytes());
        encoded_header[CHANNEL_INDEX] = self.channel;

        buf.put(&encoded_header[..]);

        LEN
    }
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

        let seq = u64::from_be_bytes(buf[SEQ_RANGE].try_into().unwrap());

        let channel = buf[CHANNEL_INDEX];

        let header = Self { seq, channel };

        Ok((header, LEN))
    }
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("buffer too small")]
    BufferTooSmall,
}
