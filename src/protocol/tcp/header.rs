use std::fmt;

use bytes::{BufMut, BytesMut};

/// Maximum allowed payload length in bytes.
///
/// This limit is enforced while decoding frames.
pub const MAX_PAYLOAD_LENGTH: u32 = 1_048_576; // 1 MiB

// ---- Wire format ----
const LENGTH_RANGE: std::ops::Range<usize> = 0..4;
const CHANNEL_INDEX: usize = 4;
const LEN: usize = 5;

/// # Wire format
/// ```text
/// byte 0..4 : payload length (u32, big-endian)
/// byte 4    : channel
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Header {
    pub length: u32,
    pub channel: u8,
}

impl Header {
    /// Encodes and appends this header to the end of `buf`.
    pub fn put_into(&self, buf: &mut BytesMut) {
        let mut header_bytes = [0; LEN];
        header_bytes[LENGTH_RANGE].copy_from_slice(&self.length.to_be_bytes());
        header_bytes[CHANNEL_INDEX] = self.channel;
        buf.put(&header_bytes[..]);
    }

    /// Decodes a header and removes a complete frame from the front of `buf`.
    ///
    /// # Errors
    /// - [`HeaderDecodingError::BufferTooSmall`] if the buffer does not
    ///   contain enough bytes to decode a full header and payload.
    /// - [`HeaderDecodingError::PayloadTooBig`] if the decoded payload
    ///   length exceeds [`MAX_PAYLOAD_LENGTH`].
    pub fn split_frame_from(buf: &mut BytesMut) -> Result<(Self, BytesMut), HeaderDecodingError> {
        if buf.len() < LEN {
            return Err(HeaderDecodingError::BufferTooSmall);
        }

        // Decode and validate payload length without consuming bytes.
        let length = u32::from_be_bytes(buf[LENGTH_RANGE].try_into().unwrap());
        if length > MAX_PAYLOAD_LENGTH {
            return Err(HeaderDecodingError::PayloadTooBig);
        }

        // Check whether the full frame is present.
        if buf.len() < LEN + length as usize {
            return Err(HeaderDecodingError::BufferTooSmall);
        }

        // Remove encoded header.
        let header_bytes = buf.split_to(LEN);

        // ---- Decoding ----
        let header = Header {
            length: u32::from_be_bytes(header_bytes[LENGTH_RANGE].try_into().unwrap()),
            channel: header_bytes[CHANNEL_INDEX],
        };

        // Remove payload.
        let payload = buf.split_to(length as usize);

        Ok((header, payload))
    }
}

#[derive(Debug)]
pub enum HeaderDecodingError {
    /// The buffer does not contain enough bytes to decode a full frame.
    BufferTooSmall,
    /// The decoded payload length exceeds [`MAX_PAYLOAD_LENGTH`].
    PayloadTooBig,
}

impl fmt::Display for HeaderDecodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeaderDecodingError::BufferTooSmall => write!(f, "buffer is too small"),
            HeaderDecodingError::PayloadTooBig => write!(f, "payload is too big"),
        }
    }
}

impl std::error::Error for HeaderDecodingError {}
