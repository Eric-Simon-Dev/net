use bytes::{BufMut, BytesMut};
use thiserror::Error;

/// Maximum allowed payload length in bytes.
///
/// This limit is enforced while creating and decoding headers.
pub const MAX_PAYLOAD_LENGTH: u32 = 1_048_576; // 1 MiB

// ---- Wire format ----
const PAYLOAD_LENGTH_RANGE: std::ops::Range<usize> = 0..4;
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
    payload_length: u32,
    pub channel: u8,
}

impl Header {
    /// # Errors
    /// - [`CreateError::PayloadTooBig`] if the provided payload
    ///   length exceeds [`crate::MAX_PAYLOAD_LENGTH`].
    pub fn new(payload_length: usize, channel: u8) -> Result<Self, CreateError> {
        let Ok(payload_length) = u32::try_from(payload_length) else {
            return Err(CreateError::PayloadTooBig);
        };

        if payload_length > MAX_PAYLOAD_LENGTH {
            return Err(CreateError::PayloadTooBig);
        }

        Ok(Self {
            payload_length,
            channel,
        })
    }
}

impl Header {
    /// Encodes and appends this header to the end of `buf`.
    pub fn put_into(&self, buf: &mut BytesMut) {
        // ---- Encoding ----
        let mut header_bytes = [0; LEN];
        header_bytes[PAYLOAD_LENGTH_RANGE].copy_from_slice(&self.payload_length.to_be_bytes());
        header_bytes[CHANNEL_INDEX] = self.channel;

        // Append encoded header.
        buf.put(&header_bytes[..]);
    }

    /// Decodes a header and removes a complete frame from the front of `buf`.
    ///
    /// # Errors
    /// - [`DecodeError::BufferTooSmall`] if the buffer does not
    ///   contain enough bytes to decode a full frame.
    /// - [`DecodeError::PayloadTooBig`] if the decoded payload
    ///   length exceeds [`crate::MAX_PAYLOAD_LENGTH`].
    pub fn split_frame_from(buf: &mut BytesMut) -> Result<(Self, BytesMut), DecodeError> {
        if buf.len() < LEN {
            return Err(DecodeError::BufferTooSmall);
        }

        // Decode and validate payload length without consuming bytes.
        let payload_length = u32::from_be_bytes(buf[PAYLOAD_LENGTH_RANGE].try_into().unwrap());
        if payload_length > MAX_PAYLOAD_LENGTH {
            return Err(DecodeError::PayloadTooBig);
        }

        // Check whether the full frame is present.
        if buf.len() < LEN + payload_length as usize {
            return Err(DecodeError::BufferTooSmall);
        }

        // Remove encoded header.
        let header_bytes = buf.split_to(LEN);

        // ---- Decoding ----
        let header = Header {
            payload_length: u32::from_be_bytes(
                header_bytes[PAYLOAD_LENGTH_RANGE].try_into().unwrap(),
            ),
            channel: header_bytes[CHANNEL_INDEX],
        };

        // Remove payload.
        let payload = buf.split_to(payload_length as usize);

        Ok((header, payload))
    }
}

// ---- Errors ----

#[derive(Debug, Error)]
pub enum CreateError {
    #[error("provided payload length exceeds `crate::MAX_PAYLOAD_LENGTH`")]
    PayloadTooBig,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("buffer does not contain enough bytes to decode a full frame")]
    BufferTooSmall,

    #[error("decoded payload length exceeds `crate::MAX_PAYLOAD_LENGTH`")]
    PayloadTooBig,
}
