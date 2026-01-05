use core::mem::size_of;
use std::error::Error;
use std::fmt;

/// Type used for the size field.
type Size = u32;

pub const SIZE_FIELD_LEN: usize = size_of::<Size>();
pub const MAX_SIZE: usize = 1_048_576; // 1 MiB

// Ensure `usize` can hold any `Size`.
const _: () = assert!(size_of::<usize>() >= size_of::<Size>());

#[derive(Debug)]
pub enum SizeError {
    BufferTooSmall,
    SizeTooLarge { size: usize },
}

impl fmt::Display for SizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SizeError::BufferTooSmall => write!(f, "buffer too small to read size field"),
            SizeError::SizeTooLarge { size } => {
                write!(f, "size {} exceeds maximum allowed {}", size, MAX_SIZE)
            }
        }
    }
}

impl Error for SizeError {}

/// Reads a size from the start of `buf`,
/// checking buffer length and maximum allowed size.
///
/// # Errors
/// - `BufferTooSmall` if `buf` is smaller than the size field.
/// - `SizeTooLarge` if the read size exceeds `MAX_SIZE`.
pub fn read_size(buf: &[u8]) -> Result<usize, SizeError> {
    let size_bytes = buf.get(..SIZE_FIELD_LEN).ok_or(SizeError::BufferTooSmall)?;
    let size = usize::try_from(Size::from_le_bytes(size_bytes.try_into().unwrap())).unwrap();

    if size > MAX_SIZE {
        return Err(SizeError::SizeTooLarge { size });
    }

    Ok(size)
}

/// Writes `size` into the start of `buf`,
/// checking buffer length and maximum allowed size.
///
/// # Errors
/// - `BufferTooSmall` if `buf` is smaller than the size field.
/// - `SizeTooLarge` if `size` exceeds `MAX_SIZE`.
pub fn write_size(buf: &mut [u8], size: usize) -> Result<(), SizeError> {
    if buf.len() < SIZE_FIELD_LEN {
        return Err(SizeError::BufferTooSmall);
    }

    if size > MAX_SIZE {
        return Err(SizeError::SizeTooLarge { size });
    }

    let size_le = Size::try_from(size).unwrap().to_le_bytes();
    buf[..SIZE_FIELD_LEN].copy_from_slice(&size_le);

    Ok(())
}
