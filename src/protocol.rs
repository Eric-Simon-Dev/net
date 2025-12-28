//! Structures and functions shared by client and server to communicate.

use num_enum::{IntoPrimitive, TryFromPrimitive};

// Max size constants
pub const MAX_PACKET_SIZE: usize = 1024;
pub const MAX_HEADER_SIZE: usize = 32;
pub const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - MAX_HEADER_SIZE;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum PacketType {
    Unreliable,
}
