//! Items shared by client and server.

use num_enum::{IntoPrimitive, TryFromPrimitive};

// ---- Max size constants ----

/// Max size (in bytes) for a packet.
pub const MAX_PACKET_SIZE: usize = 1024;

/// Max size (in bytes) for a packet's header.
pub const MAX_HEADER_SIZE: usize = 32;

/// Max size (in bytes) for a packet's payload.
pub const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - MAX_HEADER_SIZE;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum PacketType {
    Unreliable,
}
