//! # TCP/UDP/RUDP
//!
//! Both are standard protocol buid on top of IP.
//!
//! TCP guarantees :
//! - Integrity : No corrupted payload (resend).
//! - Non-duplication : Single message sent => Single message received (using sequence numbers).
//! - Delivery : Notify sender of delivery (using acknoledgments).
//! - Order : Messages arrive in the order they were sent (using sequence numbers).
//!
//! UDP guarantees :
//! - Integrity.
//! - Boundaries : Packets are not split or merged (unlike TCP).
//!
//! In some scenarios, such as gaming, we want more flexibility. Examples :
//! - If we want Non-duplication or delivery but don't care about order ?
//! - If we want acknoledgment but custom resend rules (to prioritize other packets maybe) ?
//! - Etc.
//!
//! To do that we usually implement Reliable UDP (RUDP) : A protocol implemented on top of UDP.
//!
//! Most of the times, an RUDP protocol implements some degree of reliability (hence the "Reliable")
//! but it can designates any protocol on top of UDP *in my opinion*.
//!
//! # IPv4/IPv6
//!
//! Ideally we want to handle both IPv4 and IPv6
//! to get maximum player base.
//!
//! To do that we can :
//! - Use a dual-stack socket : Can handle both version, but not always available.
//! - Use one socket for each version. It's the most common.
//!
//! For now it's single socket and server/client IP versions must match (both v4 or both v6).

// # Session & Transport
//
// Different layers in network with different responsibilities :
// - Transport : Guarantees (ex: RUDP, ENet, etc.)
// - Session : Continuity (authentification, reconnection, etc.)
//
// Still unsure about the definitions.
//
// # Channels
//
// Channel naming conventions :
// - Message := Payload + application metadata.
// - Packet := Payload + protocol metadata (unseen by user).
// - Incoming := From network to app.
// - Outgoing := From app to network.
//
// # Threads
//
// They loop continuously on blocking functions.
// Blocking allow context switching.
//
// In case of error :
// => A thread fail and shutdown.
// => Chain reaction of channel disconnections and thus threads shutdowns.
// => Finally the user receive `Err(Disconnected)`.
//
// # Buffering strategy
//
// 1. Reserve RING capacity (multiple contiguous buffers), say 4Mb (each buffer 1Mb).
// 2. "Eat" (give ownership away) buffer from the front when buffering packets.
// 3. Once we reach the end of the ring :
//      - Reallocate BUFFER capacity
//          => Optimization (from "bytes" crate) should reallocate to beginning of the ring.
//          => Avoid reallocation from the OS.
//
// We reallocate only BUFFER (1Mb) because middle/end of the ring will be in use
// by other threads which haven't dropped the packet.
// => If app hold on too long on packets (next buffer in use), this won't work.
//
// This optimized behaviour wasn't tested.
// To test it, we need to monitor if buffer pointers always fall within ring range.

pub mod client;
pub mod server;

#[cfg(test)]
mod tests;

use num_enum::{IntoPrimitive, TryFromPrimitive};

const MAX_PACKET_SIZE: usize = 1024;
const MAX_HEADER_SIZE: usize = 32;
const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - MAX_HEADER_SIZE;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
enum PacketType {
    Test,
}
