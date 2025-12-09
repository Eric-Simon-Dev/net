//! Network library.
//!
//! ## Why customizing UDP ?
//!
//! UDP is one of the fastest protocol.
//!
//! Its guarantees are :
//! - Integrity (no errors).
//! - Boundaries (no split or merge of packets).
//!
//! Its non-guarantees are :
//! - Delivery
//! - Order
//! - Non-duplication (may be duplicate to different paths to maximize chances of arrival).
//!
//! Because of performance, it's sometimes better to customize UDP and not rely on TCP.
//!
//! ## IPV4
//!
//! With a single socket bind to an IPv4 address. It can only send and receive IPv4 packets.
//! To do IPv4 and IPv6, we need :
//! - 2 sockets (one for each).
//! - a single IPv6 socket with dual-stack enabled (will mapped IPv4 to IPv6, not default).
//!
//! Mine is only IPv4.
//!
//! ## Protocols
//!
//! Custom UDP-based protocols :
//! - Raw : No additional guarantees.
//! - Ack : Answer with an acknoledgment.
//!
//! ## Buffering
//!
//! Packets are stored in `bytes::Bytes`.
//!
//! `Bytes` can be split without copying.
//! The 2 resulting `Bytes` are then *contiguous in memory* (unless they grow).
//! This allow a nice suballocation stategy :
//! 1. Allocate big buffer "a" : `aaaaaaaaaaaaaaaa` (16 bytes of a)
//! 2. Fill beginning and split : `bbbaaaaaaaaaaaaa`
//! 3. Again, again : `bbbccccccddddaaa`
//! 4. If no more capacity in "a", reallocate somewhere else.
//! "b", "c", "d" can be passed with ownership and will drop when no longer needed.
//!
//! ## Channels
//!
//! `Bytes` are passed though `crossbeam` channels
//! which are high performance and multithread-friendly.
//!
//! ## Usage
//!
//! Send/Recv using channels, update network entity
//!

//! ## Terminology
//!
//! Packet = Data from the network.
//! Message = Data that passes the protocols. Consumable by the app.

// Test

pub mod client;
pub mod server;

#[cfg(test)]
mod tests;
