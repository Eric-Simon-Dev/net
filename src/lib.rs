//! RUDP network library.
//!
//! Uses 3 threads for networking :
//! - Receiver : Receive and buffer incoming UDP packets.
//! - Sender : Send outgoing UDP packets.
//! - Handler : Interface between App and Receiver/Sender. Handle RUDP protocol logic.
//!
//! ## Usage
//!
//! Provide `client` and `server` modules,
//! depending on the network entity needed.
//!
//! Channels are used to send and receive messages using :
//! - `crossbeam::channel::{Sender, Receiver}` : Flexible, Efficient, Multi-thread channels.
//! - `bytes::{Bytes, BytesMut}` data pointers : Ergonomic, Multi-thread.
//!
//! These structures are ergonomic and used in `tokio` (very serious crate).
//!
//! ## Memos
//!
//! ### TCP/UDP/RUDP
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
//! ### IPv4/IPv6
//!
//! Ideally we want to handle both IPv4 and IPv6
//! to get maximum player base.
//!
//! To do that we can :
//! - Use a dual-stack socket : Can handle both version, but not always available.
//! - Use one socket for each version. It's the most common.
//!
//! For now it's single socket and server/client IP versions must match (both v4 or both v6).

// ## Terminology
//
// Packet = Network-aware data, meant for transport.
// Message = Network-agnostic data, meant for the app.
//
// ## Channel propagation
//
// Channels are used as dominos to propagate errors and shutdowns. Examples :
//
// Drop Self
// => Channel disconnection on Handler
// => Drop Handler
// => Channel disconnection on Recv & Send
// => Drop Recv & Drop Send
//
// Error on Recv
// => Drop Recv
// => Channel disconnection on Handler
// => Drop Handler
// => Channel disconnection on Send & Self
// => Drop Send & Return error `Disconnected` when using Self

mod client;
mod server;

pub use client::Client;
pub use server::{CLIENT_CAPACITY, Server};

#[cfg(test)]
mod tests;
