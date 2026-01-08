//! # Clients (server only)
//!
//! Messages to and from unregistered clients are dropped.
//!
//! # Size
//!
//! Messages with guarantees None but too big to fit in 1 packet will be using TCP.
//!
//! # Multiplexing
//!
//! Each message (TCP or UDP) also send a channel byte
//!
//! # Boundaries
//!
//! TCP boundaries are ensured by a payload lenght prefix at the beginning of the frame.
//! UDP messages are limited to fit one packet and thus don't need a protocol for that.
//!
//! # Deduplication
//!
//! UDP messages are prefixed with a seq number (increasing with each message)
//! that is checked upon receival with a sliding window
//! that check the last 64 packets (it's a bitmap, could be different) under highest received packet.
//! Older packets are dropped.
//!
//! # Order
//!
//! For UDP, might use the seq number but per channel and store temporaly (using timers too maybe)
//!
//! # Delivery
//!
//! Use ack numbers, highest pckts received, piggy-back on other packets, etc.

pub mod tcp;
pub mod udp;

pub const MAX_PAYLOAD_LENGTH: usize = tcp::MAX_PAYLOAD_LENGTH as usize;
