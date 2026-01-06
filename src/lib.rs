//! Transport Abstraction Crate
//!
//! This crate provides abstractions over network protocols, exposing only the guarantees
//! of each protocol while hiding the underlying implementation details.
//!
//! Currently, it supports TCP and UDP.
//! Support for RUDP (Reliable UDP) policies will be added later.
//!
//! Only connections with matching IP versions are supported (IPv4 ↔ IPv4, IPv6 ↔ IPv6).
//! Cross-version support will be added in a future release.
//!
//! This crate operates at the OSI transport layer, allowing users to build clients and servers
//! without dealing directly with TCP/UDP differences.

mod doc {
    //! # Notes on Protocols
    //!
    //! ## TCP / UDP / RUDP
    //!
    //! TCP guarantees:
    //! - **Integrity**: Payload is not corrupted.
    //! - **Non-duplication**: Each message is received exactly once.
    //! - **Delivery**: Sender is notified of delivery.
    //! - **Order**: Messages arrive in the order sent.
    //!
    //! TCP does *not* guarantee:
    //! - **Boundaries**: Messages are merged into a continuous stream.
    //!
    //! UDP guarantees:
    //! - **Integrity**: Payload is not corrupted.
    //! - **Boundaries**: Messages are not merged or split.
    //!
    //! UDP does *not* guarantee:
    //! - **Non-duplication**: Messages may be duplicated.
    //! - **Delivery**: No acknowledgment; fire and forget.
    //! - **Order**: Messages may arrive out of order.
    //!
    //! RUDP (Reliable UDP) aims to provide more guarantees than UDP while maintaining
    //! lower latency than TCP, e.g., for gaming.
    //!
    //! ## IPv4 & IPv6
    //!
    //! - **Dual-stack socket**: Handles both IP versions, but not always available.
    //! - **Separate sockets per version**: Most common approach.
    //!
    //! ## Session vs Transport
    //!
    //! - **Transport**: Guarantees (e.g., RUDP, ENet).  
    //! - **Session**: Continuity (authentication, reconnection, etc.).  
}

pub mod client;
pub mod server;

mod protocol;

pub use protocol::tcp::MAX_PAYLOAD_LENGTH as MAX_TCP_PAYLOAD_LENGTH;

#[cfg(test)]
mod tests;
