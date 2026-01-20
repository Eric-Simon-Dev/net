//! # Always guaranteed
//! 
//! ## Multiplexing (TCP/UDP)
//!
//! **Definition**: *Messages should have their own channel*.
//! 
//! **Implementation**: Channel byte in headers.
//! 
//! **Notes**:
//! - Limited to 256 channels.
//! 
//! ## Boundaries (TCP)
//!
//! **Definition**: *No merge or split from caller perspective,
//! one-to-one correspondance between send and receive (if no loss)*.
//! 
//! **Implementation**: Append a payload lenth prefix per message inside header.
//! 
//! **Notes**:
//! - An error during length parsing of one message invalidate the whole stream.
//!
//! ## Deduplication (UDP)
//! 
//! **Definition**: *Sent messages should be received at most once*.
//!
//! **Implementation**: Append an increasing sequence number per message.
//! Then use a sliding window per connection to identify already received message.
//! 
//! **Notes**:
//! - Packets that are too old are dropped. Adjust the sliding margin accordingly.
//!
//! # Optionnally guaranteed
//! 
//! For now all optional guarantees or big payloads fallback to TCP stream.
//! So it's working but not as efficiently as it should be.
//! 
//! ## Order (UDP, unimplemented)
//!
//! **Definition**: *Sent messages with `Order` guarantee should be received in the same order they were sent,
//! relative to their channel*.
//! 
//! **Implementation**: Might use sequence numbers, but it should be per channel ?, timers, etc.
//!
//! ## Delivery (UDP, unimplemented)
//! 
//! **Definition**: *Sent messages with `Delivery` guarantee should be retried until receival acknolegment.
//! Retrial should be done until connection timed out.*
//!
//! **Implementation**: Might use acknoledgment numbers, highest ack received, piggy-back on other packets, etc.

mod memo {
    //! # TCP/UDP Guarantees
    //!
    //! **Integrity** is guaranteed by both.
    //!
    //! TCP guarantees:
    //! - **Deduplication**: Each message is received exactly once.
    //! - **Delivery**: Sender is notified of delivery.
    //! - **Order**: Messages arrive in sending order.
    //!
    //! TCP does *not* guarantee:
    //! - **Boundaries**: Messages are merged into a continuous stream.
    //!
    //! UDP guarantees:
    //! - **Boundaries**: Messages are not merged or split.
    //!
    //! UDP does *not* guarantee:
    //! - **Deduplication**: Duplicated messages may be received.
    //! - **Delivery**: Sender is not notified of delivery. (fire-and-forget).
    //! - **Order**: Messages may be received out of sending order.
}

pub mod tcp;
pub mod udp;

pub const MAX_PAYLOAD_LENGTH: usize = tcp::MAX_PAYLOAD_LENGTH as usize;
