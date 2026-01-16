//! Simple OSI transport layer for client/server.
//!
//! Uses a **reactor** internally, running on a separate thread.
//!
//! ## API
//!
//! Client items are in [`client`] and server items in [`server`].
//!
//! Messages exposes:
//! - A channel byte for multiplexing.
//! - [`bytes::BytesMut`] for data.
//! - Guarantees (outgoing only):
//!     - None.
//!     - Delivery.
//!     - OrderDelivery (relative to channel).
//! - client ID (server only).
//!
//! ## Example
//!
//! ```
//! const SERVER_ADDR: &'static str = "0:12012";
//! const CLIENT_ADDR: &'static str = "0:0";
//!
//! /// Bind on `SERVER_ADDR` and wait for a single message.
//! fn server() {
//!     use net::server::listen;
//!
//!     let (outgoing, incoming, waker) = listen(SERVER_ADDR).unwrap();
//!
//!     let msg = incoming.recv().unwrap();
//!     assert_eq!(msg.data[0],0u8);
//! }
//!
//! /// Bind on `CLIENT_ADDR`, connect to `SERVER_ADDR` and send a single message.
//! fn client() {
//!     use net::client::{connect, OutgoingMessage, Guarantees};
//!     use bytes::BytesMut;
//!
//!     let (outgoing, incoming, waker) = connect(CLIENT_ADDR, SERVER_ADDR).unwrap();
//!
//!     let msg = OutgoingMessage {
//!         data: BytesMut::zeroed(1), // Send a single `0` byte.
//!         channel: 0,
//!         guarantees : Guarantees::Delivery,
//!     };
//!     outgoing.send(msg).unwrap();
//!     waker.process_available_operations().unwrap();
//! }
//!
//! # use std::{thread, time::Duration};
//! #
//! # thread::spawn(server);
//! # // Give server time to start so client connection succeeds.
//! # thread::sleep(Duration::from_millis(100));
//! # thread::spawn(client);
//! ```

mod doc {
    //! Memos
    //!
    //! # TCP / UDP / RUDP
    //!
    //! TCP and UDP protocols are our protocol primitives.
    //!
    //! RUDP is an overlay over UDP to implement some guarantees
    //! without compromising latency as much as TCP.
    //!
    //! ## TCP/UDP Guarantees
    //!
    //! **Integrity** is guaranteed by both TCP and UDP.
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
    //! - **Deduplication**: Messages may be duplicated.
    //! - **Delivery**: No acknowledgment; fire and forget.
    //! - **Order**: Messages may arrive out of order.
    //!
    //! # IPv4 & IPv6
    //!
    //! - **Dual-stack socket**: Handles both IP versions, but not always available.
    //! - **Separate sockets per version**: Most common approach.
    //!
    //! # OSI: Session & Transport layers
    //!
    //! - **Transport**: Guarantees (e.g., RUDP, ENet).  
    //! - **Session**: Continuity (authentication, reconnection, etc.).  
}

pub mod client;
pub mod server;

mod protocol;

pub use protocol::MAX_PAYLOAD_LENGTH;

#[cfg(test)]
mod tests;
